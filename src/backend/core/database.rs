use crate::backend::config::InternalStateManager;
use crate::backend::core::table::{
    CellStructure, InternalCell, InternalTableSchema, TableManager, TableSchema, TableWriteAheadLog
};
use crate::backend::errors::DataBaseErrors;
use crate::backend::schema::{Constraint, DataType};
use crate::backend::storage::wal::{DataBaseOperation, WALManager, WriteAheadLogBase};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use tracing::{info, debug, error};

fn serialize_tables<S>(
    tables: &BTreeMap<u64, Arc<RwLock<InternalTableSchema>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let export: BTreeMap<u64, InternalTableSchema> = tables
        .iter()
        .filter_map(|(k, v)| {
            match v.read() {
                Ok(guard) => Some((*k, guard.clone())),
                Err(e) => {
                    error!("Failed to acquire read lock on table {} during serialization: {}", k, e);
                    None
                }
            }
        })
        .collect();
    export.serialize(serializer)
}

fn deserialize_tables<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<u64, Arc<RwLock<InternalTableSchema>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<u64, InternalTableSchema>::deserialize(deserializer)?;
    Ok(intermediate
        .into_iter()
        .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
        .collect())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalDatabaseSchema {
    #[serde(
        serialize_with = "serialize_tables",
        deserialize_with = "deserialize_tables"
    )]
    pub tables: BTreeMap<u64, Arc<RwLock<InternalTableSchema>>>,
    pub tables_index: BTreeMap<String, u64>,
    next_table_id: u64,
    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WALManager>>,
}

pub trait DataBaseManager {
    fn create_table(&mut self, table_name: String) -> Result<u64, DataBaseErrors>;
    fn drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors>;

    fn get_table_id(&self, table_name: String) -> Result<u64, DataBaseErrors>;
    fn get_table_schema(&self, table_id: u64) -> Result<TableSchema, DataBaseErrors>;
    fn get_table_size(&self, table_id: u64) -> Result<usize, DataBaseErrors>;

    fn list_tables(&self) -> Vec<String>;

    fn create_column(
        &mut self,
        table_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<u64, DataBaseErrors>;
    fn drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors>;
    fn insert_rows(
        &mut self,
        table_id: u64,
        rows: Vec<Vec<CellStructure>>,
    ) -> Result<(), DataBaseErrors>;
    fn delete_rows(&mut self, table_id: u64, row_ids: Vec<u128>) -> Result<(), DataBaseErrors>;
    fn update_rows(
        &mut self,
        table_id: u64,
        row_ids: Vec<u128>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors>;
    fn get_row(&self, table_id: u64, row_id: u128) -> Result<Vec<CellStructure>, DataBaseErrors>;
}

pub trait DataBaseWriteAheadLog: WriteAheadLogBase {
    fn wal_create_table(&mut self, table_id: u64, name: String) -> Result<(), DataBaseErrors>;
    fn wal_drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_create_column(
        &mut self,
        table_id: u64,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: BTreeMap<Vec<u8>, u128>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_insert_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_delete_row(&mut self, table_id: u64, row_id: u128) -> Result<(), DataBaseErrors>;
    fn wal_update_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;
}

impl WriteAheadLogBase for InternalDatabaseSchema {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        match self.wal_manager.lock() {
            Ok(mut wal) => {
                if !self.internal_state_manager.read().unwrap().is_wal_replaying {
                    debug!("WAL lock acquired. Logging operation: {:?}", operation);
                    wal.append(&operation);
                    info!("Operation logged to WAL successfully");
                } else {
                    debug!("Skipping WAL log during replay for operation: {:?}", operation);
                }
                Ok(())
            },
            Err(e) => {
                error!("Failed to acquire WAL lock: {}", e);
                Err(DataBaseErrors::WalLockError)
            }
        }
    }
}
impl InternalDatabaseSchema {
    pub fn new(
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WALManager>>,
    ) -> Self {
        InternalDatabaseSchema {
            tables: BTreeMap::new(),
            tables_index: BTreeMap::new(),
            next_table_id: 0,
            internal_state_manager,
            wal_manager: wal_manager,
        }
    }

    pub fn inject_contexts(
        &mut self,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WALManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        self.wal_manager = wal_manager;

        // Also inject into all tables
        for table in self.tables.values_mut() {
            match table.write() {
                Ok(mut guard) => guard.inject_contexts(
                    self.internal_state_manager.clone(),
                    self.wal_manager.clone(),
                ),
                Err(e) => error!("Failed to acquire write lock on table during context injection: {}", e),
            }
        }
    }

    fn insert_table(&mut self, table_id: u64, table_schema: InternalTableSchema) {
        self.tables_index
            .insert(table_schema.name.clone(), table_id);
        self.tables
            .insert(table_id, Arc::new(RwLock::new(table_schema)));
    }

    fn remove_table(&mut self, table_id: u64) {
        self.tables_index
            .retain(|_key, &mut value| value != table_id);
    }
}

impl DataBaseWriteAheadLog for InternalDatabaseSchema {
    fn wal_create_table(
        &mut self,
        table_id: u64,
        table_name: String,
    ) -> Result<(), DataBaseErrors> {
        if self.tables_index.iter().any(|t| *t.0 == table_name) {
            return Err(DataBaseErrors::TableAlreadyExists(table_name));
        }
        self.insert_table(
            table_id,
            InternalTableSchema::new(
                table_id,
                table_name,
                self.internal_state_manager.clone(),
                self.wal_manager.clone(),
            ),
        );
        Ok(())
    }

    fn wal_drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors> {
        self.drop_table(table_id)
    }

    fn wal_create_column(
        &mut self,
        table_id: u64,
        column_id: u64,
        column_name: String,
        data_type: crate::backend::schema::DataType,
        constraints: Vec<crate::backend::schema::Constraint>,
        index: BTreeMap<Vec<u8>, u128>,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_create_column(
                column_id,
                column_name,
                data_type,
                constraints,
                index,
            ),
        }
    }

    fn wal_drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_drop_column(column_id),
        }
    }

    fn wal_insert_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_insert_row(row_id, row),
        }
    }

    fn wal_delete_row(&mut self, table_id: u64, row_id: u128) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_delete_row(row_id),
        }
    }

    fn wal_update_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_update_row(row_id, cells),
        }
    }
}

impl DataBaseManager for InternalDatabaseSchema {
    fn create_table(&mut self, table_name: String) -> Result<u64, DataBaseErrors> {
        info!("Creating table '{}'", table_name);
        if self.tables_index.iter().any(|t| *t.0 == table_name) {
            error!("Table '{}' already exists", table_name);
            return Err(DataBaseErrors::TableAlreadyExists(table_name));
        }

        let table_id = self.next_table_id;
        self.next_table_id += 1;
        debug!("Assigned table ID {} to table '{}'", table_id, table_name);

        debug!("Logging table creation to WAL");
        self.log_operation(DataBaseOperation::CreateTable {
            table_id,
            name: table_name.clone(),
        })?;

        self.insert_table(
            table_id,
            InternalTableSchema::new(
                table_id,
                table_name.clone(),
                self.internal_state_manager.clone(),
                self.wal_manager.clone(),
            ),
        );
        info!("Table '{}' created successfully with ID {}", table_name, table_id);
        Ok(table_id)
    }

    fn drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors> {
        info!("Dropping table {}", table_id);
        if !self.tables.contains_key(&table_id) {
            error!("Table not found: {}", table_id);
            return Err(DataBaseErrors::TableNotFound(table_id.to_string()));
        }
        debug!("Logging table drop to WAL");
        self.log_operation(DataBaseOperation::DropTable { table_id })?;
        self.remove_table(table_id);
        info!("Table {} dropped successfully", table_id);
        Ok(())
    }

    fn get_table_id(&self, table_name: String) -> Result<u64, DataBaseErrors> {
        debug!("geting table if for {table_name}");
        match self.tables_index.get(&table_name) {
            None => Err(DataBaseErrors::TableNotFound(table_name)),
            Some(i) => Ok(*i),
        }
    }

    fn get_table_schema(&self, table_id: u64) -> Result<TableSchema, DataBaseErrors> {
        debug!("getting table schema for {table_id}");
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => match i.read() {
                Ok(guard) => guard.get_schema(),
                Err(e) => {
                    error!("Failed to acquire read lock for table {}: {}", table_id, e);
                    Err(DataBaseErrors::WalLockError)
                }
            },
        }
    }

    fn get_table_size(&self, table_id: u64) -> Result<usize, DataBaseErrors> {
        debug!("getting table size for {table_id}");
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => match i.read() {
                Ok(guard) => Ok(guard.get_size()),
                Err(e) => {
                    error!("Failed to acquire read lock for table {}: {}", table_id, e);
                    Err(DataBaseErrors::WalLockError)
                }
            },
        }
    }

    fn list_tables(&self) -> Vec<String> {
        debug!("getting tables list");
        self.tables_index
            .iter()
            .map(|(table_name, _table_id)| table_name.clone())
            .collect()
    }

    fn create_column(
        &mut self,
        table_id: u64,
        column_name: String,
        data_type: crate::backend::schema::DataType,
        constraints: Vec<crate::backend::schema::Constraint>,
    ) -> Result<u64, DataBaseErrors> {
        debug!("Creating column '{}' in table {}", column_name, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring write lock for table {}", table_id);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.create_column(column_name.clone(), data_type, constraints);
                        match &result {
                            Ok(col_id) => info!("Column '{}' created with ID {} in table {}", column_name, col_id, table_id),
                            Err(e) => error!("Failed to create column '{}': {}", column_name, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }

    fn drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors> {
        debug!("Dropping column {} from table {}", column_id, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring write lock for table {}", table_id);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.drop_column(column_id);
                        match &result {
                            Ok(_) => info!("Column {} dropped from table {}", column_id, table_id),
                            Err(e) => error!("Failed to drop column {}: {}", column_id, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }

    fn insert_rows(
        &mut self,
        table_id: u64,
        rows: Vec<Vec<CellStructure>>,
    ) -> Result<(), DataBaseErrors> {
        let row_count = rows.len();
        debug!("Inserting {} rows into table {}", row_count, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring write lock for table {} to insert {} rows", table_id, row_count);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.insert_rows(rows);
                        match &result {
                            Ok(_) => info!("Successfully inserted {} rows into table {}", row_count, table_id),
                            Err(e) => error!("Failed to insert {} rows into table {}: {}", row_count, table_id, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }

    fn delete_rows(&mut self, table_id: u64, row_ids: Vec<u128>) -> Result<(), DataBaseErrors> {
        let row_count = row_ids.len();
        debug!("Deleting {} rows from table {}: {:?}", row_count, table_id, row_ids);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring write lock for table {} to delete {} rows", table_id, row_count);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.delete_rows(row_ids);
                        match &result {
                            Ok(_) => info!("Successfully deleted {} rows from table {}", row_count, table_id),
                            Err(e) => error!("Failed to delete {} rows from table {}: {}", row_count, table_id, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }

    fn update_rows(
        &mut self,
        table_id: u64,
        row_ids: Vec<u128>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors> {
        let row_count = row_ids.len();
        debug!("Updating {} rows in table {}: {:?}", row_count, table_id, row_ids);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring write lock for table {} to update {} rows", table_id, row_count);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.update_rows(row_ids, new_values);
                        match &result {
                            Ok(_) => info!("Successfully updated {} rows in table {}", row_count, table_id),
                            Err(e) => error!("Failed to update {} rows in table {}: {}", row_count, table_id, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }

    fn get_row(&self, table_id: u64, row_id: u128) -> Result<Vec<CellStructure>, DataBaseErrors> {
        debug!("Fetching row {} from table {}", row_id, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            },
            Some(i) => {
                debug!("Acquiring read lock for table {} to fetch row {}", table_id, row_id);
                match i.read() {
                    Ok(guard) => {
                        let result = guard.get_row(row_id);
                        match &result {
                            Ok(row) => debug!("Row {} retrieved from table {} with {} cells", row_id, table_id, row.len()),
                            Err(e) => error!("Failed to fetch row {} from table {}: {}", row_id, table_id, e),
                        }
                        result
                    },
                    Err(e) => {
                        error!("Failed to acquire read lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            },
        }
    }
}
