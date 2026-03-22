use crate::backend::config::InternalStateManager;
use crate::backend::core::search::{Projection, SearchCriteria, SortBy};
use crate::backend::core::table::{TableManager, TableWriteAheadLog};
use crate::backend::core::types::{ColumnId, Constraint, DataType, Index, RowId, TableId};
use crate::backend::core::types::{InternalTableSchema, Row, TableSchema};
use crate::backend::errors::DataBaseErrors;
use crate::backend::storage::wal::{DataBaseOperation, WriteAheadLogBase, WriteAheadLogManager};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{debug, error, info};

fn serialize_tables<S>(
    tables: &BTreeMap<TableId, Arc<RwLock<InternalTableSchema>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let export: BTreeMap<TableId, InternalTableSchema> = tables
        .iter()
        .filter_map(|(k, v)| match v.read() {
            Ok(guard) => Some((*k, guard.clone())),
            Err(e) => {
                error!(
                    "Failed to acquire read lock on table {} during serialization: {}",
                    k, e
                );
                None
            }
        })
        .collect();
    export.serialize(serializer)
}

fn deserialize_tables<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<TableId, Arc<RwLock<InternalTableSchema>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<TableId, InternalTableSchema>::deserialize(deserializer)?;
    Ok(intermediate
        .into_iter()
        .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
        .collect())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InternalDatabaseSchema {
    #[serde(
        serialize_with = "serialize_tables",
        deserialize_with = "deserialize_tables"
    )]
    pub tables: BTreeMap<TableId, Arc<RwLock<InternalTableSchema>>>,
    pub tables_index: BTreeMap<String, TableId>,
    next_table_id: AtomicU64,
    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WriteAheadLogManager>>,
}

impl Clone for InternalDatabaseSchema {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            tables_index: self.tables_index.clone(),
            next_table_id: AtomicU64::new(self.next_table_id.load(Ordering::SeqCst)),
            internal_state_manager: self.internal_state_manager.clone(),
            wal_manager: self.wal_manager.clone(),
        }
    }
}

pub trait DataBaseManager {
    fn create_table(&mut self, table_name: String) -> Result<TableId, DataBaseErrors>;
    fn drop_table(&mut self, table_id: TableId) -> Result<(), DataBaseErrors>;

    fn get_table_id(&self, table_name: String) -> Result<TableId, DataBaseErrors>;
    fn get_table_schema(&self, table_id: TableId) -> Result<TableSchema, DataBaseErrors>;
    fn get_table_size(&self, table_id: TableId) -> Result<usize, DataBaseErrors>;

    fn list_tables(&self) -> Vec<String>;

    fn create_column(
        &self,
        table_id: TableId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<ColumnId, DataBaseErrors>;
    fn drop_column(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors>;
    fn create_index(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors>;
    fn drop_index(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors>;

    fn insert_rows(&self, table_id: TableId, rows: Vec<Row>) -> Result<(), DataBaseErrors>;
    fn delete_rows(&self, table_id: TableId, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors>;
    fn update_rows(
        &self,
        table_id: TableId,
        row_ids: Vec<RowId>,
        new_values: Row,
    ) -> Result<(), DataBaseErrors>;
    fn get_rows(&self, table_id: TableId, row_id: Vec<RowId>) -> Result<Vec<Row>, DataBaseErrors>;
    fn search_rows(
        &self,
        table_id: TableId,
        criteria: Vec<SearchCriteria>,
        projection: Option<Projection>,
        sort_by: Option<SortBy>,
    ) -> Result<Vec<Row>, DataBaseErrors>;
}

pub trait DataBaseWriteAheadLog: WriteAheadLogBase {
    fn wal_create_table(&mut self, table_id: TableId, name: String) -> Result<(), DataBaseErrors>;
    fn wal_drop_table(&mut self, table_id: TableId) -> Result<(), DataBaseErrors>;
    fn wal_create_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: Option<Index>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DataBaseErrors>;
    fn wal_create_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        index: Index,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DataBaseErrors>;
    fn wal_insert_rows(
        &mut self,
        table_id: TableId,
        rows: Vec<(RowId, Row)>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_delete_rows(
        &mut self,
        table_id: TableId,
        row_ids: Vec<RowId>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_update_rows(
        &mut self,
        table_id: TableId,
        row_ids: Vec<RowId>,
        row: Row,
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
                    debug!(
                        "Skipping WAL log during replay for operation: {:?}",
                        operation
                    );
                }
                Ok(())
            }
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
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) -> Self {
        InternalDatabaseSchema {
            tables: BTreeMap::new(),
            tables_index: BTreeMap::new(),
            next_table_id: AtomicU64::new(0),
            internal_state_manager,
            wal_manager: wal_manager,
        }
    }

    pub fn inject_contexts(
        &mut self,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        self.wal_manager = wal_manager;

        // Also inject into all tables
        for table in self.tables.values_mut() {
            match table.write() {
                Ok(mut guard) => {
                    guard.inject_contexts(
                        self.internal_state_manager.clone(),
                        self.wal_manager.clone(),
                    );
                }
                Err(e) => error!(
                    "Failed to acquire write lock on table during context injection: {}",
                    e
                ),
            }
        }
    }

    fn insert_table(&mut self, table_id: TableId, table_schema: InternalTableSchema) {
        self.tables_index
            .insert(table_schema.name.clone(), table_id);
        self.tables
            .insert(table_id, Arc::new(RwLock::new(table_schema)));
    }

    fn remove_table(&mut self, table_id: TableId) {
        self.tables_index
            .retain(|_key, &mut value| value != table_id);
        self.tables.remove(&table_id);
    }
}

impl DataBaseWriteAheadLog for InternalDatabaseSchema {
    fn wal_create_table(
        &mut self,
        table_id: TableId,
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

    fn wal_drop_table(&mut self, table_id: TableId) -> Result<(), DataBaseErrors> {
        self.drop_table(table_id)
    }

    fn wal_create_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: Option<Index>,
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

    fn wal_drop_column(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_drop_column(column_id),
        }
    }

    fn wal_create_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
        index: Index,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_create_index(column_id, index),
        }
    }

    fn wal_drop_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_drop_index(column_id),
        }
    }

    fn wal_insert_rows(
        &mut self,
        table_id: TableId,
        rows: Vec<(RowId, Row)>,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_insert_rows(rows),
        }
    }

    fn wal_delete_rows(
        &mut self,
        table_id: TableId,
        row_ids: Vec<RowId>,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_delete_rows(row_ids),
        }
    }

    fn wal_update_rows(
        &mut self,
        table_id: TableId,
        row_ids: Vec<RowId>,
        row: Row,
    ) -> Result<(), DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => Err(DataBaseErrors::TableIDNotFound(table_id)),
            Some(i) => i.write().unwrap().wal_update_rows(row_ids, row),
        }
    }
}

impl DataBaseManager for InternalDatabaseSchema {
    fn create_table(&mut self, table_name: String) -> Result<TableId, DataBaseErrors> {
        info!("Creating table '{}'", table_name);
        if self.tables_index.iter().any(|t| *t.0 == table_name) {
            error!("Table '{}' already exists", table_name);
            return Err(DataBaseErrors::TableAlreadyExists(table_name));
        }

        let table_id = self.next_table_id.fetch_add(1, Ordering::SeqCst);
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
        info!(
            "Table '{}' created successfully with ID {}",
            table_name, table_id
        );
        Ok(table_id)
    }

    fn drop_table(&mut self, table_id: TableId) -> Result<(), DataBaseErrors> {
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

    fn get_table_id(&self, table_name: String) -> Result<TableId, DataBaseErrors> {
        debug!("geting table if for {table_name}");
        match self.tables_index.get(&table_name) {
            None => Err(DataBaseErrors::TableNotFound(table_name)),
            Some(i) => Ok(*i),
        }
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<TableSchema, DataBaseErrors> {
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

    fn get_table_size(&self, table_id: TableId) -> Result<usize, DataBaseErrors> {
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
        &self,
        table_id: TableId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<ColumnId, DataBaseErrors> {
        debug!("Creating column '{}' in table {}", column_name, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(i) => {
                debug!("Acquiring write lock for table {}", table_id);
                match i.write() {
                    Ok(mut guard) => {
                        let result =
                            guard.create_column(column_name.clone(), data_type, constraints);
                        match &result {
                            Ok(col_id) => info!(
                                "Column '{}' created with ID {} in table {}",
                                column_name, col_id, table_id
                            ),
                            Err(e) => error!("Failed to create column '{}': {}", column_name, e),
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn drop_column(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        debug!("Dropping column {} from table {}", column_id, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
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
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn create_index(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        debug!(
            "Creating index for column {} in table {}",
            column_id, table_id
        );
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(i) => {
                debug!("Acquiring write lock for table {}", table_id);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.create_index(column_id);
                        match &result {
                            Ok(_) => info!(
                                "Index created for column {} in table {}",
                                column_id, table_id
                            ),
                            Err(e) => {
                                error!("Failed to create index for column {}: {}", column_id, e)
                            }
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn drop_index(&self, table_id: TableId, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        debug!(
            "Dropping index for column {} in table {}",
            column_id, table_id
        );
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(i) => {
                debug!("Acquiring write lock for table {}", table_id);
                match i.write() {
                    Ok(mut guard) => {
                        let result = guard.drop_index(column_id);
                        match &result {
                            Ok(_) => info!(
                                "Index dropped for column {} in table {}",
                                column_id, table_id
                            ),
                            Err(e) => {
                                error!("Failed to drop index for column {}: {}", column_id, e)
                            }
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn insert_rows(&self, table_id: TableId, rows: Vec<Row>) -> Result<(), DataBaseErrors> {
        let row_count = rows.len();
        debug!("Inserting {} rows into table {}", row_count, table_id);
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(i) => {
                debug!(
                    "Acquiring write lock for table {} to insert {} rows",
                    table_id, row_count
                );
                match i.write() {
                    Ok(mut guard) => {
                        let start_row_id = guard.next_row_id.fetch_add(row_count as u64, Ordering::SeqCst);
                        let processed_rows = guard.pre_insert_rows(&rows, start_row_id);
                        match &processed_rows {
                            Ok(_) => info!("Successfully computed rows"),
                            Err(e) => error!("Failed computing rows: {}", e),
                        }
                        let processed_rows = processed_rows?;

                        let result = guard.insert_rows(processed_rows);
                        match &result {
                            Ok(_) => {
                                info!(
                                    "Successfully inserted {} rows into table {}",
                                    row_count, table_id
                                )
                            }
                            Err(e) => error!(
                                "Failed to insert {} rows into table {}: {}",
                                row_count, table_id, e
                            ),
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn delete_rows(&self, table_id: TableId, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors> {
        let row_count = row_ids.len();
        debug!(
            "Deleting {} rows from table {}: {:?}",
            row_count, table_id, row_ids
        );
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(table_arc) => {
                debug!(
                    "Acquiring write lock for table {} to delete {} rows",
                    table_id, row_count
                );

                match table_arc.write() {
                    Ok(mut guard) => {
                        let validation = guard.pre_delete_rows(&row_ids);
                        match &validation {
                            Ok(_) => info!(
                                "Successfully computed rows to delete from table {}",
                                table_id
                            ),
                            Err(e) => error!(
                                "Failed to compute rows to delete from table {}: {}",
                                table_id, e
                            ),
                        }
                        validation?;

                        let result = guard.delete_rows(row_ids);
                        match &result {
                            Ok(_) => info!(
                                "Successfully deleted {} rows from table {}",
                                row_count, table_id
                            ),
                            Err(e) => error!(
                                "Failed to delete {} rows from table {}: {}",
                                row_count, table_id, e
                            ),
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn update_rows(
        &self,
        table_id: TableId,
        row_ids: Vec<RowId>,
        new_values: Row,
    ) -> Result<(), DataBaseErrors> {
        info!("Updating the rows for {}", table_id);
        let row_count = row_ids.len();
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(db_arc) => {
                debug!(
                    "Acquiring write lock for table {} to update {} rows",
                    table_id, row_count
                );

                match db_arc.write() {
                    Ok(mut guard) => {
                        let validation_result = guard.pre_update_rows(&row_ids, &new_values);
                        match &validation_result {
                            Ok(_) => info!(
                                "Successfully validated rows to update in table {}",
                                table_id
                            ),
                            Err(e) => error!(
                                "Failed to validate rows to update in table {}: {}",
                                table_id, e
                            ),
                        }
                        validation_result?;

                        let result = guard.update_rows(row_ids, new_values);
                        match &result {
                            Ok(_) => info!(
                                "Successfully updated {} rows in table {}",
                                row_count, table_id
                            ),
                            Err(e) => error!(
                                "Failed to update {} rows in table {}: {}",
                                row_count, table_id, e
                            ),
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire write lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }

    fn get_rows(&self, table_id: TableId, row_ids: Vec<RowId>) -> Result<Vec<Row>, DataBaseErrors> {
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(db_arc) => match db_arc.read() {
                Ok(guard) => {
                    let result = guard.get_rows(&row_ids);
                    match &result {
                        Ok(rows) => {
                            debug!("Rows({}) retrieved from table {}", rows.len(), table_id)
                        }
                        Err(e) => error!("Failed to fetch rows from table {}: {}", table_id, e),
                    }
                    result
                }
                Err(e) => {
                    error!("Failed to acquire read lock for table {}: {}", table_id, e);
                    Err(DataBaseErrors::WalLockError)
                }
            },
        }
    }

    fn search_rows(
        &self,
        table_id: TableId,
        criteria: Vec<SearchCriteria>,
        projection: Option<Projection>,
        sort_by: Option<SortBy>,
    ) -> Result<Vec<Row>, DataBaseErrors> {
        debug!(
            "Searching table {} with {} criteria, projection: {}, sort_by: {}",
            table_id,
            criteria.len(),
            projection.is_some(),
            sort_by.is_some()
        );
        match self.tables.get(&table_id) {
            None => {
                error!("Table not found: {}", table_id);
                Err(DataBaseErrors::TableIDNotFound(table_id))
            }
            Some(i) => {
                debug!("Acquiring read lock for table {} to search", table_id);
                match i.read() {
                    Ok(guard) => {
                        let result = guard.find_row(criteria, projection, sort_by);
                        match &result {
                            Ok(rows) => {
                                info!("Search in table {} returned {} rows", table_id, rows.len())
                            }
                            Err(e) => error!("Failed to search table {}: {}", table_id, e),
                        }
                        result
                    }
                    Err(e) => {
                        error!("Failed to acquire read lock for table {}: {}", table_id, e);
                        Err(DataBaseErrors::WalLockError)
                    }
                }
            }
        }
    }
}
