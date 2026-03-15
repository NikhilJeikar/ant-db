use crate::backend::config::InternalStateManager;
use crate::backend::errors::DataBaseErrors;
use crate::backend::schema::{
    Constraint, DataType
};
use crate::backend::core::table::{InternalTableSchema, TableSchema, TableManager, CellStructure, InternalCell};
use crate::backend::storage::wal::{DataBaseOperation, WALManager, WalOps};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseSchema {
    tables: Vec<TableSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InternalDatabaseSchema {
    pub tables: BTreeMap<u64, InternalTableSchema>,
    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WALManager>>,
}

pub trait SchemaManager {
    fn create_table(&mut self, table_name: String) -> Result<u64, DataBaseErrors>;
    fn drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors>;

    fn get_table(&mut self, table_id: u64) -> Result<&mut InternalTableSchema, DataBaseErrors>;
    fn get_table_id(&self, table_name: String) -> Result<u64, DataBaseErrors>;
    fn get_table_schema(&self, table_id: u64) -> Result<TableSchema, DataBaseErrors>;
    fn get_table_size(&self, table_id: u64) -> Result<usize, DataBaseErrors>;

    fn list_tables(&self) -> Result<Vec<String>, DataBaseErrors>;

    fn create_column(
        &mut self,
        table_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<(), DataBaseErrors>;
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

    // replay operations for WAL recovery
    fn wal_create_table(&mut self, table_id: u64, name: String) -> Result<(), DataBaseErrors>;
    fn wal_drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_create_column(
        &mut self,
        table_id: u64,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
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

impl InternalDatabaseSchema {
    pub fn new(
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WALManager>>,
    ) -> Self {
        InternalDatabaseSchema {
            tables: BTreeMap::new(),
            internal_state_manager,
            wal_manager: wal_manager,
        }
    }
}
impl WalOps for InternalDatabaseSchema {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        let mut wal = self
            .wal_manager
            .lock()
            .map_err(|_| DataBaseErrors::WalLockError)?;
        if !self.internal_state_manager.read().unwrap().is_wal_replaying {
            println!("Writing to WAL {:?}", operation);
            wal.append(&operation);
        }
        Ok(())
    }
}

impl SchemaManager for InternalDatabaseSchema {
    fn create_table(&mut self, table_name: String) -> Result<u64, DataBaseErrors> {
        if self.tables.iter().any(|t| t.1.name == table_name) {
            return Err(DataBaseErrors::TableAlreadyExists(table_name));
        }
        let table_id = self.tables.len() as u64 + 1;
        self.log_operation(DataBaseOperation::CreateTable {
            table_id,
            name: table_name.clone(),
        })?;
        self.tables.insert(
            table_id,
            InternalTableSchema::new(
                table_id,
                table_name,
                self.internal_state_manager.clone(),
                self.wal_manager.clone(),
            ),
        );
        Ok(table_id)
    }

    fn drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors> {
        if !self.tables.contains_key(&table_id) {
            return Err(DataBaseErrors::TableNotFound(table_id.to_string()));
        }
        self.log_operation(DataBaseOperation::DropTable { table_id })?;
        self.tables.remove(&table_id);
        Ok(())
    }

    fn get_table(&mut self, table_id: u64) -> Result<&mut InternalTableSchema, DataBaseErrors> {
        self.tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))
    }

    fn get_table_id(&self, table_name: String) -> Result<u64, DataBaseErrors> {
        self.tables
            .iter()
            .find(|t| t.1.name == table_name)
            .map(|(id, _)| *id)
            .ok_or(DataBaseErrors::TableNotFound(table_name))
    }

    fn get_table_schema(&self, table_id: u64) -> Result<TableSchema, DataBaseErrors> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.get_schema()
    }

    fn get_table_size(&self, table_id: u64) -> Result<usize, DataBaseErrors> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        Ok(table.get_size())
    }

    fn list_tables(&self) -> Result<Vec<String>, DataBaseErrors> {
        Ok(self.tables.iter().map(|t| t.1.name.clone()).collect())
    }

    fn create_column(
        &mut self,
        table_id: u64,
        column_name: String,
        data_type: crate::backend::schema::DataType,
        constraints: Vec<crate::backend::schema::Constraint>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.create_column(column_name, data_type, constraints)
    }

    fn drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.drop_column(column_id)
    }

    fn insert_rows(
        &mut self,
        table_id: u64,
        rows: Vec<Vec<CellStructure>>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.insert_rows(rows)
    }

    fn delete_rows(&mut self, table_id: u64, row_ids: Vec<u128>) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.delete_rows(row_ids)
    }

    fn update_rows(
        &mut self,
        table_id: u64,
        row_ids: Vec<u128>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.update_rows(row_ids, new_values)
    }

    fn get_row(&self, table_id: u64, row_id: u128) -> Result<Vec<CellStructure>, DataBaseErrors> {
        let table = self
            .tables
            .get(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.get_row(row_id)
    }

    // WAL replay methods
    fn wal_create_table(&mut self, table_id: u64, name: String) -> Result<(), DataBaseErrors> {
        self.tables.insert(
            table_id,
            InternalTableSchema::new(
                table_id,
                name,
                self.internal_state_manager.clone(),
                self.wal_manager.clone(),
            ),
        );
        Ok(())
    }

    fn wal_drop_table(&mut self, table_id: u64) -> Result<(), DataBaseErrors> {
        self.tables.remove(&table_id);
        Ok(())
    }

    fn wal_create_column(
        &mut self,
        table_id: u64,
        column_id: u64,
        column_name: String,
        data_type: crate::backend::schema::DataType,
        constraints: Vec<crate::backend::schema::Constraint>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.wal_create_column(column_id, column_name, data_type, constraints)
    }

    fn wal_drop_column(&mut self, table_id: u64, column_id: u64) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.wal_drop_column(column_id)
    }

    fn wal_insert_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.wal_insert_row(row_id, row)
    }

    fn wal_delete_row(&mut self, table_id: u64, row_id: u128) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.wal_delete_row(row_id)
    }

    fn wal_update_row(
        &mut self,
        table_id: u64,
        row_id: u128,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DataBaseErrors::TableNotFound(table_id.to_string()))?;

        table.wal_update_row(row_id, cells)
    }
}
