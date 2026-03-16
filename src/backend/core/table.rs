use std::collections::BTreeMap;

use crate::backend::config::InternalStateManager;
use crate::backend::errors::DataBaseErrors;
use crate::backend::schema::{Constraint, DataType, DecodedData};
use crate::backend::storage::wal::{DataBaseOperation, WALManager, WalOps};
use rmp_serde::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CellStructure {
    pub column_id: u64,
    pub data: DecodedData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

// Internal structures for efficient storage and retrieval
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalCell {
    pub column_id: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalTableSchema {
    pub table_id: u64,
    pub name: String,
    pub columns: BTreeMap<u64, ColumnSchema>,
    pub rows: BTreeMap<u128, Vec<InternalCell>>,
    pub next_row_id: u128,

    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WALManager>>,
}

pub trait TableManager {
    fn create_column(
        &mut self,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<(), DataBaseErrors>;
    fn drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors>;
    fn insert_rows(&mut self, rows: Vec<Vec<CellStructure>>) -> Result<(), DataBaseErrors>;
    fn delete_rows(&mut self, row_ids: Vec<u128>) -> Result<(), DataBaseErrors>;
    fn update_rows(
        &mut self,
        row_ids: Vec<u128>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors>;

    fn get_size(&self) -> usize;
    fn get_schema(&self) -> Result<TableSchema, DataBaseErrors>;

    // replay operations for WAL recovery
    fn wal_create_column(
        &mut self,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_insert_row(
        &mut self,
        row_id: u128,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_delete_row(&mut self, row_id: u128) -> Result<(), DataBaseErrors>;
    fn wal_update_row(
        &mut self,
        row_id: u128,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;

    // temporary method for testing this should be replaced with a more flexible update method that can update specific columns in the future
    fn get_row(&self, row_id: u128) -> Result<Vec<CellStructure>, DataBaseErrors>;
}

impl InternalTableSchema {
    pub fn new(
        table_id: u64,
        name: String,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WALManager>>,
    ) -> Self {
        InternalTableSchema {
            table_id,
            name,
            columns: BTreeMap::new(),
            rows: BTreeMap::new(),
            next_row_id: 1,
            internal_state_manager,
            wal_manager,
        }
    }

    fn encode_cell(value: &DecodedData) -> Result<Vec<u8>, DataBaseErrors> {
        to_vec(value).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))
    }

    fn decode_cell(bytes: &[u8]) -> Result<DecodedData, DataBaseErrors> {
        from_slice(bytes).map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))
    }
}

impl WalOps for InternalTableSchema {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        let mut wal = self
            .wal_manager
            .lock()
            .map_err(|_| DataBaseErrors::WalLockError)?;
        if !self.internal_state_manager.read().unwrap().is_wal_replaying {
            info!("Writing to WAL {:?}", operation);
            wal.append(&operation);
        }
        Ok(())
    }
}

impl TableManager for InternalTableSchema {
    fn create_column(
        &mut self,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<(), DataBaseErrors> {
        if self.columns.iter().any(|c| c.1.name == column_name) {
            return Err(DataBaseErrors::ColumnAlreadyExists(column_name));
        }
        let column_id = self.columns.len() as u64 + 1;
        self.log_operation(DataBaseOperation::CreateColumn {
            table_id: self.table_id,
            column_id,
            name: column_name.clone(),
            data_type: data_type.clone(),
            constraints: constraints.clone(),
        })?;
        self.columns.insert(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
            },
        );
        Ok(())
    }

    fn drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id.to_string()));
        }
        self.log_operation(DataBaseOperation::DropColumn {
            table_id: self.table_id,
            column_id,
        })?;
        self.columns.remove(&column_id);

        for row in self.rows.values_mut() {
            if let Some(pos) = row.iter().position(|c| c.column_id == column_id) {
                row.remove(pos);
            }
        }

        Ok(())
    }

    fn insert_rows(&mut self, rows: Vec<Vec<CellStructure>>) -> Result<(), DataBaseErrors> {
        for row in rows {
            let mut cells = Vec::with_capacity(row.len());
            let row_id = self.next_row_id;
            self.next_row_id += 1;
            for v in row.iter() {
                cells.push(InternalCell {
                    column_id: v.column_id,
                    data: Self::encode_cell(&v.data)?,
                });
            }

            self.log_operation(DataBaseOperation::InsertRow {
                table_id: self.table_id,
                row_id,
                row: cells.clone(),
            })?;
            self.rows.insert(row_id, cells);
        }
        Ok(())
    }

    fn delete_rows(&mut self, row_ids: Vec<u128>) -> Result<(), DataBaseErrors> {
        for row_id in &row_ids {
            if !self.rows.iter().any(|r| *r.0 == *row_id) {
                return Err(DataBaseErrors::RowNotFound(*row_id));
            }
        }
        for row_id in &row_ids {
            self.log_operation(DataBaseOperation::DeleteRow {
                table_id: self.table_id,
                row_id: *row_id,
            })?;
            self.rows.remove(row_id);
        }
        Ok(())
    }

    fn update_rows(
        &mut self,
        row_ids: Vec<u128>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors> {
        for row_id in &row_ids {
            if !self.rows.iter().any(|r| *r.0 == *row_id) {
                return Err(DataBaseErrors::RowNotFound(*row_id));
            }
        }

        let mut cells = Vec::with_capacity(new_values.len());

        for v in new_values.iter() {
            cells.push(InternalCell {
                column_id: v.column_id,
                data: Self::encode_cell(&v.data)?,
            });
        }

        for row_id in &row_ids {
            self.log_operation(DataBaseOperation::UpdateRow {
                table_id: self.table_id,
                row_id: *row_id,
                cells: cells.clone(),
            })?;
            if let Some(row) = self.rows.get_mut(row_id) {
                *row = cells.clone();
            }
        }
        Ok(())
    }

    fn get_row(&self, row_id: u128) -> Result<Vec<CellStructure>, DataBaseErrors> {
        let row = self
            .rows
            .iter()
            .find(|r| *r.0 == row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?;

        let mut decoded_cells = Vec::with_capacity(row.1.len());
        for cell in row.1.iter() {
            decoded_cells.push(CellStructure {
                column_id: cell.column_id,
                data: Self::decode_cell(&cell.data)?,
            });
        }
        Ok(decoded_cells)
    }

    fn get_size(&self) -> usize {
        self.rows.len()
    }
    fn get_schema(&self) -> Result<TableSchema, DataBaseErrors> {
        let columns = self.columns.values().cloned().collect();
        Ok(TableSchema {
            name: self.name.clone(),
            columns,
        })
    }

    fn wal_create_column(
        &mut self,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<(), DataBaseErrors> {
        self.columns.insert(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
            },
        );
        Ok(())
    }
    fn wal_drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors> {
        self.columns.remove(&column_id);

        for row in self.rows.values_mut() {
            if let Some(pos) = row.iter().position(|c| c.column_id == column_id) {
                row.remove(pos);
            }
        }

        Ok(())
    }
    fn wal_insert_row(
        &mut self,
        row_id: u128,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        // Update next_row_id to ensure future inserts don't have collisions
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
        }
        self.rows.insert(row_id, row);
        Ok(())
    }
    fn wal_delete_row(&mut self, row_id: u128) -> Result<(), DataBaseErrors> {
        self.rows.remove(&row_id);
        Ok(())
    }
    fn wal_update_row(
        &mut self,
        row_id: u128,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        if let Some(row) = self.rows.get_mut(&row_id) {
            *row = cells;
        }
        Ok(())
    }
}
