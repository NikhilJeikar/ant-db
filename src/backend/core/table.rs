use std::collections::{BTreeMap, HashSet};

use crate::backend::config::InternalStateManager;
use crate::backend::errors::DataBaseErrors;
use crate::backend::schema::{Constraint, DataType, DecodedData};
use crate::backend::storage::wal::{DataBaseOperation, WALManager, WriteAheadLogBase};
use rmp_serde::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{info, error};

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
    pub index: BTreeMap<Vec<u8>, u64>,
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
    pub rows: BTreeMap<u64, Vec<InternalCell>>,
    pub next_row_id: u64,
    pub next_column_id: u64,

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
    ) -> Result<u64, DataBaseErrors>;
    fn drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors>;

    fn insert_rows(&mut self, rows: Vec<Vec<CellStructure>>) -> Result<(), DataBaseErrors>;
    fn delete_rows(&mut self, row_ids: Vec<u64>) -> Result<(), DataBaseErrors>;
    fn update_rows(
        &mut self,
        row_ids: Vec<u64>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors>;

    fn get_row(&self, row_id: u64) -> Result<Vec<CellStructure>, DataBaseErrors>;

    fn get_size(&self) -> usize;
    fn get_schema(&self) -> Result<TableSchema, DataBaseErrors>;
}
pub trait TableWriteAheadLog: WriteAheadLogBase {
    // replay operations for WAL recovery
    fn wal_create_column(
        &mut self,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: BTreeMap<Vec<u8>, u64>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_insert_row(
        &mut self,
        row_id: u64,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_delete_row(&mut self, row_id: u64) -> Result<(), DataBaseErrors>;
    fn wal_update_row(
        &mut self,
        row_id: u64,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors>;
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
            next_column_id: 1,
            internal_state_manager,
            wal_manager,
        }
    }

    pub fn inject_contexts(
        &mut self,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WALManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        self.wal_manager = wal_manager;
    }

    fn encode_cell(value: &DecodedData) -> Result<Vec<u8>, DataBaseErrors> {
        to_vec(value).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))
    }

    fn decode_cell(bytes: &[u8]) -> Result<DecodedData, DataBaseErrors> {
        from_slice(bytes).map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))
    }

    fn duplicate_cells(cells: &Vec<InternalCell>) -> Option<u64> {
        let mut present_columns:HashSet<u64> = HashSet::new();
        for cell in cells {
            if present_columns.contains(&cell.column_id) {
                return Some(cell.column_id);
            }
            present_columns.insert(cell.column_id);
        }
        return None;
    }

    fn validate_data_type(data: &DecodedData, expected_type: &DataType) -> bool {
        match (data, expected_type) {
            (DecodedData::IntegerU8(_), DataType::IntegerU8) => true,
            (DecodedData::IntegerU16(_), DataType::IntegerU16) => true,
            (DecodedData::IntegerU32(_), DataType::IntegerU32) => true,
            (DecodedData::IntegerU64(_), DataType::IntegerU64) => true,
            (DecodedData::IntegerU128(_), DataType::IntegerU128) => true,
            (DecodedData::IntegerI8(_), DataType::IntegerI8) => true,
            (DecodedData::IntegerI16(_), DataType::IntegerI16) => true,
            (DecodedData::IntegerI32(_), DataType::IntegerI32) => true,
            (DecodedData::IntegerI64(_), DataType::IntegerI64) => true,
            (DecodedData::IntegerI128(_), DataType::IntegerI128) => true,
            (DecodedData::FloatF32(_), DataType::FloatF32) => true,
            (DecodedData::FloatF64(_), DataType::FloatF64) => true,
            (DecodedData::String(_), DataType::String) => true,
            (DecodedData::Boolean(_), DataType::Boolean) => true,
            (DecodedData::Bytes(_), DataType::Bytes) => true,
            _ => false,
        }
    }

    fn get_data_type_name(data: &DecodedData) -> &'static str {
        match data {
            DecodedData::IntegerU8(_) => "IntegerU8",
            DecodedData::IntegerU16(_) => "IntegerU16",
            DecodedData::IntegerU32(_) => "IntegerU32",
            DecodedData::IntegerU64(_) => "IntegerU64",
            DecodedData::IntegerU128(_) => "IntegerU128",
            DecodedData::IntegerI8(_) => "IntegerI8",
            DecodedData::IntegerI16(_) => "IntegerI16",
            DecodedData::IntegerI32(_) => "IntegerI32",
            DecodedData::IntegerI64(_) => "IntegerI64",
            DecodedData::IntegerI128(_) => "IntegerI128",
            DecodedData::FloatF32(_) => "FloatF32",
            DecodedData::FloatF64(_) => "FloatF64",
            DecodedData::String(_) => "String",
            DecodedData::Boolean(_) => "Boolean",
            DecodedData::Bytes(_) => "Bytes",
        }
    }

    fn get_expected_type_name(data_type: &DataType) -> &'static str {
        match data_type {
            DataType::IntegerU8 => "IntegerU8",
            DataType::IntegerU16 => "IntegerU16",
            DataType::IntegerU32 => "IntegerU32",
            DataType::IntegerU64 => "IntegerU64",
            DataType::IntegerU128 => "IntegerU128",
            DataType::IntegerI8 => "IntegerI8",
            DataType::IntegerI16 => "IntegerI16",
            DataType::IntegerI32 => "IntegerI32",
            DataType::IntegerI64 => "IntegerI64",
            DataType::IntegerI128 => "IntegerI128",
            DataType::FloatF32 => "FloatF32",
            DataType::FloatF64 => "FloatF64",
            DataType::String => "String",
            DataType::Boolean => "Boolean",
            DataType::Bytes => "Bytes",
        }
    }

    fn internal_insert_column(&mut self, column_id: u64, column_schema: ColumnSchema) {
        self.columns.insert(column_id, column_schema);
    }

    fn internal_drop_column(&mut self, column_id: u64) {
        self.columns.remove(&column_id);

        for row in self.rows.values_mut() {
            if let Some(pos) = row.iter().position(|c| c.column_id == column_id) {
                row.remove(pos);
            }
        }
    }

    fn internal_insert_row(
        &mut self,
        row_id: u64,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        if let Some(col) = Self::duplicate_cells(&row) {
            return Err(DataBaseErrors::RowColumnDuplicate(row_id, col));
        }

        let row_id_u64 = row_id as u64;
        for cell in &row {
            let column = match self.columns.get_mut(&cell.column_id) {
                Some(c) => c,
                None => return Err(DataBaseErrors::ColumnNotFound(cell.column_id)),
            };

            column.index.insert(cell.data.clone(), row_id_u64);
        }

        self.rows.insert(row_id_u64, row);

        Ok(())
    }

    fn internal_delete_row(&mut self, row_id: u64) -> Result<(), DataBaseErrors> {
        let row_id_u64 = row_id as u64;
        let row = self
            .rows
            .remove(&row_id_u64)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?;

        for cell in &row {
            let column = self
                .columns
                .get_mut(&cell.column_id)
                .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;

            column.index.remove(&cell.data);
        }

        self.rows.remove(&row_id);

        Ok(())
    }

    fn internal_update_row(
        &mut self,
        row_id: u64,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        if let Some(col) = Self::duplicate_cells(&row) {
            return Err(DataBaseErrors::RowColumnDuplicate(row_id, col));
        }

        let prev_row = self
            .rows
            .get(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?
            .clone();

        for cell in &prev_row {
            let column = match self.columns.get_mut(&cell.column_id) {
                Some(c) => c,
                None => return Err(DataBaseErrors::ColumnNotFound(cell.column_id)),
            };

            column.index.remove(&cell.data);
        }

        for cell in &row {
            let column = match self.columns.get_mut(&cell.column_id) {
                Some(c) => c,
                None => return Err(DataBaseErrors::ColumnNotFound(cell.column_id)),
            };

            column.index.insert(cell.data.clone(), row_id);
        }

        self.rows.insert(row_id, row);

        Ok(())
    }
}

impl WriteAheadLogBase for InternalTableSchema {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        let mut wal = self
            .wal_manager
            .lock()
            .map_err(|e| {
                error!("Failed to acquire WAL lock: {}", e);
                DataBaseErrors::WalLockError
            })?;
        
        let is_replaying = self
            .internal_state_manager
            .read()
            .map(|guard| guard.is_wal_replaying)
            .unwrap_or(false);
        
        if !is_replaying {
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
    ) -> Result<u64, DataBaseErrors> {
        if self.columns.iter().any(|c| c.1.name == column_name) {
            return Err(DataBaseErrors::ColumnAlreadyExists(column_name));
        }

        let column_id = self.next_column_id;
        self.next_column_id += 1;

        self.log_operation(DataBaseOperation::CreateColumn {
            table_id: self.table_id,
            column_id,
            name: column_name.clone(),
            data_type: data_type.clone(),
            constraints: constraints.clone(),
            index: BTreeMap::new(),
        })?;

        self.internal_insert_column(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
                index: BTreeMap::new(),
            },
        );
        Ok(column_id)
    }

    fn drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        self.log_operation(DataBaseOperation::DropColumn {
            table_id: self.table_id,
            column_id,
        })?;

        self.internal_drop_column(column_id);

        Ok(())
    }

    fn insert_rows(&mut self, rows: Vec<Vec<CellStructure>>) -> Result<(), DataBaseErrors> {
        for row in rows {
            let mut cells = Vec::with_capacity(row.len());

            let row_id = self.next_row_id;
            self.next_row_id += 1;

            // Validate data types for all cells in the row
            for v in row.iter() {
                let column = self.columns.get(&v.column_id)
                    .ok_or(DataBaseErrors::ColumnNotFound(v.column_id))?;
                
                if !Self::validate_data_type(&v.data, &column.data_type) {
                    return Err(DataBaseErrors::DataTypeMismatch(
                        v.column_id,
                        Self::get_expected_type_name(&column.data_type).to_string(),
                        Self::get_data_type_name(&v.data).to_string(),
                    ));
                }
            }

            // Encode cells after validation
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

            let _ = match self.internal_insert_row(row_id, cells) {
                Ok(_) => continue,
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    fn delete_rows(&mut self, row_ids: Vec<u64>) -> Result<(), DataBaseErrors> {
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

            let _ = match self.internal_delete_row(*row_id) {
                Ok(_) => continue,
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    fn update_rows(
        &mut self,
        row_ids: Vec<u64>,
        new_values: Vec<CellStructure>,
    ) -> Result<(), DataBaseErrors> {
        for row_id in &row_ids {
            if !self.rows.iter().any(|r| *r.0 == *row_id) {
                return Err(DataBaseErrors::RowNotFound(*row_id));
            }
        }

        // Validate data types for all cells
        for cell in new_values.iter() {
            let column = self.columns.get(&cell.column_id)
                .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;
            
            if !Self::validate_data_type(&cell.data, &column.data_type) {
                return Err(DataBaseErrors::DataTypeMismatch(
                    cell.column_id,
                    Self::get_expected_type_name(&column.data_type).to_string(),
                    Self::get_data_type_name(&cell.data).to_string(),
                ));
            }
        }

        let mut cells = Vec::with_capacity(new_values.len());

        for cell in new_values.iter() {
            cells.push(InternalCell {
                column_id: cell.column_id,
                data: Self::encode_cell(&cell.data)?,
            });
        }

        for row_id in &row_ids {
            self.log_operation(DataBaseOperation::UpdateRow {
                table_id: self.table_id,
                row_id: *row_id,
                cells: cells.clone(),
            })?;

            let _ = match self.internal_update_row(*row_id, cells.clone()) {
                Ok(_) => continue,
                Err(e) => return Err(e),
            };
        }
        Ok(())
    }

    fn get_row(&self, row_id: u64) -> Result<Vec<CellStructure>, DataBaseErrors> {
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
}

impl TableWriteAheadLog for InternalTableSchema {
    fn wal_create_column(
        &mut self,
        column_id: u64,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: BTreeMap<Vec<u8>, u64>,
    ) -> Result<(), DataBaseErrors> {
        if self.columns.iter().any(|c| c.1.name == column_name) {
            return Err(DataBaseErrors::ColumnAlreadyExists(column_name));
        }

        self.internal_insert_column(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
                index,
            },
        );
        Ok(())
    }
    fn wal_drop_column(&mut self, column_id: u64) -> Result<(), DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        self.internal_drop_column(column_id);

        Ok(())
    }
    fn wal_insert_row(
        &mut self,
        row_id: u64,
        row: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
        }
        self.internal_insert_row(row_id, row)
    }
    fn wal_delete_row(&mut self, row_id: u64) -> Result<(), DataBaseErrors> {
        self.internal_delete_row(row_id)
    }
    fn wal_update_row(
        &mut self,
        row_id: u64,
        cells: Vec<InternalCell>,
    ) -> Result<(), DataBaseErrors> {
        self.internal_update_row(row_id, cells)
    }
}
