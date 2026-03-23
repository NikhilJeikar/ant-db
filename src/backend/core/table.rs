use std::{collections::{BTreeMap, HashSet}, sync::{Arc, Mutex, RwLock, atomic::{AtomicU16, AtomicU64}}};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::backend::{config::InternalStateManager, core::{
    column::{Column, ColumnID, DataBaseDataType},
    row::{Cell, DataBaseDataEntry, Row, RowID},
}, errors::DataBaseErrors, storage::wal::{DataBaseOperation, WriteAheadLogBase, WriteAheadLogManager}};

pub type TableID = u64;

#[derive(Debug, Serialize, Deserialize)]
struct Table {
    table_id: TableID,

    name: String,
    rows: BTreeMap<RowID, Row>,
    columns: BTreeMap<ColumnID, Column>,

    next_row_id: AtomicU64,
    next_column_id: AtomicU16,

    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WriteAheadLogManager>>,
}

impl Table {
    pub fn new(
        table_id: TableID,
        name: String,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) -> Self {
        Table {
            table_id,
            name,
            columns: BTreeMap::new(),
            rows: BTreeMap::new(),
            next_row_id: AtomicU64::new(0),
            next_column_id: AtomicU16::new(0),
            internal_state_manager,
            wal_manager,
        }
    }

    pub fn inject_contexts(
        &mut self,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        self.wal_manager = wal_manager;
    }

    fn duplicate_cells(cells: &Vec<Cell>) -> Option<ColumnID> {
        let mut present_columns: HashSet<ColumnID> = HashSet::new();
        for cell in cells {
            if present_columns.contains(&cell.column_id) {
                return Some(cell.column_id);
            }
            present_columns.insert(cell.column_id);
        }
        return None;
    }

    fn validate_data_type(data: &DataBaseDataEntry, expected_type: &DataBaseDataType, is_nullable: bool) -> bool {
        let data_type = &data.data_type();
        data_type == expected_type || (is_nullable && *data_type == DataBaseDataType::Null)
    }
}

impl WriteAheadLogBase for Table {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        let mut wal = self.wal_manager.lock().map_err(|e| {
            error!("Failed to acquire WAL lock: {}", e);
            DataBaseErrors::WalLockError
        })?;

        let is_replaying = self
            .internal_state_manager
            .read()
            .map(|guard| guard.is_wal_replaying)
            .unwrap_or(false);

        if !is_replaying {
            wal.append(&operation);
        }
        Ok(())
    }
}