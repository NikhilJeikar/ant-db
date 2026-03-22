use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::backend::core::row::{Row, RowID};
use crate::backend::core::table::TableID;
use crate::backend::errors::DataBaseErrors;
use crate::backend::core::transaction::TransactionID;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey {
        refered_table_id: TableID,
        refered_column_id: ColumnID,
    },
    Check(String),
    AutoIncrement,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DataType {
    IntegerU8,
    IntegerU16,
    IntegerU32,
    IntegerU64,
    IntegerU128,
    IntegerI8,
    IntegerI16,
    IntegerI32,
    IntegerI64,
    IntegerI128,
    FloatF32,
    FloatF64,
    String,
    Boolean,
    Bytes,
}

pub type ColumnID = u16;
pub type HashType = u64;

pub struct Column {
    pub column_id: ColumnID,
    pub name: String,
    pub data_type: DataType,
    constraint: BTreeSet<Constraint>,
    index: Option<BTreeMap<HashType, BTreeSet<RowID>>>,
    is_nullable: bool,
}

impl Column {
    pub fn new(column_id: ColumnID, name: String, data_type: DataType, constraint: BTreeSet<Constraint>) -> Self {
        let mut is_nullable = true;
        let mut index = None;
        if constraint.contains(&Constraint::NotNull) {
            is_nullable = false;
        }
        if constraint.contains(&Constraint::PrimaryKey)
            || constraint.contains(&Constraint::Unique)
        {
            index = Some(BTreeMap::new());
        }
        Self {
            column_id,
            name,
            data_type,
            constraint,
            index: index,
            is_nullable,
        }
    }

    pub fn create_index(&mut self, rows: &HashMap<RowID, Row>) -> Result<usize, DataBaseErrors> {
        if self.index.is_some() {
            return Err(DataBaseErrors::IndexExist(self.name.clone()));
        }

        let mut index: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();

        for (row_id, row) in rows {
            for version in row.get_raw_rows() {
                for cell in &version.data {
                    if cell.column_id == self.column_id {
                        index.entry(cell.key()).or_default().insert(*row_id);
                        break;
                    }
                }
            }
        }

        self.index = Some(index);

        Ok(rows.len())
    }

    pub fn drop_index(&mut self) {
        self.index = None;
    }

    pub fn prune(
        &mut self,
        oldest_active_txn: TransactionID,
        rows: &HashMap<RowID, Row>,
    ) {
        let Some(index) = &mut self.index else {
            return;
        };

        let mut empty_keys = Vec::new();

        for (key, row_ids) in index.iter_mut() {
            row_ids.retain(|row_id| {
                if let Some(row) = rows.get(row_id) {
                    row.get_raw_rows().iter().any(|version| {
                        match version.deleted_by {
                            None => true, // still alive
                            Some(del) => del >= oldest_active_txn,
                        }
                    })
                } else {
                    false // row missing
                }
            });

            if row_ids.is_empty() {
                empty_keys.push(*key);
            }
        }

        for key in empty_keys {
            index.remove(&key);
        }
    }
}
