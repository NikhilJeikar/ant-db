use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::backend::core::row::{Cell, Row, RowID};
use crate::backend::core::table::TableID;
use crate::backend::core::transaction::{Transaction, TransactionID};
use crate::backend::errors::DataBaseErrors;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey {
        refered_table_id: TableID,
        refered_column_id: ColumnID,
    },
    Check,
    AutoIncrement,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DataBaseDataType {
    Null,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub column_id: ColumnID,
    pub name: String,
    pub data_type: DataBaseDataType,
    constraint: BTreeSet<Constraint>,
    index: Option<BTreeMap<HashType, BTreeSet<RowID>>>,
    is_nullable: bool,
}

impl Column {
    pub fn new(
        column_id: ColumnID,
        name: String,
        data_type: DataBaseDataType,
        constraint: BTreeSet<Constraint>,
    ) -> Self {
        let mut is_nullable = true;
        let mut index = None;
        if constraint.contains(&Constraint::NotNull) {
            is_nullable = false;
        }
        if constraint.contains(&Constraint::PrimaryKey) || constraint.contains(&Constraint::Unique)
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

    pub fn update_index(&mut self, cell: Cell, row_id: RowID) {
        match self.index.as_mut() {
            Some(index) => {
                index.entry(cell.key()).or_default().insert(row_id);
            }
            None => {}
        }
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID, rows: &HashMap<RowID, Row>) {
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

    pub fn schema_validation(
        &self,
        cell: Cell,
        rows: &HashMap<RowID, Row>,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        for constraint in &self.constraint {
            match constraint {
                Constraint::NotNull => {
                    if cell.is_null() {
                        return Err(DataBaseErrors::NullValue());
                    }
                }
                Constraint::Unique | Constraint::PrimaryKey => {
                    match &self.index {
                        Some(index) => {
                            for (_, row_ids) in index {
                                for row_id in row_ids {
                                    match rows.get(row_id) {
                                        Some(versioned_row) => {
                                            match versioned_row.get_versioned_row(transaction) {
                                                Some(row) => {
                                                    for lookup_cell in &row.data {
                                                        if lookup_cell.column_id == self.column_id {
                                                            if lookup_cell.data == cell.data {
                                                                return Err(DataBaseErrors::UniqueConstraint(self.name.clone()));
                                                            }
                                                            break;
                                                        }
                                                    }
                                                }
                                                None => {
                                                    return Err(DataBaseErrors::RowNotFound(
                                                        *row_id,
                                                    ));
                                                }
                                            }
                                        }
                                        None => {
                                            return Err(DataBaseErrors::RowNotFound(*row_id));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            return Err(DataBaseErrors::IndexNotFound(self.name.clone()));
                        }
                    }
                }
                Constraint::ForeignKey {
                    refered_table_id: _,
                    refered_column_id: _,
                } => {
                    //TODO: Implement it as part of the Database overhaul
                }
                Constraint::Check => {
                    //TODO: Implement it as part of the Check logic sepratly
                }
                Constraint::AutoIncrement => {
                    //NOTE: This is not validation this is a property so this won't be implented here rather create a preprocessing before schema validation and enter it through there
                }
            }
        }
        Ok(())
    }
}
