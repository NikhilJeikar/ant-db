use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

use crate::backend::core::column::{ColumnID, HashType};
use crate::backend::core::transaction::{Transaction,TransactionID};

pub type RowID = u64;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum DataBaseDataType {
    IntegerU8(u8),
    IntegerU16(u16),
    IntegerU32(u32),
    IntegerU64(u64),
    IntegerU128(u128),
    IntegerI8(i8),
    IntegerI16(i16),
    IntegerI32(i32),
    IntegerI64(i64),
    IntegerI128(i128),
    FloatF32(NotNan<f32>),
    FloatF64(NotNan<f64>),
    String(String),
    Boolean(bool),
    Bytes(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cell {
    pub column_id: ColumnID,
    pub data: DataBaseDataType,
}

impl Cell {
    pub fn key(&self) -> HashType {
        let mut hasher = XxHash64::with_seed(0);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InternalRow {
    pub created_by: TransactionID,
    pub deleted_by: Option<TransactionID>,
    pub data: Vec<Cell>,
}

impl InternalRow {
    fn is_visible(&self, transaction: &Transaction) -> bool {
        transaction
            .snapshot
            .is_visible(self.created_by, self.deleted_by)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    data: Vec<InternalRow>,
}

impl Row {
    pub fn new(transaction_id: TransactionID, data: Vec<Cell>) -> Self {
        let mut row = Row { data: Vec::new() };
        row.data.push(InternalRow {
            created_by: transaction_id,
            deleted_by: None,
            data,
        });
        row
    }

    pub fn remove(&mut self, transaction: &Transaction) {
        for row in self.data.iter_mut().rev() {
            if row.is_visible(transaction) {
                row.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }
    }

    pub fn update(&mut self, transaction: &Transaction, data: Vec<Cell>) {
        for row in self.data.iter_mut().rev() {
            if row.is_visible(transaction) {
                row.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }

        self.data.push(InternalRow {
            created_by: transaction.transaction_id,
            deleted_by: None,
            data,
        });
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID) {
        self.data.retain(|row| {
            match row.deleted_by {
                None => true,
                Some(del) => del >= oldest_active_txn,
            }
        });
    }

    pub fn get_raw_rows(&self) -> &Vec<InternalRow> {
        &self.data
    }

    pub fn get_versioned_row(&self, transaction: &Transaction) -> Option<&InternalRow> {
        self.data
            .iter()
            .rev()
            .find(|row| row.is_visible(transaction))
    }
}