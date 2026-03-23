use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

use crate::backend::core::column::{ColumnID, DataBaseDataType, HashType};
use crate::backend::core::transaction::{Transaction, TransactionID};

pub type RowID = u64;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum DataBaseDataEntry {
    Null,
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

impl DataBaseDataEntry {
    pub fn data_type(&self) -> DataBaseDataType {
        match self {
            DataBaseDataEntry::Null => DataBaseDataType::Null,

            DataBaseDataEntry::IntegerU8(_) => DataBaseDataType::IntegerU8,
            DataBaseDataEntry::IntegerU16(_) => DataBaseDataType::IntegerU16,
            DataBaseDataEntry::IntegerU32(_) => DataBaseDataType::IntegerU32,
            DataBaseDataEntry::IntegerU64(_) => DataBaseDataType::IntegerU64,
            DataBaseDataEntry::IntegerU128(_) => DataBaseDataType::IntegerU128,

            DataBaseDataEntry::IntegerI8(_) => DataBaseDataType::IntegerI8,
            DataBaseDataEntry::IntegerI16(_) => DataBaseDataType::IntegerI16,
            DataBaseDataEntry::IntegerI32(_) => DataBaseDataType::IntegerI32,
            DataBaseDataEntry::IntegerI64(_) => DataBaseDataType::IntegerI64,
            DataBaseDataEntry::IntegerI128(_) => DataBaseDataType::IntegerI128,

            DataBaseDataEntry::FloatF32(_) => DataBaseDataType::FloatF32,
            DataBaseDataEntry::FloatF64(_) => DataBaseDataType::FloatF64,

            DataBaseDataEntry::String(_) => DataBaseDataType::String,
            DataBaseDataEntry::Boolean(_) => DataBaseDataType::Boolean,
            DataBaseDataEntry::Bytes(_) => DataBaseDataType::Bytes,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cell {
    pub column_id: ColumnID,
    pub data: DataBaseDataEntry,
}

impl Cell {
    pub fn key(&self) -> HashType {
        let mut hasher = XxHash64::with_seed(0);
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_null(&self) -> bool {
        matches!(self.data, DataBaseDataEntry::Null)
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
        self.data.retain(|row| match row.deleted_by {
            None => true,
            Some(del) => del >= oldest_active_txn,
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
