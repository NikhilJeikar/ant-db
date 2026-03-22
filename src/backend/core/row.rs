use crate::backend::core::types::{ColumnId, HashType};
use crate::backend::errors::DataBaseErrors;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

pub type TransactionID = u64;

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
    pub column_id: ColumnId,
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
    created_by: TransactionID,
    deleted_by: Option<TransactionID>,
    data: Vec<Cell>,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    data: Vec<InternalRow>,
}

impl Row {
    fn new(&mut self, transaction_id: TransactionID, data: Vec<Cell>) -> Self {
        let mut row = Row { data: Vec::new() };
        row.data.push(InternalRow {
            created_by: transaction_id,
            deleted_by: None,
            data,
        });
        row
    }
    fn remove(&mut self, refered_transaction_id: TransactionID, transaction_id: TransactionID) {
        // NOTE: This could turn bad when Multiple people try to delete the entry, We only take who did it first
        for row in &mut self.data {
            if row.created_by <= refered_transaction_id && row.deleted_by.is_none() {
                row.deleted_by = Some(transaction_id);
            }
        }
    }
    fn update(&mut self, refered_transaction_id: TransactionID, transaction_id: TransactionID, data: Vec<Cell>) {
        for row in &mut self.data {
            // NOTE: This could turn bad when Multiple people try to update the entry, We only take who did it first
            if row.created_by == refered_transaction_id && row.deleted_by.is_none() {
                row.deleted_by = Some(transaction_id);
            }
        }
        self.data.push(
            InternalRow {
            created_by: transaction_id,
            deleted_by: None,
            data,
        }
        );
    }

    fn prune(&mut self, transaction_id: TransactionID) {
        self.data.retain(|row| {
            row.deleted_by.map_or(true, |del| del > transaction_id)
        });
    }
}
