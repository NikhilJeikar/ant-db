use ahash::AHashMap;
use nohash_hasher::BuildNoHashHasher;
use ordered_float::NotNan;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash};

use crate::backend::core::column::{ColumnID, DataBaseDataType};
use crate::backend::core::transaction::{Transaction, TransactionHeader, TransactionID};

pub type RowID = u64;
pub type RowData = AHashMap<ColumnID, DataBaseDataEntry, BuildNoHashHasher<ColumnID>>;

fn serialize_row_data<S>(data: &RowData, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ordered: BTreeMap<ColumnID, &DataBaseDataEntry> = data.iter().map(|(&k, v)| (k, v)).collect();
    ordered.serialize(serializer)
}

fn deserialize_row_data<'de, D>(deserializer: D) -> Result<RowData, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<ColumnID, DataBaseDataEntry>::deserialize(deserializer)?;
    let mut row_data = AHashMap::with_hasher(BuildNoHashHasher::default());
    row_data.extend(intermediate.into_iter());
    Ok(row_data)
}

fn row_data_from_map<S>(data: AHashMap<ColumnID, DataBaseDataEntry, S>) -> RowData
where
    S: BuildHasher,
{
    let mut row_data = AHashMap::with_hasher(BuildNoHashHasher::default());
    row_data.extend(data);
    row_data
}

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

    pub fn key(&self, _column_id: ColumnID) -> u64 {
        match self {
            DataBaseDataEntry::Null => 0,
            DataBaseDataEntry::IntegerU8(value) => *value as u64,
            DataBaseDataEntry::IntegerU16(value) => *value as u64,
            DataBaseDataEntry::IntegerU32(value) => *value as u64,
            DataBaseDataEntry::IntegerU64(value) => *value,
            DataBaseDataEntry::IntegerU128(value) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::Hasher;

                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                hasher.finish()
            }
            DataBaseDataEntry::IntegerI8(value) => *value as u64,
            DataBaseDataEntry::IntegerI16(value) => *value as u64,
            DataBaseDataEntry::IntegerI32(value) => *value as u64,
            DataBaseDataEntry::IntegerI64(value) => *value as u64,
            DataBaseDataEntry::IntegerI128(value) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::Hasher;

                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                hasher.finish()
            }
            DataBaseDataEntry::FloatF32(value) => value.into_inner().to_bits() as u64,
            DataBaseDataEntry::FloatF64(value) => value.into_inner().to_bits(),
            DataBaseDataEntry::String(value) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::Hasher;

                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                hasher.finish()
            }
            DataBaseDataEntry::Boolean(value) => *value as u64,
            DataBaseDataEntry::Bytes(value) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::Hasher;

                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, DataBaseDataEntry::Null)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InternalRow {
    pub transaction_header: TransactionHeader,
    #[serde(serialize_with = "serialize_row_data", deserialize_with = "deserialize_row_data")]
    pub data: RowData,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Row {
    versions: Vec<InternalRow>,
}

impl Row {
    pub fn new<S>(
        transaction: &Transaction,
        data: AHashMap<ColumnID, DataBaseDataEntry, S>,
    ) -> Self
    where
        S: BuildHasher,
    {
        let row_data = row_data_from_map(data);
        let mut row = Row { versions: Vec::new() };
        row.versions.push(InternalRow {
            transaction_header: TransactionHeader {
                created_by: transaction.transaction_id,
                deleted_by: None,
            },
            data: row_data,
        });
        row
    }

    pub fn remove(&mut self, transaction: &Transaction) {
        for row in self.versions.iter_mut().rev() {
            if row.transaction_header.is_visible(transaction) {
                row.transaction_header.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }
    }

    pub fn update<S>(
        &mut self,
        transaction: &Transaction,
        data: AHashMap<ColumnID, DataBaseDataEntry, S>,
    ) where
        S: BuildHasher,
    {
        for row in self.versions.iter_mut().rev() {
            if row.transaction_header.is_visible(transaction) {
                row.transaction_header.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }

        self.versions.push(InternalRow {
            transaction_header: TransactionHeader {
                created_by: transaction.transaction_id,
                deleted_by: None,
            },
            data: row_data_from_map(data),
        });
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID) {
        self.versions.retain(|row| match row.transaction_header.deleted_by {
            None => true,
            Some(del) => del >= oldest_active_txn,
        });
    }

    pub fn rollback_transaction(&mut self, transaction: &Transaction) {
        for version in self.versions.iter_mut() {
            if version.transaction_header.created_by == transaction.transaction_id {
                if version.transaction_header.deleted_by.is_none() {
                    version.transaction_header.deleted_by = Some(transaction.transaction_id);
                }
            }

            if version.transaction_header.deleted_by == Some(transaction.transaction_id)
                && version.transaction_header.created_by != transaction.transaction_id
            {
                version.transaction_header.deleted_by = None;
            }
        }
    }

    pub fn get_raw_rows(&self) -> &Vec<InternalRow> {
        &self.versions
    }

    pub fn get_versioned_row(&self, transaction: &Transaction) -> Option<&InternalRow> {
        self.versions
            .iter()
            .rev()
            .find(|row| row.transaction_header.is_visible(transaction))
    }
}
