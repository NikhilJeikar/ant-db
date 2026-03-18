use crate::backend::config::InternalStateManager;
use crate::backend::errors::DataBaseErrors;
use crate::backend::storage::wal::WriteAheadLogManager;
use ordered_float::NotNan;
use rmp_serde::to_vec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::{Arc, Mutex, RwLock};
use twox_hash::XxHash64;

pub type TableId = u64;
pub type RowId = u64;
pub type ColumnId = u64;

pub type HashType = u64;

pub type Index = BTreeMap<HashType, Vec<RowId>>;

pub type Row = Vec<CellSchema>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum DecodedData {
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

impl DecodedData {
    pub fn type_name(&self) -> &'static str {
        match self {
            DecodedData::IntegerU8(_) => "U8",
            DecodedData::IntegerU16(_) => "U16",
            DecodedData::IntegerU32(_) => "U32",
            DecodedData::IntegerU64(_) => "U64",
            DecodedData::IntegerU128(_) => "128",
            DecodedData::IntegerI8(_) => "I8",
            DecodedData::IntegerI16(_) => "I16",
            DecodedData::IntegerI32(_) => "I32",
            DecodedData::IntegerI64(_) => "I64",
            DecodedData::IntegerI128(_) => "I128",
            DecodedData::FloatF32(_) => "FloatF32",
            DecodedData::FloatF64(_) => "FloatF64",
            DecodedData::String(_) => "String",
            DecodedData::Boolean(_) => "Boolean",
            DecodedData::Bytes(_) => "Bytes",
        }
    }
    pub fn data_type(&self) -> DataType {
        match self {
            DecodedData::IntegerU8(_) => DataType::IntegerU8,
            DecodedData::IntegerU16(_) => DataType::IntegerU16,
            DecodedData::IntegerU32(_) => DataType::IntegerU32,
            DecodedData::IntegerU64(_) => DataType::IntegerU64,
            DecodedData::IntegerU128(_) => DataType::IntegerU128,
            DecodedData::IntegerI8(_) => DataType::IntegerI8,
            DecodedData::IntegerI16(_) => DataType::IntegerI16,
            DecodedData::IntegerI32(_) => DataType::IntegerI32,
            DecodedData::IntegerI64(_) => DataType::IntegerI64,
            DecodedData::IntegerI128(_) => DataType::IntegerI128,
            DecodedData::FloatF32(_) => DataType::FloatF32,
            DecodedData::FloatF64(_) => DataType::FloatF64,
            DecodedData::String(_) => DataType::String,
            DecodedData::Boolean(_) => DataType::Boolean,
            DecodedData::Bytes(_) => DataType::Bytes,
        }
    }
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

impl DataType {
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::IntegerU8 => "U8",
            DataType::IntegerU16 => "U16",
            DataType::IntegerU32 => "U32",
            DataType::IntegerU64 => "U64",
            DataType::IntegerU128 => "U128",
            DataType::IntegerI8 => "I8",
            DataType::IntegerI16 => "I16",
            DataType::IntegerI32 => "I32",
            DataType::IntegerI64 => "I64",
            DataType::IntegerI128 => "I128",
            DataType::FloatF32 => "FloatF32",
            DataType::FloatF64 => "FloatF64",
            DataType::String => "String",
            DataType::Boolean => "Boolean",
            DataType::Bytes => "Bytes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey(String, String), // Referenced table and column
    Check(String),
    AutoIncrement,
}

// Schema definitions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CellSchema {
    pub column_id: ColumnId,
    pub data: DecodedData,
}

impl CellSchema {
    pub fn get_binary_data(&self) -> Result<Vec<u8>, DataBaseErrors> {
        to_vec(&self.data).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))
    }

    pub fn get_key(&self) -> HashType {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(&self.get_binary_data().unwrap());
        hasher.finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
    pub index: Option<Index>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalTableSchema {
    pub table_id: TableId,

    pub name: String,
    pub columns: BTreeMap<ColumnId, ColumnSchema>,
    pub rows: BTreeMap<RowId, Row>,

    pub next_row_id: RowId,
    pub next_column_id: ColumnId,

    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub wal_manager: Arc<Mutex<WriteAheadLogManager>>,
}
