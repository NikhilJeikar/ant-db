use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
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
    FloatF32(f32),
    FloatF64(f64),
    String(String),
    Boolean(bool),
    Bytes(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey(String, String), // Referenced table and column
    Check(String),
    AutoIncrement,
}
