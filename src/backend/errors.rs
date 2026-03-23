use crate::backend::core::types::Row;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataBaseErrors {
    // Column-related errors
    #[error("Column '{0}' already exists in the table.")]
    ColumnAlreadyExists(String),
    #[error("Column '{0}' not found in the table.")]
    ColumnNotFound(u64),
    #[error("Index not found for column {0}.")]
    IndexNotFound(String),
    #[error("Index already exist for column {0}.")]
    IndexExist(String),
    
    //Constraint errors
    #[error("Value cannot be nullable.")]
    NullValue(),
    #[error("Unique Constraint failed for {0}")]
    UniqueConstraint(String),
    


    // Row-related errors
    #[error("Row with ID '{0}' not found in the table.")]
    RowNotFound(u64),
    #[error("Multiple entries for row({0:?}) same column {1}")]
    RowColumnDuplicate(Row, u64),
    #[error("Data type mismatch for column {0}: expected {1}, got {2}")]
    DataTypeMismatch(u64, &'static str, &'static str),

    // Table-related errors
    #[error("Table '{0}' already exists.")]
    TableAlreadyExists(String),
    #[error("Table '{0}' not found.")]
    TableNotFound(String),
    #[error("Table '{0}' not found.")]
    TableIDNotFound(u64),

    // Serialization errors
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    // WAL-related errors
    #[error("WAL lock error.")]
    WalLockError,
    #[error("WAL replay error: {0}")]
    WalReplayError(String),

    // I/O errors
    #[error("I/O error: {0}")]
    IOError(String),
}
