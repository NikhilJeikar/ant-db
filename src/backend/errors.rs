use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataBaseErrors {
    // Query-related errors
    #[error("Invalid query syntax.")]
    InvalidQuerySyntax,

    // Column-related errors
    #[error("Row has {0} cells but expected {1} cells based on the table schema.")]
    RowColumnMismatch(usize, usize),
    #[error("Column '{0}' already exists in the table.")]
    ColumnAlreadyExists(String),
    #[error("Constraint violation for column '{0}': {1}.")]
    ConstraintViolation(String, String),
    #[error("Column '{0}' not found in the table.")]
    ColumnNotFound(String),
    #[error("Maximum column limit of {0} exceeded.")]
    MaximumColumnLimitExceeded(usize),

    // Row-related errors
    #[error("Row with ID '{0}' not found in the table.")]
    RowNotFound(u128),

    // Table-related errors
    #[error("Table '{0}' already exists.")]
    TableAlreadyExists(String),
    #[error("Table '{0}' not found.")]
    TableNotFound(String),

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
