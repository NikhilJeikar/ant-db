
use rmp_serde::to_vec;
use rmp_serde::from_slice;
use std::fs;


use crate::backend::core::database::InternalDatabaseSchema;
use crate::backend::errors::DataBaseErrors;

pub fn write_snapshot(db: &InternalDatabaseSchema, path: &str) -> Result<(), DataBaseErrors> {
    let bytes = to_vec(db).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
    fs::write(path, bytes).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
    Ok(())
}

pub fn read_snapshot(path: &str) -> Result<InternalDatabaseSchema, DataBaseErrors> {
    let bytes = fs::read(path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
    let db: InternalDatabaseSchema =
        from_slice(&bytes).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
    Ok(db)
}   