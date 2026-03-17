use std::collections::BTreeMap;

use crate::backend::config::Config;
use crate::backend::core::database::{InternalDatabaseSchema, DataBaseWriteAheadLog};
use crate::backend::core::table::InternalCell;
use crate::backend::schema::{Constraint, DataType};
use bincode::deserialize_from;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Write};

use tracing::{debug, error, info};

use crate::backend::errors::DataBaseErrors;

#[derive(Serialize, Deserialize, Debug)]
pub enum DataBaseOperation {
    CreateTable {
        table_id: u64,
        name: String,
    },
    DropTable {
        table_id: u64,
    },
    CreateColumn {
        table_id: u64,
        column_id: u64,
        name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: BTreeMap<Vec<u8>, u128>,
    },
    DropColumn {
        table_id: u64,
        column_id: u64,
    },
    InsertRow {
        table_id: u64,
        row_id: u128,
        row: Vec<InternalCell>,
    },
    DeleteRow {
        table_id: u64,
        row_id: u128,
    },
    UpdateRow {
        table_id: u64,
        row_id: u128,
        cells: Vec<InternalCell>,
    },
}

#[derive(Debug)]
pub struct WALManager {
    pub config: Config,
    writer: BufWriter<std::fs::File>,
}

impl Default for WALManager {
    fn default() -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal.log")
            .expect("Failed to open WAL file");

        Self {
            config: Config::default(),
            writer: BufWriter::new(file),
        }
    }
}

impl WALManager {
    pub fn new(config: Config) -> Self {
        info!("Initializing WAL Manager with path: {}", config.wal_path);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(config.wal_path.as_str())
            .unwrap();

        Self {
            config,
            writer: BufWriter::new(file),
        }
    }

    pub fn append(&mut self, op: &DataBaseOperation) {
        debug!("Appending operation to WAL: {:?}", op);
        bincode::serialize_into(&mut self.writer, op)
            .map_err(|e| debug!("Failed to serialize operation: {}", e))
            .ok();
        self.writer.flush()
            .map_err(|e| debug!("Failed to flush WAL: {}", e))
            .ok();
        debug!("Operation successfully written to WAL");
    }

    pub fn get_wal_size(&self) -> u64 {
        match self.writer.get_ref().metadata() {
            Ok(metadata) => {
                let size = metadata.len();
                debug!("WAL size: {} bytes", size);
                size
            },
            Err(e) => {
                error!("Failed to get WAL metadata: {}", e);
                0
            }
        }
    }

    pub fn clear_wal(&mut self) -> Result<(), DataBaseErrors> {
        info!("Clearing WAL file");
        self.writer
            .get_ref()
            .set_len(0)
            .map_err(|e| {
                error!("Failed to clear WAL: {}", e);
                DataBaseErrors::IOError(e.to_string())
            })?;
        info!("WAL file cleared successfully");
        Ok(())
    }
}

pub trait WriteAheadLogBase {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors>;
}

fn apply_operation(
    db: &mut InternalDatabaseSchema,
    op: DataBaseOperation,
) -> Result<(), DataBaseErrors> {
    match op {
        DataBaseOperation::CreateTable { table_id, name } => {
            db.wal_create_table(table_id, name)
                .map_err(|e| {
                    error!("Failed to apply CreateTable operation: {}", e);
                    e
                })?
        }
        DataBaseOperation::DropTable { table_id } => db.wal_drop_table(table_id)
            .map_err(|e| {
                error!("Failed to apply DropTable operation: {}", e);
                e
            })?,
        DataBaseOperation::CreateColumn {
            table_id,
            column_id,
            name,
            data_type,
            constraints,
            index,
        } => {
            db.wal_create_column(table_id, column_id, name, data_type, constraints, index)
                .map_err(|e| {
                    error!("Failed to apply CreateColumn operation: {}", e);
                    e
                })?
        }
        DataBaseOperation::DropColumn {
            table_id,
            column_id,
        } => db.wal_drop_column(table_id, column_id)
            .map_err(|e| {
                error!("Failed to apply DropColumn operation: {}", e);
                e
            })?,
        DataBaseOperation::InsertRow {
            table_id,
            row_id,
            row,
        } => db.wal_insert_row(table_id, row_id, row)
            .map_err(|e| {
                error!("Failed to apply InsertRow operation: {}", e);
                e
            })?,
        DataBaseOperation::DeleteRow { table_id, row_id } => {
            db.wal_delete_row(table_id, row_id)
                .map_err(|e| {
                    error!("Failed to apply DeleteRow operation: {}", e);
                    e
                })?
        }
        DataBaseOperation::UpdateRow {
            table_id,
            row_id,
            cells,
        } => db.wal_update_row(table_id, row_id, cells)
            .map_err(|e| {
                error!("Failed to apply UpdateRow operation: {}", e);
                e
            })?,
    };
    Ok(())
}

pub fn replay_wal(db: &mut InternalDatabaseSchema, wal_path: &str) -> Result<(), DataBaseErrors> {
    let file = File::open(wal_path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
    let mut reader = BufReader::new(file);

    loop {
        match deserialize_from::<_, DataBaseOperation>(&mut reader) {
            Ok(op) => {
                info!("Replaying operation: {:?}", op);
                let ops = apply_operation(db, op);
                match ops {
                    Ok(_) => {
                        debug!("Operation applied successfully");
                    }
                    Err(e) => {
                        error!("Failed to apply operation: {}", e);
                        return Err(DataBaseErrors::WalReplayError(e.to_string()));
                    }
                }
            }
            Err(e) => {
                if let bincode::ErrorKind::Io(ref io_err) = *e {
                    if io_err.kind() == ErrorKind::UnexpectedEof {
                        debug!("End of WAL is reached");
                        break;
                    }
                }
                error!("Failed to read operation from WAL: {}", e);
                return Err(DataBaseErrors::WalReplayError(format!("WAL replay failed due to read error: {}", e)));
            }
        }
    }

    Ok(())
}
