use crate::backend::config::Config;
use crate::backend::core::database::{DataBaseWriteAheadLog, InternalDatabaseSchema};
use crate::backend::core::types::{ColumnId, Constraint, DataType, Index, Row, RowId, TableId};
use bincode::deserialize_from;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Write};

use tracing::{debug, error, info};

use crate::backend::errors::DataBaseErrors;

#[derive(Serialize, Deserialize, Debug)]
pub enum DataBaseOperation {
    CreateTable {
        table_id: TableId,
        name: String,
    },
    DropTable {
        table_id: TableId,
    },
    CreateColumn {
        table_id: TableId,
        column_id: ColumnId,
        name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: Option<Index>,
    },
    DropColumn {
        table_id: TableId,
        column_id: ColumnId,
    },
    CreateIndex {
        table_id: TableId,
        column_id: ColumnId,
        index: Index,
    },
    DropIndex {
        table_id: TableId,
        column_id: ColumnId,
    },
    InsertRow {
        table_id: TableId,
        rows: Vec<(RowId, Row)>,
    },
    DeleteRow {
        table_id: TableId,
        row_ids: Vec<RowId>,
    },
    UpdateRows {
        table_id: TableId,
        row_ids: Vec<RowId>,
        row: Row,
    },
}

#[derive(Debug)]
pub struct WriteAheadLogManager {
    pub config: Config,
    writer: BufWriter<std::fs::File>,
}

impl Default for WriteAheadLogManager {
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

impl WriteAheadLogManager {
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
        if let Err(e) = bincode::serialize_into(&mut self.writer, op) {
            error!("Failed to serialize operation: {}", e);
            return;
        }

        if let Err(e) = self.writer.flush() {
            error!("Failed to flush WAL writer: {}", e);
        }
    }

    pub fn get_wal_size(&self) -> u64 {
        match self.writer.get_ref().metadata() {
            Ok(metadata) => {
                let size = metadata.len();
                debug!("WAL size: {} bytes", size);
                size
            }
            Err(e) => {
                error!("Failed to get WAL metadata: {}", e);
                0
            }
        }
    }

    pub fn clear_wal(&mut self) -> Result<(), DataBaseErrors> {
        info!("Clearing WAL file");
        self.writer.get_ref().set_len(0).map_err(|e| {
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
            db.wal_create_table(table_id, name).map_err(|e| {
                error!("Failed to apply CreateTable operation: {}", e);
                e
            })?
        }
        DataBaseOperation::DropTable { table_id } => db.wal_drop_table(table_id).map_err(|e| {
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
        } => db
            .wal_create_column(table_id, column_id, name, data_type, constraints, index)
            .map_err(|e| {
                error!("Failed to apply CreateColumn operation: {}", e);
                e
            })?,
        DataBaseOperation::DropColumn {
            table_id,
            column_id,
        } => db.wal_drop_column(table_id, column_id).map_err(|e| {
            error!("Failed to apply DropColumn operation: {}", e);
            e
        })?,
        DataBaseOperation::CreateIndex {
            table_id,
            column_id,
            index,
        } => db
            .wal_create_index(table_id, column_id, index)
            .map_err(|e| {
                error!("Failed to apply CreateIndex operation: {}", e);
                e
            })?,
        DataBaseOperation::DropIndex {
            table_id,
            column_id,
        } => db.wal_drop_index(table_id, column_id).map_err(|e| {
            error!("Failed to apply DropIndex operation: {}", e);
            e
        })?,
        DataBaseOperation::InsertRow { table_id, rows } => {
            db.wal_insert_rows(table_id, rows).map_err(|e| {
                error!("Failed to apply InsertRow operation: {}", e);
                e
            })?
        }
        DataBaseOperation::DeleteRow { table_id, row_ids } => {
            db.wal_delete_rows(table_id, row_ids).map_err(|e| {
                error!("Failed to apply DeleteRow operation: {}", e);
                e
            })?
        }
        DataBaseOperation::UpdateRows {
            table_id,
            row_ids,
            row,
        } => db.wal_update_rows(table_id, row_ids, row).map_err(|e| {
            error!("Failed to apply UpdateRows operation: {}", e);
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
                return Err(DataBaseErrors::WalReplayError(format!(
                    "WAL replay failed due to read error: {}",
                    e
                )));
            }
        }
    }

    Ok(())
}
