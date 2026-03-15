use std::sync::{Arc, RwLock, Mutex};
use tracing::{info, error};

use crate::backend::config::{Config, InternalStateManager};
use crate::backend::logger::init_logger;
use crate::backend::storage::snapshot::read_snapshot;
use crate::backend::storage::wal::replay_wal;
use crate::backend::core::database::InternalDatabaseSchema;

pub fn setup() -> (Arc<RwLock<InternalStateManager>>, Arc<Mutex<crate::backend::storage::wal::WALManager>>, InternalDatabaseSchema) {
    let config = Config::from_file("config.toml");
    let internal_state_manager = Arc::new(RwLock::new(InternalStateManager::new(config)));
    let wal_manager = Arc::new(Mutex::new(crate::backend::storage::wal::WALManager::new(internal_state_manager.read().unwrap().config.wal_path.as_str())));
    let _logger_guard = init_logger(internal_state_manager.read().unwrap().config.log_path.as_str());

    let mut db =  if internal_state_manager.read().unwrap().config.snapshot_path.is_empty() {
        info!("No snapshot path provided, starting with empty database");
        InternalDatabaseSchema::new(internal_state_manager.clone(), wal_manager.clone())
    } else {
        info!("Reading snapshot from {}", internal_state_manager.read().unwrap().config.snapshot_path);
        match read_snapshot(internal_state_manager.read().unwrap().config.snapshot_path.as_str()) {
            Ok(db) => {
                info!("Snapshot loaded successfully");
                db
            },
            Err(e) => {
                error!("Failed to read snapshot: {}, starting with empty database", e);
                InternalDatabaseSchema::new(internal_state_manager.clone(), wal_manager.clone())
            }
        }
    };
    internal_state_manager.write().unwrap().is_wal_replaying = true;

    match replay_wal(&mut db, internal_state_manager.read().unwrap().config.wal_path.as_str()) {
        Ok(_) => {
            info!("WAL replay completed successfully");
        }
        Err(e) => {
            error!("Failed to replay WAL: {}", e);
        }
    }

    internal_state_manager.write().unwrap().is_wal_replaying = false;
    (internal_state_manager, wal_manager, db)
}