use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{error, info};

use crate::backend::config::{Config, InternalStateManager};

use crate::backend::logger::{LoggerHandle, init_logger};
use crate::backend::core::database::Database;


pub fn setup() -> (
    Arc<RwLock<InternalStateManager>>,
    Arc<RwLock<Database>>,
    LoggerHandle,
) {
    let config = Config::from_file("db_config.toml");
    let auto_vacuum_interval_secs = config.auto_vacuum_interval_secs;
    let internal_state_manager = Arc::new(RwLock::new(InternalStateManager::new(config.clone())));
    let logger_handle = init_logger(
        internal_state_manager
            .read()
            .unwrap()
            .config
            .log_path
            .as_str(),
    );
    info!("Configuration loaded: {:?}", config);
    if config.database.is_empty() {
        panic!("`database` must be set in db_config.toml");
    }
    let snapshot_path = config.snapshot_path.clone();
    let database_name = config.database.clone();
    let db = match Database::load_from_snapshot(
        &snapshot_path,
        &database_name,
        internal_state_manager.clone(),
    ) {
        Ok(Some(db)) => {
            info!("Loaded database from snapshot at {snapshot_path}");
            db
        }
        Ok(None) => {
            info!("No snapshot found at {snapshot_path}, starting with empty database");
            let mut db = Database::new(database_name, internal_state_manager.clone());
            db.bind_tables();
            db
        }
        Err(err) => {
            panic!("Failed to load database snapshot from {snapshot_path}: {err}");
        }
    };
    let db = Arc::new(RwLock::new(db));

    if auto_vacuum_interval_secs > 0 {
        let db_for_vacuum = db.clone();
        let snapshot_path = snapshot_path.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(auto_vacuum_interval_secs.max(1));
            loop {
                tokio::time::sleep(interval).await;
                if let Ok(database) = db_for_vacuum.read() {
                    database.auto_vacuum();
                    if let Err(err) = database.save_snapshot(&snapshot_path) {
                        error!("Failed to save database snapshot: {err}");
                    }
                }
            }
        });
    }

    (internal_state_manager, db, logger_handle)
}

