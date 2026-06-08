use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::info;

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
    let db = Arc::new(RwLock::new(Database::new(
        config.database.clone(),
        internal_state_manager.clone(),
    )));
    db.write().unwrap().bind_tables();

    if auto_vacuum_interval_secs > 0 {
        let db_for_vacuum = db.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(auto_vacuum_interval_secs.max(1));
            loop {
                tokio::time::sleep(interval).await;
                if let Ok(mut database) = db_for_vacuum.write() {
                    database.auto_vacuum();
                }
            }
        });
    }

    (internal_state_manager, db, logger_handle)
}

