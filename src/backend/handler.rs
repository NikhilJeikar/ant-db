use std::sync::{Arc, RwLock};
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
    let db = Arc::new(RwLock::new(Database::new(internal_state_manager.clone())));
    (internal_state_manager, db, logger_handle)
}

