use std::sync::{Arc, Mutex, RwLock};
use tracing::{error, info};
use actix_web::{web, App, HttpServer};

use crate::backend::config::{Config, InternalStateManager};
use crate::backend::core::database::InternalDatabaseSchema;
use crate::backend::logger::{LoggerHandle, init_logger};
use crate::backend::storage::snapshot::{read_snapshot_with_context, start_snapshot_monitor};
use crate::backend::storage::wal::{WALManager, replay_wal};
use crate::backend::api;

pub fn setup() -> (
    Arc<RwLock<InternalStateManager>>,
    Arc<Mutex<WALManager>>,
    Arc<RwLock<InternalDatabaseSchema>>,
    Arc<std::sync::atomic::AtomicBool>,
    LoggerHandle,
) {
    let config = Config::from_file("db_config.toml");
    let internal_state_manager = Arc::new(RwLock::new(InternalStateManager::new(config.clone())));
    let wal_manager = Arc::new(Mutex::new(WALManager::new(
        internal_state_manager.read().unwrap().config.clone(),
    )));
    let logger_handle = init_logger(
        internal_state_manager
            .read()
            .unwrap()
            .config
            .log_path
            .as_str(),
    );
    info!("Configuration loaded: {:?}", config);
    let mut db = if internal_state_manager
        .read()
        .unwrap()
        .config
        .snapshot_path
        .is_empty()
    {
        info!("No snapshot path provided, starting with empty database");
        InternalDatabaseSchema::new(internal_state_manager.clone(), wal_manager.clone())
    } else {
        info!(
            "Reading snapshot from {}",
            internal_state_manager.read().unwrap().config.snapshot_path
        );
        match read_snapshot_with_context(
            internal_state_manager
                .read()
                .unwrap()
                .config
                .snapshot_path
                .as_str(),
            internal_state_manager.clone(),
            wal_manager.clone(),
        ) {
            Ok(db) => {
                info!("Snapshot loaded successfully");
                db
            }
            Err(e) => {
                error!(
                    "Failed to read snapshot: {}, starting with empty database",
                    e
                );
                InternalDatabaseSchema::new(internal_state_manager.clone(), wal_manager.clone())
            }
        }
    };
    internal_state_manager.write().unwrap().is_wal_replaying = true;

    match replay_wal(
        &mut db,
        internal_state_manager
            .read()
            .unwrap()
            .config
            .wal_path
            .as_str(),
    ) {
        Ok(_) => {
            info!("WAL replay completed successfully");
        }
        Err(e) => {
            error!("Failed to replay WAL: {}", e);
        }
    }

    internal_state_manager.write().unwrap().is_wal_replaying = false;
    db.wal_manager = wal_manager.clone();

    // Wrap database in Arc<RwLock<>> for thread-safe sharing
    let db_arc = Arc::new(RwLock::new(db));

    // Start the snapshot monitor thread
    let snapshot_monitor_shutdown = start_snapshot_monitor(
        wal_manager.clone(),
        db_arc.clone(),
        config.wal_sync_interval
    );

    info!("Snapshot monitor started with 5 second check interval");

    (
        internal_state_manager,
        wal_manager,
        db_arc,
        snapshot_monitor_shutdown,
        logger_handle,
    )
}

/// Start the HTTP API server
/// This function starts the Actix-web server on the specified address and port
pub async fn start_api_server(db: Arc<RwLock<InternalDatabaseSchema>>, host: &str, port: u16) -> std::io::Result<()> {
    let address = format!("{}:{}", host, port);
    info!("Starting HTTP API server on {}", address);
    
    let address_clone = address.clone();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .configure(api::configure_routes)
    })
    .bind(&address_clone)?
    .run()
    .await
}
