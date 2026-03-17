use crate::backend::handler::setup;
use tracing::info;
mod backend;

#[tokio::main]
async fn main() {
    // Initialize database system
    info!("Initializing database system...");
    let (_internal_state_manager, _wal_manager, db_arc, _snapshot_monitor_shutdown, _logger_handle) =
        setup();


    // Start the API server
    if let Err(e) = backend::handler::start_api_server(db_arc, "127.0.0.1", 8080).await {
        eprintln!("API server error: {}", e);
    }
}
