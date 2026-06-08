use crate::backend::core::pg_wire::run_pgwire_server;
use crate::backend::handler::setup;
use tracing::{error, info};
mod backend;

#[tokio::main]
async fn main() {
    info!("Initializing database system...");
    let (internal_state_manager, db_arc, _logger_handle) = setup();
    let snapshot_path = internal_state_manager
        .read()
        .unwrap()
        .config
        .snapshot_path
        .clone();

    let server_addr = "127.0.0.1:5432";
    info!("Starting pgwire server at {}", server_addr);
    tokio::select! {
        () = run_pgwire_server(db_arc.clone(), server_addr) => {}
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received, saving database snapshot...");
        }
    }

    if let Ok(db) = db_arc.read() {
        if let Err(err) = db.save_snapshot(&snapshot_path) {
            error!("Failed to save database snapshot on shutdown: {err}");
        } else {
            info!("Database snapshot saved to {snapshot_path}");
        }
    }
}
