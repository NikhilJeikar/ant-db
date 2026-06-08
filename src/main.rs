use crate::backend::core::pg_wire::run_pgwire_server;
use crate::backend::handler::setup;
use tracing::info;
mod backend;

#[tokio::main]
async fn main() {
    info!("Initializing database system...");
    let (_internal_state_manager, db_arc, _logger_handle) = setup();

    let server_addr = "127.0.0.1:5432";
    info!("Starting pgwire server at {}", server_addr);
    run_pgwire_server(db_arc, server_addr).await;
}
