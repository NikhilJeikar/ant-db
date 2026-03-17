use crate::backend::handler::setup;
use tracing::info;
mod backend;

#[tokio::main]
async fn main() {
    // Initialize database system
    info!("Initializing database system...");
    let (_internal_state_manager, _wal_manager, db_arc, _snapshot_monitor_shutdown, _logger_handle) = setup();
    
    info!("Database initialization complete.");
    info!("");
    info!("Starting HTTP API server on http://127.0.0.1:8080");
    info!("");
    info!("Available API Endpoints:");
    info!("═══════════════════════════════════════════════════════════════════════════");
    info!("HEALTH:");
    info!("  GET  /api/health");
    info!("");
    info!("TABLES:");
    info!("  POST   /api/tables                     - Create a new table");
    info!("  GET    /api/tables                     - List all tables");
    info!("  DELETE /api/tables/{{table_id}}            - Drop a table");
    info!("  GET    /api/tables/{{table_id}}/schema     - Get table schema");
    info!("  GET    /api/tables/{{table_id}}/size       - Get row count");
    info!("");
    info!("COLUMNS:");
    info!("  POST   /api/tables/{{table_id}}/columns             - Create column");
    info!("  DELETE /api/tables/{{table_id}}/columns/{{col_id}}  - Drop column");
    info!("");
    info!("ROWS:");
    info!("  POST   /api/tables/{{table_id}}/rows        - Insert rows");
    info!("  GET    /api/tables/{{table_id}}/rows/{{row_id}} - Get a row");
    info!("  DELETE /api/tables/{{table_id}}/rows        - Delete rows");
    info!("  PUT    /api/tables/{{table_id}}/rows        - Update rows");
    info!("═══════════════════════════════════════════════════════════════════════════");
    info!("");
    info!("Example requests:");
    info!("  curl -X POST http://127.0.0.1:8080/api/tables -H 'Content-Type: application/json' \\");
    info!("       -d '{{\"name\": \"users\"}}'");
    info!("");
    
    // Start the API server
    if let Err(e) = backend::handler::start_api_server(db_arc, "127.0.0.1", 8080).await {
        eprintln!("API server error: {}", e);
    }
}
