use crate::backend::core::database::{DataBaseManager, InternalDatabaseSchema};
use crate::backend::core::search::{Projection, SearchCriteria, SortBy};
use crate::backend::core::types::{Constraint, DataType, Row};
use crate::backend::errors::DataBaseErrors;
use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use tracing::{debug, error, info};

// Type alias for database with proper locking
pub type Database = Arc<RwLock<InternalDatabaseSchema>>;

// ==================== Request/Response Types ====================

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateTableRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateTableResponse {
    pub table_id: u64,
    pub name: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateColumnRequest {
    pub column_name: String,
    pub data_type: DataType,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateColumnResponse {
    pub column_id: u64,
    pub column_name: String,
    pub table_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InsertRowsRequest {
    pub rows: Vec<Row>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InsertRowsResponse {
    pub row_count: usize,
    pub table_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteRowsRequest {
    pub row_ids: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteRowsResponse {
    pub deleted_count: usize,
    pub table_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateRowsRequest {
    pub row_ids: Vec<u64>,
    pub new_values: Row,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateRowsResponse {
    pub updated_count: usize,
    pub table_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListTablesResponse {
    pub tables: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorResponse {
    pub error: String,
    pub status: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateIndexResponse {
    pub table_id: u64,
    pub column_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DropIndexResponse {
    pub table_id: u64,
    pub column_id: u64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetRowsRequest {
    pub row_ids: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RowResponse {
    pub row_id: u64,
    pub cells: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetRowsResponse {
    pub table_id: u64,
    pub row_count: usize,
    pub rows: Vec<RowResponse>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnSchemaResponse {
    pub column_id: u64,
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaResponse {
    pub table_id: u64,
    pub table_name: String,
    pub columns: Vec<ColumnSchemaResponse>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchRowsRequest {
    pub criteria: Vec<SearchCriteria>,
    pub projection: Option<Projection>,
    pub sort_by: Option<SortBy>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchRowsResponse {
    pub table_id: u64,
    pub row_count: usize,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub message: String,
}

// ==================== Helper Functions ====================

/// Convert database error to HTTP response
fn error_to_response(error: DataBaseErrors) -> HttpResponse {
    error!("Database error: {}", error);
    HttpResponse::InternalServerError().json(ErrorResponse {
        error: error.to_string(),
        status: 500,
    })
}

/// Handle lock acquisition errors
fn handle_lock_error(
    error: std::sync::PoisonError<std::sync::RwLockWriteGuard<InternalDatabaseSchema>>,
) -> HttpResponse {
    error!("Failed to acquire write lock: {}", error);
    HttpResponse::ServiceUnavailable().json(ErrorResponse {
        error: "Database lock temporarily unavailable".to_string(),
        status: 503,
    })
}

fn handle_read_lock_error(
    error: std::sync::PoisonError<std::sync::RwLockReadGuard<InternalDatabaseSchema>>,
) -> HttpResponse {
    error!("Failed to acquire read lock: {}", error);
    HttpResponse::ServiceUnavailable().json(ErrorResponse {
        error: "Database lock temporarily unavailable".to_string(),
        status: 503,
    })
}

// ==================== Table Endpoints ====================

/// Create a new table
/// POST /api/tables
pub async fn create_table(
    db: web::Data<Database>,
    req: web::Json<CreateTableRequest>,
) -> impl Responder {
    debug!("API: Creating table '{}'", req.name);

    match db.write() {
        Ok(mut db_guard) => match db_guard.create_table(req.name.clone()) {
            Ok(table_id) => {
                info!("API: Table '{}' created with ID {}", req.name, table_id);
                HttpResponse::Created().json(CreateTableResponse {
                    table_id,
                    name: req.name.clone(),
                    message: format!("Table '{}' created successfully", req.name),
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_lock_error(e),
    }
}

/// Drop a table
/// DELETE /api/tables/{table_id}
pub async fn drop_table(db: web::Data<Database>, table_id: web::Path<u64>) -> impl Responder {
    let table_id = table_id.into_inner();
    debug!("API: Dropping table {}", table_id);

    match db.write() {
        Ok(mut db_guard) => match db_guard.drop_table(table_id) {
            Ok(_) => {
                info!("API: Table {} dropped successfully", table_id);
                HttpResponse::Ok().json(serde_json::json!({
                    "message": format!("Table {} dropped successfully", table_id),
                    "table_id": table_id
                }))
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_lock_error(e),
    }
}

/// Get table ID by name
/// GET /api/tables/by-name/{name}
pub async fn get_table_id(db: web::Data<Database>, name: web::Path<String>) -> impl Responder {
    let table_name = name.into_inner();
    debug!("API: Getting table ID for '{}'", table_name);

    match db.read() {
        Ok(db_guard) => match db_guard.get_table_id(table_name.clone()) {
            Ok(table_id) => {
                info!("API: Table ID {} found for '{}'", table_id, table_name);
                HttpResponse::Ok().json(serde_json::json!({
                    "table_name": table_name,
                    "table_id": table_id
                }))
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// List all tables
/// GET /api/tables
pub async fn list_tables(db: web::Data<Database>) -> impl Responder {
    debug!("API: Listing all tables");

    match db.read() {
        Ok(db_guard) => {
            let tables = db_guard.list_tables();
            let count = tables.len();
            info!("API: Found {} tables", count);
            HttpResponse::Ok().json(ListTablesResponse { tables, count })
        }
        Err(e) => handle_read_lock_error(e),
    }
}

/// Get table schema
/// GET /api/tables/{table_id}/schema
pub async fn get_table_schema(db: web::Data<Database>, table_id: web::Path<u64>) -> impl Responder {
    let table_id = table_id.into_inner();
    debug!("API: Getting table schema for {}", table_id);

    match db.read() {
        Ok(db_guard) => {
            match db_guard.get_table_schema(table_id) {
                Ok(schema) => {
                    info!("API: Schema retrieved for table {}", table_id);

                    // Convert to JSON-safe response format (excluding indexes with non-string keys)
                    let columns: Vec<ColumnSchemaResponse> = schema
                        .columns
                        .into_iter()
                        .enumerate()
                        .map(|(idx, col)| ColumnSchemaResponse {
                            column_id: idx as u64,
                            name: col.name,
                            data_type: col.data_type,
                            constraints: col.constraints,
                        })
                        .collect();

                    let response = SchemaResponse {
                        table_id,
                        table_name: schema.name,
                        columns,
                    };

                    HttpResponse::Ok().json(response)
                }
                Err(e) => error_to_response(e),
            }
        }
        Err(e) => handle_read_lock_error(e),
    }
}

/// Get table size (row count)
/// GET /api/tables/{table_id}/size
pub async fn get_table_size(db: web::Data<Database>, table_id: web::Path<u64>) -> impl Responder {
    let table_id = table_id.into_inner();
    debug!("API: Getting table size for {}", table_id);

    match db.read() {
        Ok(db_guard) => match db_guard.get_table_size(table_id) {
            Ok(size) => {
                info!("API: Table {} has {} rows", table_id, size);
                HttpResponse::Ok().json(serde_json::json!({
                    "table_id": table_id,
                    "row_count": size
                }))
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

// ==================== Column Endpoints ====================

/// Create a column in a table
/// POST /api/tables/{table_id}/columns
pub async fn create_column(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<CreateColumnRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    debug!(
        "API: Creating column '{}' in table {}",
        req.column_name, table_id
    );

    match db.read() {
        Ok(db_guard) => {
            match db_guard.create_column(
                table_id,
                req.column_name.clone(),
                req.data_type.clone(),
                req.constraints.clone(),
            ) {
                Ok(column_id) => {
                    info!("API: Column {} created in table {}", column_id, table_id);
                    HttpResponse::Created().json(CreateColumnResponse {
                        column_id,
                        column_name: req.column_name.clone(),
                        table_id,
                        message: format!("Column '{}' created successfully", req.column_name),
                    })
                }
                Err(e) => error_to_response(e),
            }
        }
        Err(e) => handle_read_lock_error(e),
    }
}

/// Drop a column from a table
/// DELETE /api/tables/{table_id}/columns/{column_id}
pub async fn drop_column(db: web::Data<Database>, path: web::Path<(u64, u64)>) -> impl Responder {
    let (table_id, column_id) = path.into_inner();
    debug!("API: Dropping column {} from table {}", column_id, table_id);

    match db.read() {
        Ok(db_guard) => match db_guard.drop_column(table_id, column_id) {
            Ok(_) => {
                info!("API: Column {} dropped from table {}", column_id, table_id);
                HttpResponse::Ok().json(serde_json::json!({
                    "message": format!("Column {} dropped successfully", column_id),
                    "table_id": table_id,
                    "column_id": column_id
                }))
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// Create an index for a column in a table
/// POST /api/tables/{table_id}/columns/{column_id}/index
pub async fn create_index(db: web::Data<Database>, path: web::Path<(u64, u64)>) -> impl Responder {
    let (table_id, column_id) = path.into_inner();
    debug!(
        "API: Creating index for column {} in table {}",
        column_id, table_id
    );

    match db.read() {
        Ok(db_guard) => match db_guard.create_index(table_id, column_id) {
            Ok(_) => {
                info!(
                    "API: Index created for column {} in table {}",
                    column_id, table_id
                );
                HttpResponse::Created().json(CreateIndexResponse {
                    table_id,
                    column_id,
                    message: format!("Index created successfully for column {}", column_id),
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// Drop an index for a column in a table
/// DELETE /api/tables/{table_id}/columns/{column_id}/index
pub async fn drop_index(db: web::Data<Database>, path: web::Path<(u64, u64)>) -> impl Responder {
    let (table_id, column_id) = path.into_inner();
    debug!(
        "API: Dropping index for column {} in table {}",
        column_id, table_id
    );

    match db.read() {
        Ok(db_guard) => match db_guard.drop_index(table_id, column_id) {
            Ok(_) => {
                info!(
                    "API: Index dropped for column {} in table {}",
                    column_id, table_id
                );
                HttpResponse::Ok().json(DropIndexResponse {
                    table_id,
                    column_id,
                    message: format!("Index dropped successfully for column {}", column_id),
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

// ==================== Row Endpoints ====================

/// Insert rows into a table
/// POST /api/tables/{table_id}/rows
pub async fn insert_rows(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<InsertRowsRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    let row_count = req.rows.len();
    debug!("API: Inserting {} rows into table {}", row_count, table_id);

    match db.read() {
        Ok(db_guard) => match db_guard.insert_rows(table_id, req.rows.clone()) {
            Ok(_) => {
                info!("API: {} rows inserted into table {}", row_count, table_id);
                HttpResponse::Created().json(InsertRowsResponse {
                    row_count,
                    table_id,
                    message: format!("{} rows inserted successfully", row_count),
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// Get multiple rows from a table
/// POST /api/tables/{table_id}/rows/get
pub async fn get_rows(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<GetRowsRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    let row_ids = req.row_ids.clone();
    debug!(
        "API: Getting {} rows from table {}",
        row_ids.len(),
        table_id
    );

    match db.read() {
        Ok(db_guard) => match db_guard.get_rows(table_id, row_ids.clone()) {
            Ok(rows) => {
                info!("API: Retrieved {} rows from table {}", rows.len(), table_id);

                let rows = row_ids
                    .into_iter()
                    .zip(rows.into_iter())
                    .map(|(row_id, row)| RowResponse {
                        row_id,
                        cells: row
                            .into_iter()
                            .filter_map(|cell| serde_json::to_value(cell).ok())
                            .collect(),
                    })
                    .collect::<Vec<_>>();

                HttpResponse::Ok().json(GetRowsResponse {
                    table_id,
                    row_count: rows.len(),
                    rows,
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// Delete rows from a table
/// DELETE /api/tables/{table_id}/rows
pub async fn delete_rows(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<DeleteRowsRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    let deleted_count = req.row_ids.len();
    debug!(
        "API: Deleting {} rows from table {}",
        deleted_count, table_id
    );

    match db.read() {
        Ok(db_guard) => match db_guard.delete_rows(table_id, req.row_ids.clone()) {
            Ok(_) => {
                info!(
                    "API: {} rows deleted from table {}",
                    deleted_count, table_id
                );
                HttpResponse::Ok().json(DeleteRowsResponse {
                    deleted_count,
                    table_id,
                    message: format!("{} rows deleted successfully", deleted_count),
                })
            }
            Err(e) => error_to_response(e),
        },
        Err(e) => handle_read_lock_error(e),
    }
}

/// Update rows in a table
/// PUT /api/tables/{table_id}/rows
pub async fn update_rows(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<UpdateRowsRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    let updated_count = req.row_ids.len();
    debug!("API: Updating {} rows in table {}", updated_count, table_id);

    match db.read() {
        Ok(db_guard) => {
            match db_guard.update_rows(table_id, req.row_ids.clone(), req.new_values.clone()) {
                Ok(_) => {
                    info!("API: {} rows updated in table {}", updated_count, table_id);
                    HttpResponse::Ok().json(UpdateRowsResponse {
                        updated_count,
                        table_id,
                        message: format!("{} rows updated successfully", updated_count),
                    })
                }
                Err(e) => error_to_response(e),
            }
        }
        Err(e) => handle_read_lock_error(e),
    }
}

/// Search rows in a table with optional projection and sorting
/// POST /api/tables/{table_id}/search
pub async fn search_rows(
    db: web::Data<Database>,
    table_id: web::Path<u64>,
    req: web::Json<SearchRowsRequest>,
) -> impl Responder {
    let table_id = table_id.into_inner();
    debug!(
        "API: Searching table {} with {} criteria",
        table_id,
        req.criteria.len()
    );

    match db.read() {
        Ok(db_guard) => {
            match db_guard.search_rows(
                table_id,
                req.criteria.clone(),
                req.projection.clone(),
                req.sort_by.clone(),
            ) {
                Ok(rows) => {
                    let row_count = rows.len();
                    info!(
                        "API: Search in table {} returned {} rows",
                        table_id, row_count
                    );

                    // Convert CellSchema to JSON-safe format
                    let json_rows: Vec<Vec<serde_json::Value>> = rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .filter_map(|cell| serde_json::to_value(cell).ok())
                                .collect()
                        })
                        .collect();

                    HttpResponse::Ok().json(SearchRowsResponse {
                        table_id,
                        row_count,
                        rows: json_rows,
                        message: format!("Found {} matching rows", row_count),
                    })
                }
                Err(e) => error_to_response(e),
            }
        }
        Err(e) => handle_read_lock_error(e),
    }
}

// ==================== Health Check ====================

/// Health check endpoint
/// GET /api/health
pub async fn health_check() -> impl Responder {
    debug!("API: Health check");
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "Database API",
        "version": "0.1.0"
    }))
}

// ==================== API Configuration ====================

/// Configure all API routes
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        // Health check
        .route("/api/health", web::get().to(health_check))
        // Table operations
        .route("/api/tables", web::post().to(create_table))
        .route("/api/tables", web::get().to(list_tables))
        .route("/api/tables/by-name/{name}", web::get().to(get_table_id))
        .route("/api/tables/{table_id}", web::delete().to(drop_table))
        .route(
            "/api/tables/{table_id}/schema",
            web::get().to(get_table_schema),
        )
        .route("/api/tables/{table_id}/size", web::get().to(get_table_size))
        // Column operations
        .route(
            "/api/tables/{table_id}/columns",
            web::post().to(create_column),
        )
        .route(
            "/api/tables/{table_id}/columns/{column_id}",
            web::delete().to(drop_column),
        )
        .route(
            "/api/tables/{table_id}/columns/{column_id}/index",
            web::post().to(create_index),
        )
        .route(
            "/api/tables/{table_id}/columns/{column_id}/index",
            web::delete().to(drop_index),
        )
        // Row operations
        .route("/api/tables/{table_id}/rows", web::post().to(insert_rows))
        .route("/api/tables/{table_id}/rows", web::delete().to(delete_rows))
        .route("/api/tables/{table_id}/rows", web::put().to(update_rows))
        .route("/api/tables/{table_id}/rows/get", web::post().to(get_rows))
        .route("/api/tables/{table_id}/search", web::post().to(search_rows));
}
