use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use ahash::AHashMap;
use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use ordered_float::NotNan;
use sqlparser::ast::{
    AlterTableOperation,
    ColumnOption,
    ColumnOptionDef,
    CopySource,
    CopyTarget,
    DataType,
    Expr as SqlExpr,
    Ident,
    ObjectName,
    ObjectType,
    OrderByExpr,
    ShowStatementFilter,
    Statement,
    TableConstraint,
};
use tokio::net::TcpListener;

use bytes::Bytes;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::{send_copy_out_response, CopyHandler};
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{CopyResponse, DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::{CommandComplete, NoticeResponse};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use crate::backend::core::column::{ColumnID, Constraint};
use crate::backend::core::copy::{
    append_copy_data, encode_copy_payload, parse_copy_options, parse_copy_rows, resolve_copy_columns,
    CopyColumnSpec, CopyOptions,
};
use crate::backend::core::database::Database;
use crate::backend::core::plan::{self, ExecutionResult};
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::table::{Table, TableID};
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

fn normalize_identifier(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

fn normalize_simple_query(query: &str) -> String {
    let query = query.trim().trim_end_matches('\0');
    if query.ends_with(';') {
        return query.to_string();
    }

    let upper = query.to_ascii_uppercase();
    if upper.contains(" FROM STDIN")
        && !upper.contains(" TO ")
        && upper.starts_with("COPY ")
    {
        format!("{query};")
    } else {
        query.to_string()
    }
}

fn sql_like_matches(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    let value = if case_insensitive {
        value.to_ascii_lowercase()
    } else {
        value.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_lowercase()
    } else {
        pattern.to_string()
    };

    if pattern == "%" {
        return true;
    }

    let starts_with_wild = pattern.starts_with('%');
    let ends_with_wild = pattern.ends_with('%');
    let trimmed = pattern.trim_matches('%');

    if starts_with_wild && ends_with_wild {
        return value.contains(trimmed);
    }
    if starts_with_wild {
        return value.ends_with(trimmed);
    }
    if ends_with_wild {
        return value.starts_with(trimmed);
    }
    value == trimmed
}

fn parse_table_name(name: &sqlparser::ast::ObjectName) -> Result<String, DataBaseErrors> {
    match name.0.last() {
        Some(ident) => Ok(normalize_identifier(&ident.value)),
        None => Err(DataBaseErrors::QueryError(
            "Missing table name in SQL statement".into(),
        )),
    }
}

fn parse_index_name(name: &sqlparser::ast::ObjectName) -> Result<String, DataBaseErrors> {
    parse_table_name(name)
}

fn parse_index_column(column: &OrderByExpr) -> Result<String, DataBaseErrors> {
    match &column.expr {
        SqlExpr::Identifier(ident) => Ok(normalize_identifier(&ident.value)),
        SqlExpr::CompoundIdentifier(idents) => match idents.last() {
            Some(ident) => Ok(normalize_identifier(&ident.value)),
            None => Err(DataBaseErrors::QueryError(
                "Missing column name in CREATE INDEX".into(),
            )),
        },
        _ => Err(DataBaseErrors::QueryError(
            "CREATE INDEX supports only simple column references".into(),
        )),
    }
}

fn parse_create_table_data_type(
    data_type: &DataType,
) -> Result<crate::backend::core::column::DataBaseDataType, DataBaseErrors> {
    use crate::backend::core::column::DataBaseDataType;
    Ok(match data_type {
        DataType::Boolean => DataBaseDataType::Boolean,
        DataType::Text
        | DataType::Character(_)
        | DataType::Char(_)
        | DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_)
        | DataType::CharacterLargeObject(_)
        | DataType::CharLargeObject(_)
        | DataType::Clob(_)
        | DataType::Uuid => DataBaseDataType::String,
        DataType::Binary(_) | DataType::Varbinary(_) | DataType::Blob(_) | DataType::Bytes(_) => {
            DataBaseDataType::Bytes
        }
        DataType::Float(_) | DataType::Float4 | DataType::Float32 | DataType::Float64 | DataType::Real | DataType::Float8 | DataType::Double | DataType::DoublePrecision => {
            DataBaseDataType::FloatF64
        }
        DataType::Int2(_) | DataType::SmallInt(_) | DataType::Int16 => DataBaseDataType::IntegerI16,
        DataType::UnsignedInt2(_) | DataType::UnsignedSmallInt(_) => DataBaseDataType::IntegerU16,
        DataType::Int4(_) | DataType::Integer(_) | DataType::Int(_) | DataType::Int8(_) | DataType::Int32 => DataBaseDataType::IntegerI32,
        DataType::UnsignedInt(_) | DataType::UnsignedInt4(_) | DataType::UnsignedInteger(_) => DataBaseDataType::IntegerU32,
        DataType::Int64 | DataType::BigInt(_) | DataType::UnsignedBigInt(_) | DataType::UnsignedInt8(_) => DataBaseDataType::IntegerI64,
        DataType::Int128 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::UInt128 => DataBaseDataType::IntegerI128,
        DataType::Numeric(_) | DataType::Decimal(_) | DataType::BigNumeric(_) | DataType::BigDecimal(_) | DataType::Dec(_) => {
            DataBaseDataType::FloatF64
        }
        DataType::Timestamp(..) | DataType::Datetime(_) => DataBaseDataType::Timestamp,
        _ => {
            return Err(DataBaseErrors::QueryError(format!(
                "Unsupported column type in CREATE TABLE: {data_type:?}",
            )))
        }
    })
}

fn parse_table_name_from_object(name: &ObjectName) -> Result<String, DataBaseErrors> {
    name.0
        .last()
        .map(|ident| normalize_identifier(&ident.value))
        .ok_or_else(|| DataBaseErrors::QueryError("Missing table name in foreign key".into()))
}

fn resolve_foreign_key_reference(
    db: &Database,
    foreign_table: &ObjectName,
    referred_columns: &[Ident],
    transaction: &Transaction,
) -> Result<(TableID, ColumnID), DataBaseErrors> {
    if referred_columns.len() != 1 {
        return Err(DataBaseErrors::QueryError(
            "Composite foreign keys are not supported".into(),
        ));
    }

    let table_name = parse_table_name_from_object(foreign_table)?;
    let column_name = normalize_identifier(&referred_columns[0].value);
    let table = db
        .get_table(table_name.clone())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name))?;
    let table_guard = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
    let column_map = table_guard.get_visible_column_map(transaction)?;
    let column_id = column_map.get(&column_name).copied().ok_or_else(|| {
        DataBaseErrors::QueryError(format!(
            "Foreign key references unknown column '{column_name}'",
        ))
    })?;
    Ok((table_guard.table_id(), column_id))
}

fn parse_column_constraints(
    options: &[ColumnOptionDef],
    db: &Database,
    transaction: &Transaction,
) -> Result<BTreeSet<Constraint>, DataBaseErrors> {
    let mut constraints = BTreeSet::new();
    for option in options {
        match &option.option {
            ColumnOption::NotNull => {
                constraints.insert(Constraint::NotNull);
            }
            ColumnOption::Null => {}
            ColumnOption::Unique { is_primary, .. } => {
                if *is_primary {
                    constraints.insert(Constraint::PrimaryKey);
                } else {
                    constraints.insert(Constraint::Unique);
                }
            }
            ColumnOption::DialectSpecific(tokens) => {
                let text = tokens
                    .iter()
                    .map(|token| token.to_string().to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                if text == "auto_increment" || text == "autoincrement" {
                    constraints.insert(Constraint::AutoIncrement);
                } else {
                    return Err(DataBaseErrors::QueryError(format!(
                        "Unsupported dialect-specific column option: {text}",
                    )));
                }
            }
            ColumnOption::Default(_) => {
                return Err(DataBaseErrors::QueryError(
                    "DEFAULT expressions are unsupported in CREATE TABLE".into(),
                ));
            }
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                ..
            } => {
                let (refered_table_id, refered_column_id) =
                    resolve_foreign_key_reference(db, foreign_table, referred_columns, transaction)?;
                constraints.insert(Constraint::ForeignKey {
                    refered_table_id,
                    refered_column_id,
                });
            }
            ColumnOption::Check(_) => {
                constraints.insert(Constraint::Check);
            }
            ColumnOption::Generated { .. } => {
                return Err(DataBaseErrors::QueryError(
                    "GENERATED columns are unsupported".into(),
                ));
            }
            ColumnOption::Comment(_) | ColumnOption::CharacterSet(_) | ColumnOption::OnUpdate(_) => {}
            _ => {
                return Err(DataBaseErrors::QueryError(format!(
                    "Unsupported column option in CREATE TABLE: {option:?}",
                )));
            }
        }
    }
    Ok(constraints)
}

fn build_table_constraint_map(
    constraints: &[TableConstraint],
    column_name_map: &HashMap<String, ColumnID>,
    db: &Database,
    transaction: &Transaction,
) -> Result<HashMap<ColumnID, BTreeSet<Constraint>>, DataBaseErrors> {
    let mut column_constraints: HashMap<ColumnID, BTreeSet<Constraint>> = HashMap::new();
    for constraint in constraints {
        match constraint {
            TableConstraint::Unique { columns, .. } => {
                for ident in columns {
                    let column_name = normalize_identifier(&ident.value);
                    let column_id = column_name_map.get(&column_name).ok_or_else(|| {
                        DataBaseErrors::QueryError(format!(
                            "Unknown column '{}' in UNIQUE table constraint",
                            column_name,
                        ))
                    })?;
                    column_constraints
                        .entry(*column_id)
                        .or_default()
                        .insert(Constraint::Unique);
                }
            }
            TableConstraint::PrimaryKey { columns, .. } => {
                for ident in columns {
                    let column_name = normalize_identifier(&ident.value);
                    let column_id = column_name_map.get(&column_name).ok_or_else(|| {
                        DataBaseErrors::QueryError(format!(
                            "Unknown column '{}' in PRIMARY KEY table constraint",
                            column_name,
                        ))
                    })?;
                    column_constraints
                        .entry(*column_id)
                        .or_default()
                        .insert(Constraint::PrimaryKey);
                }
            }
            TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
                ..
            } => {
                if columns.len() != 1 {
                    return Err(DataBaseErrors::QueryError(
                        "Composite foreign keys are not supported".into(),
                    ));
                }
                let local_name = normalize_identifier(&columns[0].value);
                let local_id = column_name_map.get(&local_name).ok_or_else(|| {
                    DataBaseErrors::QueryError(format!(
                        "Unknown column '{local_name}' in FOREIGN KEY table constraint",
                    ))
                })?;
                let (refered_table_id, refered_column_id) =
                    resolve_foreign_key_reference(db, foreign_table, referred_columns, transaction)?;
                column_constraints
                    .entry(*local_id)
                    .or_default()
                    .insert(Constraint::ForeignKey {
                        refered_table_id,
                        refered_column_id,
                    });
            }
            TableConstraint::Check { .. } => {
                return Err(DataBaseErrors::QueryError(
                    "Table-level CHECK constraints are not supported; use a column CHECK".into(),
                ));
            }
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "Unsupported table constraint in CREATE TABLE".into(),
                ));
            }
        }
    }
    Ok(column_constraints)
}

fn show_tables_matches(name: &str, filter: &ShowStatementFilter) -> Result<bool, DataBaseErrors> {
    match filter {
        ShowStatementFilter::Like(pattern) => Ok(sql_like_matches(name, pattern, false)),
        ShowStatementFilter::ILike(pattern) => Ok(sql_like_matches(name, pattern, true)),
        ShowStatementFilter::Where(_) => Err(DataBaseErrors::QueryError(
            "SHOW TABLES WHERE filtering is unsupported".into(),
        )),
    }
}

struct CopySession {
    table_name: String,
    columns: Vec<CopyColumnSpec>,
    options: CopyOptions,
    buffer: Vec<u8>,
    transaction: Transaction,
    auto_commit: bool,
}

pub struct PgWireHandler {
    pub db: Arc<RwLock<Database>>,
    pub active_transactions: Arc<RwLock<HashMap<SocketAddr, Transaction>>>,
    copy_sessions: Arc<RwLock<HashMap<SocketAddr, CopySession>>>,
    /// Serializes BEGIN/COMMIT/ROLLBACK bookkeeping to avoid lock-order deadlocks.
    txn_lifecycle: Arc<Mutex<()>>,
}

impl NoopStartupHandler for PgWireHandler {}

impl PgWireHandler {
    fn clone_state(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            active_transactions: Arc::clone(&self.active_transactions),
            copy_sessions: Arc::clone(&self.copy_sessions),
            txn_lifecycle: Arc::clone(&self.txn_lifecycle),
        }
    }

    fn connection_transaction_at(&self, addr: SocketAddr) -> Option<Transaction> {
        self.active_transactions
            .read()
            .unwrap()
            .get(&addr)
            .cloned()
    }

    fn connection_transaction<C>(&self, client: &C) -> Option<Transaction>
    where
        C: ClientInfo,
    {
        self.connection_transaction_at(client.socket_addr())
    }

    fn create_connection_transaction_at(&self, addr: SocketAddr) -> Result<Transaction, DataBaseErrors> {
        if self.connection_transaction_at(addr).is_some() {
            return Err(DataBaseErrors::QueryError(
                "Transaction already in progress".into(),
            ));
        }

        let _guard = self.txn_lifecycle.lock().unwrap();
        let transaction = {
            let mut db = self.db.write().unwrap();
            db.create_transaction()
        };
        self.active_transactions
            .write()
            .unwrap()
            .insert(addr, transaction.clone());
        Ok(transaction)
    }

    fn create_connection_transaction<C>(&self, client: &C) -> Result<Transaction, DataBaseErrors>
    where
        C: ClientInfo,
    {
        self.create_connection_transaction_at(client.socket_addr())
    }

    fn commit_connection_transaction_at(&self, addr: SocketAddr) -> Result<(), DataBaseErrors> {
        let _guard = self.txn_lifecycle.lock().unwrap();
        let transaction = self
            .active_transactions
            .write()
            .unwrap()
            .remove(&addr)
            .ok_or_else(|| DataBaseErrors::QueryError("No active transaction to commit".into()))?;
        let mut db = self.db.write().unwrap();
        db.commit_transaction(transaction.transaction_id);
        Ok(())
    }

    fn commit_connection_transaction<C>(&self, client: &C) -> Result<(), DataBaseErrors>
    where
        C: ClientInfo,
    {
        self.commit_connection_transaction_at(client.socket_addr())
    }

    fn rollback_connection_transaction_at(&self, addr: SocketAddr) -> Result<(), DataBaseErrors> {
        let _guard = self.txn_lifecycle.lock().unwrap();
        let transaction = self
            .active_transactions
            .write()
            .unwrap()
            .remove(&addr)
            .ok_or_else(|| DataBaseErrors::QueryError("No active transaction to rollback".into()))?;
        let mut db = self.db.write().unwrap();
        db.rollback_transaction(&transaction);
        Ok(())
    }

    fn rollback_connection_transaction<C>(&self, client: &C) -> Result<(), DataBaseErrors>
    where
        C: ClientInfo,
    {
        self.rollback_connection_transaction_at(client.socket_addr())
    }

    fn transaction_for_query_at(
        &self,
        addr: SocketAddr,
    ) -> Result<(Transaction, bool), DataBaseErrors> {
        if let Some(transaction) = self.connection_transaction_at(addr) {
            return Ok((transaction, false));
        }

        let mut db = self.db.write().unwrap();
        let transaction = db.create_transaction();
        Ok((transaction, true))
    }

    fn transaction_for_query<C>(&self, client: &C) -> Result<(Transaction, bool), DataBaseErrors>
    where
        C: ClientInfo,
    {
        self.transaction_for_query_at(client.socket_addr())
    }

    fn execute_transactional_at<F, R>(
        &self,
        addr: SocketAddr,
        action: F,
    ) -> Result<R, DataBaseErrors>
    where
        F: FnOnce(&Transaction) -> Result<R, DataBaseErrors>,
    {
        let (transaction, should_commit) = self.transaction_for_query_at(addr)?;
        let result = action(&transaction);

        if should_commit {
            let mut db = self.db.write().unwrap();
            match &result {
                Ok(_) => db.commit_transaction(transaction.transaction_id),
                Err(_) => db.rollback_transaction(&transaction),
            }
        } else if result.is_err() {
            let _guard = self.txn_lifecycle.lock().unwrap();
            self.active_transactions.write().unwrap().remove(&addr);
            let mut db = self.db.write().unwrap();
            db.rollback_transaction(&transaction);
        }

        result
    }

    fn execute_transactional<C, F, R>(&self, client: &C, action: F) -> Result<R, DataBaseErrors>
    where
        C: ClientInfo,
        F: FnOnce(&Transaction) -> Result<R, DataBaseErrors>,
    {
        self.execute_transactional_at(client.socket_addr(), action)
    }

    async fn run_on_blocking_pool<F, R>(&self, task: F) -> Result<R, DataBaseErrors>
    where
        F: FnOnce(Self) -> Result<R, DataBaseErrors> + Send + 'static,
        R: Send + 'static,
    {
        let handler = self.clone_state();
        tokio::task::spawn_blocking(move || task(handler))
            .await
            .map_err(|err| DataBaseErrors::QueryError(format!("database worker failed: {err}")))?
    }

    fn run_query_pipeline_at(
        &self,
        addr: SocketAddr,
        statement: Statement,
    ) -> Result<ExecutionResult, DataBaseErrors> {
        self.execute_transactional_at(addr, |transaction| {
            let db_read = self.db.read().unwrap();
            plan::plan_and_execute(&db_read, statement, transaction, |table_id, column_id, value| {
                let db = self.db.read().unwrap();
                db.foreign_key_value_exists(table_id, column_id, value, transaction)
            })
        })
    }

    async fn run_query_pipeline_async<C>(
        &self,
        client: &C,
        statement: Statement,
    ) -> Result<ExecutionResult, DataBaseErrors>
    where
        C: ClientInfo + Send + Sync,
    {
        let addr = client.socket_addr();
        self.run_on_blocking_pool(move |handler| handler.run_query_pipeline_at(addr, statement))
            .await
    }

    fn begin_copy_transaction<C>(&self, client: &C) -> Result<(Transaction, bool), DataBaseErrors>
    where
        C: ClientInfo,
    {
        if let Some(transaction) = self.connection_transaction(client) {
            return Ok((transaction, false));
        }

        let mut db = self.db.write().unwrap();
        let transaction = db.create_transaction();
        Ok((transaction, true))
    }

    fn finish_copy_transaction(
        &self,
        transaction: &Transaction,
        auto_commit: bool,
        success: bool,
    ) {
        if !auto_commit {
            return;
        }

        let mut db = self.db.write().unwrap();
        if success {
            db.commit_transaction(transaction.transaction_id);
        } else {
            db.rollback_transaction(transaction);
        }
    }

    async fn send_query_error<C>(
        &self,
        client: &mut C,
        message: String,
    ) -> PgWireResult<Vec<Response<'static>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), message),
            )))
            .await?;
        Ok(vec![Response::Execution(Tag::new("ERROR"))])
    }

    async fn respond_planned_error<C>(
        &self,
        client: &mut C,
        err: DataBaseErrors,
    ) -> PgWireResult<Vec<Response<'static>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (code, message) = match &err {
            DataBaseErrors::TableNotFound(name) => ("42P01", format!("Table '{name}' not found")),
            _ => ("42601", err.to_string()),
        };
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new("ERROR".to_owned(), code.to_owned(), message),
            )))
            .await?;
        Ok(vec![Response::Execution(Tag::new("ERROR"))])
    }

    fn responses_contain_error(responses: &[Response]) -> bool {
        responses.iter().any(|response| {
            matches!(response, Response::Execution(tag) if *tag == Tag::new("ERROR"))
        })
    }

    async fn execute_simple_statement<'a, C>(
        &self,
        client: &mut C,
        statement: Statement,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match statement {
            Statement::ShowTables { filter, .. } => {
                let field = FieldInfo::new(
                    "table_name".into(),
                    None,
                    None,
                    Type::VARCHAR,
                    FieldFormat::Text,
                );
                let schema = Arc::new(vec![field]);
                let table_names: Vec<_> = self
                    .db
                    .read()
                    .unwrap()
                    .tables
                    .keys()
                    .cloned()
                    .collect();

                let table_names: Result<Vec<String>, DataBaseErrors> = table_names
                    .into_iter()
                    .filter_map(|name| match &filter {
                        Some(filter) => match show_tables_matches(&name, filter) {
                            Ok(true) => Some(Ok(name)),
                            Ok(false) => None,
                            Err(err) => Some(Err(err)),
                        },
                        None => Some(Ok(name)),
                    })
                    .collect();

                let table_names = match table_names {
                    Ok(names) => names,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let schema_ref = schema.clone();
                let data_row_stream = stream::iter(table_names.into_iter().map(move |name: String| {
                    let mut encoder = DataRowEncoder::new(schema_ref.clone());
                    encoder.encode_field(&Some(name.as_str()))?;
                    encoder.finish()
                }));
                return Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))]);
            }
            Statement::CreateTable(create_table) => {
                let table_name = match parse_table_name(&create_table.name) {
                    Ok(name) => name,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let (transaction, should_commit) = match self.transaction_for_query(client) {
                    Ok(pair) => pair,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let result = (|| {
                    if create_table.query.is_some() {
                        return Err(DataBaseErrors::QueryError(
                            "CREATE TABLE AS SELECT is unsupported".into(),
                        ));
                    }

                    let mut db = self.db.write().unwrap();
                    if db.get_table(table_name.clone()).is_some() {
                        if create_table.if_not_exists {
                            return Ok(());
                        }
                        return Err(DataBaseErrors::TableAlreadyExists(table_name.clone()));
                    }

                    db.create_table(table_name.clone())?;
                    let table_ref = db.get_table(table_name.clone()).unwrap();
                    let mut table_write = table_ref.write().unwrap();
                    let mut column_name_map = HashMap::new();

                    for column_def in &create_table.columns {
                        let column_name = normalize_identifier(&column_def.name.value);
                        let data_type = parse_create_table_data_type(&column_def.data_type)?;
                        let constraints =
                            parse_column_constraints(&column_def.options, &db, &transaction)?;
                        let column_id = table_write.create_column(
                            column_name.clone(),
                            data_type,
                            constraints,
                            &transaction,
                        )?;
                        column_name_map.insert(column_name, column_id);
                    }

                    let table_constraint_map = build_table_constraint_map(
                        &create_table.constraints,
                        &column_name_map,
                        &db,
                        &transaction,
                    )?;
                    for (column_id, extra_constraints) in table_constraint_map {
                        table_write.add_column_constraints(column_id, extra_constraints, &transaction)?;
                    }

                    Ok(())
                })();

                if should_commit {
                    if result.is_ok() {
                        let mut db = self.db.write().unwrap();
                        db.commit_transaction(transaction.transaction_id);
                    } else {
                        let mut db = self.db.write().unwrap();
                        db.rollback_transaction(&transaction);
                    }
                }

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))]),
                    Err(DataBaseErrors::TableAlreadyExists(name)) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42710".to_owned(),
                                    format!("Table '{}' already exists", name),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    format!("{}", err),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::CreateIndex(create_index) => {
                if create_index.unique {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                "CREATE UNIQUE INDEX is unsupported; use UNIQUE or PRIMARY KEY constraints".to_owned(),
                            ),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }
                if create_index.concurrently {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                "CREATE INDEX CONCURRENTLY is unsupported".to_owned(),
                            ),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }
                if create_index.columns.len() != 1 {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                "CREATE INDEX supports only a single column".to_owned(),
                            ),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }

                let table_name = match parse_table_name(&create_index.table_name) {
                    Ok(name) => name,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };
                let column_name = match parse_index_column(&create_index.columns[0]) {
                    Ok(name) => name,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };
                let index_name = match create_index.name.as_ref() {
                    Some(name) => match parse_index_name(name) {
                        Ok(name) => Some(name),
                        Err(err) => {
                            client
                                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                                )))
                                .await?;
                            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                        }
                    },
                    None => None,
                };

                let result = self.execute_transactional(client, |transaction| {
                    let table_ref = {
                        let db_read = self.db.read().unwrap();
                        db_read.get_table(table_name.clone())
                    };
                    let table = match table_ref {
                        Some(table) => table,
                        None => return Err(DataBaseErrors::TableNotFound(table_name.clone())),
                    };
                    let mut table_write = table.write().unwrap();
                    if create_index.if_not_exists
                        && table_write.is_column_indexed(&column_name, transaction)?
                    {
                        return Ok(());
                    }
                    table_write.create_column_index(index_name, &column_name, transaction)?;
                    Ok(())
                });

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))]),
                    Err(DataBaseErrors::TableNotFound(name)) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42P01".to_owned(),
                                    format!("Table '{}' not found", name),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    format!("{}", err),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                if object_type == ObjectType::Index {
                    if names.len() != 1 {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    "DROP INDEX supports only one index name".to_owned(),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }

                    let index_name = match parse_index_name(&names[0]) {
                        Ok(name) => name,
                        Err(err) => {
                            client
                                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                    ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                                )))
                                .await?;
                            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                        }
                    };

                    let result = self.execute_transactional(client, |transaction| {
                        let tables: Vec<_> = {
                            let db_read = self.db.read().unwrap();
                            db_read.tables.values().cloned().collect()
                        };
                        let mut dropped = false;
                        let mut last_err: Option<DataBaseErrors> = None;
                        for table in tables {
                            let mut table_write = table.write().unwrap();
                            match table_write.drop_column_index(&index_name, transaction) {
                                Ok(()) => {
                                    dropped = true;
                                    break;
                                }
                                Err(DataBaseErrors::IndexNotFound(_)) => {}
                                Err(err) => last_err = Some(err),
                            }
                        }
                        if dropped {
                            return Ok(());
                        }
                        if if_exists {
                            return Ok(());
                        }
                        if let Some(err) = last_err {
                            return Err(err);
                        }
                        Err(DataBaseErrors::IndexNotFound(index_name.clone()))
                    });

                    match result {
                        Ok(_) => return Ok(vec![Response::Execution(Tag::new("DROP INDEX"))]),
                        Err(DataBaseErrors::IndexNotFound(name)) => {
                            client
                                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                    ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "42704".to_owned(),
                                        format!("Index '{}' not found", name),
                                    ),
                                )))
                                .await?;
                            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                        }
                        Err(err) => {
                            client
                                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                    ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "42601".to_owned(),
                                        format!("{}", err),
                                    ),
                                )))
                                .await?;
                            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                        }
                    }
                }

                if object_type != ObjectType::Table {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                "Unsupported DROP target".to_owned(),
                            ),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }

                if names.len() != 1 {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                "DROP TABLE supports only one table name".to_owned(),
                            ),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }

                let table_name = match parse_table_name(&names[0]) {
                    Ok(name) => name,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let result = (|| {
                    let mut db = self.db.write().unwrap();
                    if db.get_table(table_name.clone()).is_none() {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(DataBaseErrors::TableNotFound(table_name.clone()));
                    }
                    db.drop_table(table_name.clone());
                    Ok(())
                })();

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("DROP TABLE"))]),
                    Err(DataBaseErrors::TableNotFound(name)) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42P01".to_owned(),
                                    format!("Table '{}' not found", name),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    format!("{}", err),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::StartTransaction { .. } => {
                let addr = client.socket_addr();
                match self
                    .run_on_blocking_pool(move |handler| handler.create_connection_transaction_at(addr))
                    .await
                {
                    Ok(_) => return Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]),
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "25001".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::Commit { .. } => {
                let addr = client.socket_addr();
                match self
                    .run_on_blocking_pool(move |handler| handler.commit_connection_transaction_at(addr))
                    .await
                {
                    Ok(_) => return Ok(vec![Response::TransactionEnd(Tag::new("COMMIT"))]),
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "25001".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::Rollback { savepoint, .. } => {
                if savepoint.is_some() {
                    client
                        .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                            ErrorInfo::new("ERROR".to_owned(), "0A000".to_owned(), "SAVEPOINT rollback is unsupported".to_owned()),
                        )))
                        .await?;
                    return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                }

                let addr = client.socket_addr();
                match self
                    .run_on_blocking_pool(move |handler| handler.rollback_connection_transaction_at(addr))
                    .await
                {
                    Ok(_) => return Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]),
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "25001".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::AlterTable {
                name,
                if_exists,
                operations,
                ..
            } => {
                let table_name = match parse_table_name(&name) {
                    Ok(name) => name,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), format!("{}", err)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let result = self.execute_transactional(client, |transaction| {
                    let table_ref = {
                        let db_read = self.db.read().unwrap();
                        db_read.get_table(table_name.clone())
                    };

                    let table = match table_ref {
                        Some(table) => table,
                        None => return Err(DataBaseErrors::TableNotFound(table_name.clone())),
                    };

                    let mut table_write = table.write().unwrap();
                    for operation in operations {
                        match operation {
                            AlterTableOperation::DropColumn {
                                column_name,
                                if_exists: op_if_exists,
                                cascade,
                            } => {
                                if cascade {
                                    return Err(DataBaseErrors::QueryError(
                                        "ALTER TABLE DROP COLUMN CASCADE is unsupported".into(),
                                    ));
                                }

                                let normalized = normalize_identifier(&column_name.value);
                                let visible_columns = table_write.get_visible_column_map(transaction)?;
                                if let Some(column_id) = visible_columns.get(&normalized) {
                                    table_write.drop_column(*column_id, transaction)?;
                                } else if !op_if_exists && !if_exists {
                                    return Err(DataBaseErrors::QueryError(format!(
                                        "Column '{}' not found",
                                        normalized,
                                    )));
                                }
                            }
                            _ => {
                                return Err(DataBaseErrors::QueryError(
                                    "Unsupported ALTER TABLE operation".into(),
                                ));
                            }
                        }
                    }

                    Ok(())
                });

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("ALTER TABLE"))]),
                    Err(DataBaseErrors::TableNotFound(name)) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42P01".to_owned(),
                                    format!("Table '{}' not found", name),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    format!("{}", err),
                                ),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                }
            }
            Statement::Delete(delete) => {
                match self.run_query_pipeline_async(client, Statement::Delete(delete)).await {
                    Ok(ExecutionResult::RowsAffected { tag, .. }) => {
                        return Ok(vec![Response::Execution(Tag::new(tag))]);
                    }
                    Ok(_) => {
                        return self
                            .send_query_error(client, "Unexpected DELETE pipeline result".to_string())
                            .await;
                    }
                    Err(err) => return self.respond_planned_error(client, err).await,
                }
            }
            Statement::Insert(insert) => {
                match self.run_query_pipeline_async(client, Statement::Insert(insert)).await {
                    Ok(ExecutionResult::RowsAffected { tag, .. }) => {
                        return Ok(vec![Response::Execution(Tag::new(tag))]);
                    }
                    Ok(_) => {
                        return self
                            .send_query_error(client, "Unexpected INSERT pipeline result".to_string())
                            .await;
                    }
                    Err(err) => return self.respond_planned_error(client, err).await,
                }
            }
            Statement::Update {
                table,
                assignments,
                selection,
                from,
                returning,
            } => {
                match self
                    .run_query_pipeline_async(
                        client,
                        Statement::Update {
                            table,
                            assignments,
                            selection,
                            from,
                            returning,
                        },
                    )
                    .await
                {
                    Ok(ExecutionResult::RowsAffected { tag, .. }) => {
                        return Ok(vec![Response::Execution(Tag::new(tag))]);
                    }
                    Ok(_) => {
                        return self
                            .send_query_error(client, "Unexpected UPDATE pipeline result".to_string())
                            .await;
                    }
                    Err(err) => return self.respond_planned_error(client, err).await,
                }
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                ..
            } => {
                if !legacy_options.is_empty() {
                    return self
                        .send_query_error(
                            client,
                            "Legacy COPY options are not supported; use WITH (...)".to_string(),
                        )
                        .await;
                }

                let copy_options = match parse_copy_options(&options) {
                    Ok(options) => options,
                    Err(err) => return self.send_query_error(client, err.to_string()).await,
                };

                if copy_options.binary {
                    return self
                        .send_query_error(client, "COPY binary format is not supported".to_string())
                        .await;
                }

                match (to, &target) {
                    (false, CopyTarget::Stdin) => {
                        let (table_name, requested_columns) = match &source {
                            CopySource::Table { table_name, columns } => {
                                match parse_table_name(table_name) {
                                    Ok(name) => (name, columns.as_slice()),
                                    Err(err) => {
                                        return self.send_query_error(client, err.to_string()).await;
                                    }
                                }
                            }
                            CopySource::Query(_) => {
                                return self
                                    .send_query_error(
                                        client,
                                        "COPY (query) FROM STDIN is not supported".to_string(),
                                    )
                                    .await;
                            }
                        };

                        let (transaction, auto_commit) = match self.begin_copy_transaction(client) {
                            Ok(value) => value,
                            Err(err) => return self.send_query_error(client, err.to_string()).await,
                        };

                        let columns_result: Result<Vec<CopyColumnSpec>, String> = {
                            let db_read = self.db.read().unwrap();
                            match db_read.get_table(table_name.clone()) {
                                None => Err(format!("Table '{table_name}' not found")),
                                Some(table_ref) => match table_ref.read() {
                                    Ok(table) => resolve_copy_columns(
                                        &table,
                                        &transaction,
                                        requested_columns,
                                        normalize_identifier,
                                    )
                                    .map_err(|err| err.to_string()),
                                    Err(_) => {
                                        Err("Failed to acquire table read lock".to_string())
                                    }
                                },
                            }
                        };
                        let columns = match columns_result {
                            Ok(columns) => columns,
                            Err(message) => return self.send_query_error(client, message).await,
                        };
                        let column_count = columns.len();

                        self.copy_sessions.write().unwrap().insert(
                            client.socket_addr(),
                            CopySession {
                                table_name,
                                columns,
                                options: copy_options,
                                buffer: Vec::new(),
                                transaction,
                                auto_commit,
                            },
                        );

                        return Ok(vec![Response::CopyIn(CopyResponse::new(
                            0,
                            column_count,
                            vec![0; column_count],
                        ))]);
                    }
                    (true, CopyTarget::Stdout) => {
                        let (table_name, requested_columns) = match &source {
                            CopySource::Table { table_name, columns } => {
                                match parse_table_name(table_name) {
                                    Ok(name) => (name, columns.as_slice()),
                                    Err(err) => {
                                        return self.send_query_error(client, err.to_string()).await;
                                    }
                                }
                            }
                            CopySource::Query(_) => {
                                return self
                                    .send_query_error(
                                        client,
                                        "COPY (query) TO STDOUT is not supported".to_string(),
                                    )
                                    .await;
                            }
                        };

                        let result = self.execute_transactional(client, |transaction| {
                            let db_read = self.db.read().unwrap();
                            let table_ref = db_read
                                .get_table(table_name.clone())
                                .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.clone()))?;
                            let table = table_ref.read().map_err(|_| {
                                DataBaseErrors::QueryError(
                                    "Failed to acquire table read lock".into(),
                                )
                            })?;

                            let columns =
                                resolve_copy_columns(&table, transaction, requested_columns, normalize_identifier)?;
                            let rows = table.collect_visible_rows(transaction)?;
                            let payload = encode_copy_payload(&rows, &columns, &copy_options)?;
                            Ok((rows.len(), payload, columns.len()))
                        });

                        match result {
                            Ok((row_count, payload, column_count)) => {
                                send_copy_out_response(
                                    client,
                                    CopyResponse::new(0, column_count, vec![0; column_count]),
                                )
                                .await?;
                                if !payload.is_empty() {
                                    client
                                        .send(PgWireBackendMessage::CopyData(CopyData::new(
                                            Bytes::from(payload),
                                        )))
                                        .await?;
                                }
                                client
                                    .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                                    .await?;
                                return Ok(vec![Response::Execution(
                                    Tag::new("COPY").with_rows(row_count),
                                )]);
                            }
                            Err(DataBaseErrors::TableNotFound(name)) => {
                                return self
                                    .send_query_error(
                                        client,
                                        format!("Table '{name}' not found"),
                                    )
                                    .await;
                            }
                            Err(err) => {
                                return self.send_query_error(client, err.to_string()).await;
                            }
                        }
                    }
                    (false, CopyTarget::File { .. } | CopyTarget::Program { .. }) => {
                        return self
                            .send_query_error(
                                client,
                                "COPY FROM file/program is not supported; use STDIN".to_string(),
                            )
                            .await;
                    }
                    (true, CopyTarget::File { .. } | CopyTarget::Program { .. }) => {
                        return self
                            .send_query_error(
                                client,
                                "COPY TO file/program is not supported; use STDOUT".to_string(),
                            )
                            .await;
                    }
                    _ => {
                        return self
                            .send_query_error(client, "Unsupported COPY target".to_string())
                            .await;
                    }
                }
            }
            Statement::Query(query) => {
                let (column_names, search_results) = match self
                    .run_query_pipeline_async(client, Statement::Query(query))
                    .await
                {
                    Ok(ExecutionResult::Select { column_names, rows }) => (column_names, rows),
                    Ok(_) => {
                        return self
                            .send_query_error(client, "Unexpected SELECT pipeline result".to_string())
                            .await;
                    }
                    Err(err) => return self.respond_planned_error(client, err).await,
                };

                let schema: Arc<Vec<FieldInfo>> = Arc::new(
                    column_names
                        .iter()
                        .map(|column_name| {
                            FieldInfo::new(
                                column_name.clone(),
                                None,
                                None,
                                Type::VARCHAR,
                                FieldFormat::Text,
                            )
                        })
                        .collect(),
                );

                let schema_ref = schema.clone();
                let data_row_stream = stream::iter(search_results.into_iter().map(move |row| {
                    let mut encoder = DataRowEncoder::new(schema_ref.clone());
                    for column_name in &column_names {
                        let value = row
                            .values
                            .get(column_name)
                            .map(|entry| entry.to_wire_text());
                        encoder.encode_field(&value.as_deref())?;
                    }
                    encoder.finish()
                }));

                return Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))]);
            }
            _ => {
                client
                    .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42601".to_owned(),
                            "Unsupported SQL statement".to_owned(),
                        ),
                    )))
                    .await?;
                return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
            }
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for PgWireHandler {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = normalize_simple_query(query);

        let statements = match plan::parse_sql(&query) {
            Ok(statements) => statements,
            Err(err) => {
                client
                    .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                        ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42601".to_owned(),
                            err.to_string(),
                        ),
                    )))
                    .await?;
                return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
            }
        };

        if statements.is_empty() {
            client
                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        "Empty SQL statement".to_owned(),
                    ),
                )))
                .await?;
            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
        }

        let mut all_responses: Vec<Response<'a>> = Vec::new();
        for statement in statements {
            let batch = self.execute_simple_statement(client, statement).await?;
            let failed = Self::responses_contain_error(&batch);
            all_responses.extend(batch);
            if failed {
                break;
            }
        }

        Ok(all_responses)
    }
}

#[async_trait]
impl CopyHandler for PgWireHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut sessions = self.copy_sessions.write().unwrap();
        let session = sessions.get_mut(&client.socket_addr()).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "57014".to_owned(),
                "No active COPY session".to_owned(),
            )))
        })?;
        append_copy_data(&mut session.buffer, &copy_data.data);
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self
            .copy_sessions
            .write()
            .unwrap()
            .remove(&client.socket_addr())
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "57014".to_owned(),
                    "No active COPY session".to_owned(),
                )))
            })?;

        let result = (|| -> Result<usize, DataBaseErrors> {
            let rows = parse_copy_rows(&session.buffer, &session.columns, &session.options)?;
            let db_read = self.db.read().unwrap();
            let table_ref = db_read
                .get_table(session.table_name.clone())
                .ok_or_else(|| DataBaseErrors::TableNotFound(session.table_name.clone()))?;
            let table = table_ref
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;

            for row_data in &rows {
                table.insert_row(row_data.clone(), &session.transaction, |table_id, column_id, value| {
                    let db = self.db.read().unwrap();
                    db.foreign_key_value_exists(table_id, column_id, value, &session.transaction)
                })?;
            }

            Ok(rows.len())
        })();

        match result {
            Ok(row_count) => {
                self.finish_copy_transaction(&session.transaction, session.auto_commit, true);
                client
                    .send(PgWireBackendMessage::CommandComplete(CommandComplete::from(
                        Tag::new("COPY").with_rows(row_count),
                    )))
                    .await?;
                Ok(())
            }
            Err(err) => {
                self.finish_copy_transaction(&session.transaction, session.auto_commit, false);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    err.to_string(),
                ))))
            }
        }
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(session) = self.copy_sessions.write().unwrap().remove(&client.socket_addr()) {
            self.finish_copy_transaction(&session.transaction, session.auto_commit, false);
        }

        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "57014".to_owned(),
            format!("COPY FROM STDIN terminated: {}", fail.message),
        )))
    }
}

struct PgWireHandlerFactoryImpl {
    handler: Arc<PgWireHandler>,
}

impl PgWireHandlerFactory for PgWireHandlerFactoryImpl {
    type StartupHandler = PgWireHandler;
    type SimpleQueryHandler = PgWireHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = PgWireHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.handler.clone()
    }
}

pub async fn run_pgwire_server(db: Arc<RwLock<Database>>, addr: &str) {
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("pgwire listening on {}", addr);

    let factory = Arc::new(PgWireHandlerFactoryImpl {
        handler: Arc::new(PgWireHandler {
            db,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            copy_sessions: Arc::new(RwLock::new(HashMap::new())),
            txn_lifecycle: Arc::new(Mutex::new(())),
        }),
    });

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            if let Err(err) = process_socket(socket, None, factory_ref).await {
                tracing::error!("pgwire socket failed: {:?}", err);
            }
        });
    }
}
