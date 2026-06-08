use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use ahash::AHashMap;
use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use ordered_float::NotNan;
use sqlparser::ast::{
    AlterTableOperation,
    ColumnOption,
    ColumnOptionDef,
    DataType,
    Expr as SqlExpr,
    FromTable,
    Ident,
    ObjectType,
    SetExpr,
    ShowStatementFilter,
    Statement,
    TableConstraint,
    TableFactor,
    UnaryOperator,
    Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::NoticeResponse;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use crate::backend::core::column::ColumnID;
use crate::backend::core::database::Database;
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::search::SearchRequest;
use crate::backend::core::table::Table;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

fn normalize_identifier(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

fn parse_sql_value(value: &SqlValue) -> Result<DataBaseDataEntry, DataBaseErrors> {
    match value {
        SqlValue::Number(text, _) => {
            if text.contains('.') || text.contains('e') || text.contains('E') {
                let float = text.parse::<f64>().map_err(|err| {
                    DataBaseErrors::QueryError(format!(
                        "Unable to parse numeric literal '{text}': {err}",
                    ))
                })?;
                Ok(DataBaseDataEntry::FloatF64(NotNan::new(float).map_err(|_| {
                    DataBaseErrors::QueryError("Floating point literal must not be NaN".into())
                })?))
            } else if let Ok(unsigned) = text.parse::<u64>() {
                Ok(DataBaseDataEntry::IntegerU64(unsigned))
            } else if let Ok(signed) = text.parse::<i64>() {
                Ok(DataBaseDataEntry::IntegerI64(signed))
            } else if let Ok(unsigned128) = text.parse::<u128>() {
                Ok(DataBaseDataEntry::IntegerU128(unsigned128))
            } else if let Ok(signed128) = text.parse::<i128>() {
                Ok(DataBaseDataEntry::IntegerI128(signed128))
            } else {
                Err(DataBaseErrors::QueryError(format!(
                    "Numeric literal '{text}' is too large or invalid",
                )))
            }
        }
        SqlValue::SingleQuotedString(value) => Ok(DataBaseDataEntry::String(value.clone())),
        SqlValue::Boolean(flag) => Ok(DataBaseDataEntry::Boolean(*flag)),
        SqlValue::Null => Ok(DataBaseDataEntry::Null),
        _ => Err(DataBaseErrors::QueryError(format!(
            "Unsupported literal value in INSERT/UPDATE statement: {value:?}",
        ))),
    }
}

fn parse_sql_literal(expr: &SqlExpr) -> Result<DataBaseDataEntry, DataBaseErrors> {
    match expr {
        SqlExpr::Value(value) => parse_sql_value(value),
        SqlExpr::UnaryOp { op: UnaryOperator::Minus, expr } => {
            let literal = parse_sql_literal(expr)?;
            match literal {
                DataBaseDataEntry::IntegerU64(value) => Ok(DataBaseDataEntry::IntegerI64(-(value as i64))),
                DataBaseDataEntry::IntegerI64(value) => Ok(DataBaseDataEntry::IntegerI64(-value)),
                DataBaseDataEntry::IntegerU128(value) => Ok(DataBaseDataEntry::IntegerI128(-(value as i128))),
                DataBaseDataEntry::IntegerI128(value) => Ok(DataBaseDataEntry::IntegerI128(-value)),
                DataBaseDataEntry::FloatF64(value) => Ok(DataBaseDataEntry::FloatF64(NotNan::new(-value.into_inner()).map_err(|_| {
                    DataBaseErrors::QueryError("Floating point literal must not be NaN".into())
                })?)),
                _ => Err(DataBaseErrors::QueryError(
                    "Only numeric literals support unary minus in INSERT/UPDATE.".into(),
                )),
            }
        }
        SqlExpr::Nested(expr) => parse_sql_literal(expr),
        _ => Err(DataBaseErrors::QueryError(
            "Only literal expressions are supported in INSERT/UPDATE values".into(),
        )),
    }
}

fn build_insert_data(
    table: &Table,
    transaction: &Transaction,
    requested_columns: &[Ident],
    values: &[SqlExpr],
) -> Result<AHashMap<ColumnID, DataBaseDataEntry>, DataBaseErrors> {
    let visible_column_map = table.get_visible_column_map(transaction)?;

    let insert_column_names: Vec<String> = if !requested_columns.is_empty() {
        requested_columns
            .iter()
            .map(|identifier| normalize_identifier(&identifier.value))
            .collect()
    } else {
        let mut columns: Vec<(String, ColumnID)> = visible_column_map
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
        columns.sort_by_key(|(_, id)| *id);
        columns.into_iter().map(|(name, _)| name).collect()
    };

    if insert_column_names.len() != values.len() {
        return Err(DataBaseErrors::QueryError(format!(
            "INSERT column count {} does not match value count {}",
            insert_column_names.len(),
            values.len()
        )));
    }

    let mut row_data = AHashMap::new();
    for (column_name, expr) in insert_column_names.iter().zip(values.iter()) {
        let column_id = visible_column_map
            .get(column_name)
            .ok_or_else(|| DataBaseErrors::QueryError(format!("Column '{}' not found", column_name)))?;
        let value = parse_sql_literal(expr)?;
        row_data.insert(*column_id, value);
    }

    Ok(row_data)
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
        _ => {
            return Err(DataBaseErrors::QueryError(format!(
                "Unsupported column type in CREATE TABLE: {data_type:?}",
            )))
        }
    })
}

fn parse_column_constraints(
    options: &[ColumnOptionDef],
) -> Result<BTreeSet<crate::backend::core::column::Constraint>, DataBaseErrors> {
    let mut constraints = BTreeSet::new();
    for option in options {
        match &option.option {
            ColumnOption::NotNull => {
                constraints.insert(crate::backend::core::column::Constraint::NotNull);
            }
            ColumnOption::Null => {}
            ColumnOption::Unique { is_primary, .. } => {
                if *is_primary {
                    constraints.insert(crate::backend::core::column::Constraint::PrimaryKey);
                } else {
                    constraints.insert(crate::backend::core::column::Constraint::Unique);
                }
            }
            ColumnOption::DialectSpecific(tokens) => {
                let text = tokens
                    .iter()
                    .map(|token| token.to_string().to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                if text == "auto_increment" || text == "autoincrement" {
                    constraints.insert(crate::backend::core::column::Constraint::AutoIncrement);
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
            ColumnOption::ForeignKey { .. } => {
                return Err(DataBaseErrors::QueryError(
                    "Foreign key constraints are unsupported".into(),
                ));
            }
            ColumnOption::Check(_) => {
                return Err(DataBaseErrors::QueryError(
                    "CHECK constraints are unsupported".into(),
                ));
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
) -> Result<HashMap<ColumnID, BTreeSet<crate::backend::core::column::Constraint>>, DataBaseErrors> {
    let mut column_constraints: HashMap<ColumnID, BTreeSet<crate::backend::core::column::Constraint>> = HashMap::new();
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
                        .insert(crate::backend::core::column::Constraint::Unique);
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
                        .insert(crate::backend::core::column::Constraint::PrimaryKey);
                }
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

pub struct PgWireHandler {
    pub db: Arc<RwLock<Database>>,
    pub active_transactions: Arc<RwLock<HashMap<SocketAddr, Transaction>>>,
}

impl NoopStartupHandler for PgWireHandler {}

impl PgWireHandler {
    fn connection_transaction<C>(&self, client: &C) -> Option<Transaction>
    where
        C: ClientInfo,
    {
        let active_transactions = self.active_transactions.read().unwrap();
        active_transactions.get(&client.socket_addr()).cloned()
    }

    fn create_connection_transaction<C>(&self, client: &C) -> Result<Transaction, DataBaseErrors>
    where
        C: ClientInfo,
    {
        if self.connection_transaction(client).is_some() {
            return Err(DataBaseErrors::QueryError(
                "Transaction already in progress".into(),
            ));
        }

        let mut db = self.db.write().unwrap();
        let transaction = db.create_transaction();
        self.active_transactions
            .write()
            .unwrap()
            .insert(client.socket_addr(), transaction.clone());
        Ok(transaction)
    }

    fn commit_connection_transaction<C>(&self, client: &C) -> Result<(), DataBaseErrors>
    where
        C: ClientInfo,
    {
        let transaction = self
            .active_transactions
            .write()
            .unwrap()
            .remove(&client.socket_addr())
            .ok_or_else(|| DataBaseErrors::QueryError("No active transaction to commit".into()))?;
        let mut db = self.db.write().unwrap();
        db.commit_transaction(transaction.transaction_id);
        Ok(())
    }

    fn rollback_connection_transaction<C>(&self, client: &C) -> Result<(), DataBaseErrors>
    where
        C: ClientInfo,
    {
        let transaction = self
            .active_transactions
            .write()
            .unwrap()
            .remove(&client.socket_addr())
            .ok_or_else(|| DataBaseErrors::QueryError("No active transaction to rollback".into()))?;
        let mut db = self.db.write().unwrap();
        db.rollback_transaction(&transaction);
        Ok(())
    }

    fn transaction_for_query<C>(&self, client: &C) -> Result<(Transaction, bool), DataBaseErrors>
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

    fn execute_transactional<C, F, R>(&self, client: &C, action: F) -> Result<R, DataBaseErrors>
    where
        C: ClientInfo,
        F: FnOnce(&Transaction) -> Result<R, DataBaseErrors>,
    {
        let (transaction, should_commit) = self.transaction_for_query(client)?;
        let result = action(&transaction);

        if should_commit {
            let mut db = self.db.write().unwrap();
            match &result {
                Ok(_) => db.commit_transaction(transaction.transaction_id),
                Err(_) => db.rollback_transaction(&transaction),
            }
        }

        result
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
        let query = query.trim();
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    format!("Query received: {}", query),
                ),
            )))
            .await?;

        let dialect = PostgreSqlDialect {};
        let statements = match Parser::parse_sql(&dialect, query) {
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

        let statement = match statements.into_iter().next() {
            Some(statement) => statement,
            None => {
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
        };

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
                        let constraints = parse_column_constraints(&column_def.options)?;
                        let column_id = table_write.create_column(
                            column_name.clone(),
                            data_type,
                            constraints,
                            &transaction,
                        )?;
                        column_name_map.insert(column_name, column_id);
                    }

                    let table_constraint_map = build_table_constraint_map(&create_table.constraints, &column_name_map)?;
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
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
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
                match self.create_connection_transaction(client) {
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
                match self.commit_connection_transaction(client) {
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

                match self.rollback_connection_transaction(client) {
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
                let result = self.execute_transactional(client, |transaction| {
                    if !delete.order_by.is_empty() {
                        return Err(DataBaseErrors::QueryError(
                            "DELETE ORDER BY is unsupported".into(),
                        ));
                    }
                    if delete.limit.is_some() {
                        return Err(DataBaseErrors::QueryError(
                            "DELETE LIMIT is unsupported".into(),
                        ));
                    }
                    if delete.using.is_some() {
                        return Err(DataBaseErrors::QueryError(
                            "DELETE USING is unsupported".into(),
                        ));
                    }

                    let table_name = if !delete.tables.is_empty() {
                        match delete.tables[0].0.last() {
                            Some(ident) => normalize_identifier(&ident.value),
                            None => {
                                return Err(DataBaseErrors::QueryError(
                                    "Missing DELETE table name".into(),
                                ));
                            }
                        }
                    } else {
                        match &delete.from {
                            FromTable::WithFromKeyword(tables)
                            | FromTable::WithoutKeyword(tables) => {
                                if tables.len() != 1 {
                                    return Err(DataBaseErrors::QueryError(
                                        "DELETE supports exactly one target table".into(),
                                    ));
                                }
                                match &tables[0].relation {
                                    TableFactor::Table { name, .. } => match name.0.last() {
                                        Some(ident) => normalize_identifier(&ident.value),
                                        None => {
                                            return Err(DataBaseErrors::QueryError(
                                                "Missing DELETE table name".into(),
                                            ));
                                        }
                                    },
                                    _ => {
                                        return Err(DataBaseErrors::QueryError(
                                            "Unsupported DELETE target".into(),
                                        ));
                                    }
                                }
                            }
                        }
                    };

                    let table_ref = {
                        let db_read = self.db.read().unwrap();
                        db_read.get_table(table_name.clone())
                    };

                    let table = match table_ref {
                        Some(table) => table,
                        None => return Err(DataBaseErrors::TableNotFound(table_name.clone())),
                    };

                    let _visible_columns = table.read().unwrap().get_visible_column_map(transaction)?;
                    let filter = if let Some(selection) = delete.selection.as_ref() {
                        Some(SearchRequest::parse_sql_expression(selection)?)
                    } else {
                        None
                    };

                    let search_request = SearchRequest {
                        table_name: table_name.clone(),
                        projection: None,
                        filter,
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                    };

                    let rows = table.read().unwrap().search(&search_request, transaction)?;
                    let mut deleted_count = 0;
                    for row in rows {
                        table.read().unwrap().delete_row(row.row_id, transaction)?;
                        deleted_count += 1;
                    }

                    Ok(deleted_count)
                });

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("DELETE"))]),
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
            Statement::Insert(insert) => {
                let table_name = match insert.table_name.0.last() {
                    Some(ident) => normalize_identifier(&ident.value),
                    None => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    "Missing table name in INSERT statement".to_owned(),
                                ),
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

                    let table_read = table.read().unwrap();
                    let mut inserted_count = 0;
                    let source_query = insert.source.ok_or_else(|| {
                        DataBaseErrors::QueryError("INSERT source must be VALUES or query".into())
                    })?;

                    match source_query.body.as_ref() {
                        SetExpr::Values(values) => {
                            for row in values.rows.iter() {
                                let row_data = build_insert_data(
                                    &table_read,
                                    transaction,
                                    &insert.columns,
                                    row,
                                )?;
                                table_read.insert_row(row_data, transaction)?;
                                inserted_count += 1;
                            }
                        }
                        _ => {
                            return Err(DataBaseErrors::QueryError(
                                "INSERT only supports VALUES sources".into(),
                            ));
                        }
                    }

                    Ok(inserted_count)
                });

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("INSERT"))]),
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
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match &table.relation {
                    TableFactor::Table { name, .. } => match name.0.last() {
                        Some(ident) => normalize_identifier(&ident.value),
                        None => {
                            client
                                .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                    ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "42601".to_owned(),
                                        "Missing table name in UPDATE statement".to_owned(),
                                    ),
                                )))
                                .await?;
                            return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                        }
                    },
                    _ => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "42601".to_owned(),
                                    "Unsupported UPDATE target".to_owned(),
                                ),
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

                    let table_read = table.read().unwrap();
                    let visible_columns = table_read.get_visible_column_map(transaction)?;
                    let filter = if let Some(selection) = selection.as_ref() {
                        Some(SearchRequest::parse_sql_expression(selection)?)
                    } else {
                        None
                    };

                    let search_request = SearchRequest {
                        table_name: table_name.clone(),
                        projection: None,
                        filter,
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                    };

                    let rows = table_read.search(&search_request, transaction)?;

                    let mut updated_count = 0;
                    for row in rows {
                        let mut row_data_by_id = AHashMap::new();
                        for (column_name, value) in row.values {
                            let column_id = visible_columns
                                .get(&column_name)
                                .ok_or_else(|| {
                                    DataBaseErrors::QueryError(format!(
                                        "Column '{}' not found during update", column_name,
                                    ))
                                })?;
                            row_data_by_id.insert(*column_id, value);
                        }

                        for assignment in assignments.iter() {
                            let column_name = match &assignment.target {
                                sqlparser::ast::AssignmentTarget::ColumnName(name) => match name.0.last() {
                                    Some(ident) => normalize_identifier(&ident.value),
                                    None => {
                                        return Err(DataBaseErrors::QueryError(
                                            "Invalid assignment target".into(),
                                        ));
                                    }
                                },
                                _ => {
                                    return Err(DataBaseErrors::QueryError(
                                        "Only single-column UPDATE assignments are supported".into(),
                                    ));
                                }
                            };

                            let column_id = visible_columns.get(&column_name).ok_or_else(|| {
                                DataBaseErrors::QueryError(format!(
                                    "Assignment column '{}' not found", column_name,
                                ))
                            })?;

                            let value = parse_sql_literal(&assignment.value)?;
                            row_data_by_id.insert(*column_id, value);
                        }

                        table_read.update_row(row.row_id, row_data_by_id, transaction)?;
                        updated_count += 1;
                    }

                    Ok(updated_count)
                });

                match result {
                    Ok(_) => return Ok(vec![Response::Execution(Tag::new("UPDATE"))]),
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
            Statement::Query(_) => {
                let search_request = match SearchRequest::from_sql(query) {
                    Ok(request) => request,
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                };

                let outcome = self.execute_transactional(client, |transaction| {
                    let table_ref = {
                        let db_read = self.db.read().unwrap();
                        db_read.get_table(search_request.table_name.clone())
                    };

                    let table = match table_ref {
                        Some(table) => table,
                        None => return Err(DataBaseErrors::TableNotFound(search_request.table_name.clone())),
                    };

                    let search_results = {
                        let table_read = table.read().unwrap();
                        table_read.search(&search_request, transaction)
                    };

                    let search_results = search_results?;

                    let column_names = if let Some(projection) = &search_request.projection {
                        projection.clone()
                    } else {
                        let table_read = table.read().unwrap();
                        table_read.get_visible_column_names(transaction)
                    };

                    Ok((column_names, search_results))
                });

                let (column_names, search_results) = match outcome {
                    Ok((columns, rows)) => (columns, rows),
                    Err(DataBaseErrors::TableNotFound(table_name)) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42P01".to_owned(), format!("Table '{}' not found", table_name)),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
                    Err(err) => {
                        client
                            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), err.to_string()),
                            )))
                            .await?;
                        return Ok(vec![Response::Execution(Tag::new("ERROR"))]);
                    }
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
                            .map(|entry| format!("{:?}", entry));
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

struct PgWireHandlerFactoryImpl {
    handler: Arc<PgWireHandler>,
}

impl PgWireHandlerFactory for PgWireHandlerFactoryImpl {
    type StartupHandler = PgWireHandler;
    type SimpleQueryHandler = PgWireHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

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
        Arc::new(NoopCopyHandler)
    }
}

pub async fn run_pgwire_server(db: Arc<RwLock<Database>>, addr: &str) {
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("pgwire listening on {}", addr);

    let factory = Arc::new(PgWireHandlerFactoryImpl {
        handler: Arc::new(PgWireHandler { db, active_transactions: Arc::new(RwLock::new(HashMap::new())) }),
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
