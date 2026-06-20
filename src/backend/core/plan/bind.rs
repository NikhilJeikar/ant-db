use sqlparser::ast::{
    AssignmentTarget, Delete, FromTable, Insert, Query, SetExpr, Statement, TableFactor,
};

use crate::backend::core::database::Database;
use crate::backend::core::plan::dml::{
    build_insert_data, normalize_identifier, parse_update_assignment,
};
use crate::backend::core::plan::logical::LogicalPlan;
use crate::backend::core::search::SearchRequest;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

pub fn bind_statement(
    db: &Database,
    statement: Statement,
    transaction: &Transaction,
) -> Result<LogicalPlan, DataBaseErrors> {
    match statement {
        Statement::Query(query) => Ok(LogicalPlan::Select(bind_select(*query)?)),
        Statement::Delete(delete) => bind_delete(delete),
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => bind_update(table, assignments, selection),
        Statement::Insert(insert) => bind_insert(db, insert, transaction),
        Statement::ShowTables { filter, .. } => Ok(LogicalPlan::ShowTables { filter }),
        other => Ok(LogicalPlan::Passthrough(other)),
    }
}

fn bind_select(query: Query) -> Result<SearchRequest, DataBaseErrors> {
    SearchRequest::from_query(&query)
}

fn bind_delete(delete: Delete) -> Result<LogicalPlan, DataBaseErrors> {
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

    let table_name = resolve_delete_table(&delete)?;
    let filter = delete
        .selection
        .as_ref()
        .map(SearchRequest::parse_sql_expression)
        .transpose()?;

    Ok(LogicalPlan::Delete {
        table_name,
        filter,
    })
}

fn resolve_delete_table(delete: &Delete) -> Result<String, DataBaseErrors> {
    if !delete.tables.is_empty() {
        return delete.tables[0]
            .0
            .last()
            .map(|ident| normalize_identifier(&ident.value))
            .ok_or_else(|| DataBaseErrors::QueryError("Missing DELETE table name".into()));
    }

    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return Err(DataBaseErrors::QueryError(
                    "DELETE supports exactly one target table".into(),
                ));
            }
            match &tables[0].relation {
                TableFactor::Table { name, .. } => name
                    .0
                    .last()
                    .map(|ident| normalize_identifier(&ident.value))
                    .ok_or_else(|| DataBaseErrors::QueryError("Missing DELETE table name".into())),
                _ => Err(DataBaseErrors::QueryError(
                    "Unsupported DELETE target".into(),
                )),
            }
        }
    }
}

fn bind_update(
    table: sqlparser::ast::TableWithJoins,
    assignments: Vec<sqlparser::ast::Assignment>,
    selection: Option<sqlparser::ast::Expr>,
) -> Result<LogicalPlan, DataBaseErrors> {
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .map(|ident| normalize_identifier(&ident.value))
            .ok_or_else(|| DataBaseErrors::QueryError("Missing table name in UPDATE".into()))?,
        _ => {
            return Err(DataBaseErrors::QueryError(
                "Unsupported UPDATE target".into(),
            ));
        }
    };

    let mut bound_assignments = Vec::new();
    for assignment in &assignments {
        let column_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => name
                .0
                .last()
                .map(|ident| normalize_identifier(&ident.value))
                .ok_or_else(|| DataBaseErrors::QueryError("Invalid assignment target".into()))?,
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "Only single-column UPDATE assignments are supported".into(),
                ));
            }
        };
        let value = parse_update_assignment(&column_name, &assignment.value)?;
        bound_assignments.push((column_name, value));
    }

    let filter = selection
        .as_ref()
        .map(SearchRequest::parse_sql_expression)
        .transpose()?;

    Ok(LogicalPlan::Update {
        table_name,
        assignments: bound_assignments,
        filter,
    })
}

fn bind_insert(
    db: &Database,
    insert: Insert,
    transaction: &Transaction,
) -> Result<LogicalPlan, DataBaseErrors> {
    let table_name = insert
        .table_name
        .0
        .last()
        .map(|ident| normalize_identifier(&ident.value))
        .ok_or_else(|| DataBaseErrors::QueryError("Missing table name in INSERT".into()))?;

    let table = db
        .get_table(table_name.clone())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.clone()))?;
    let table = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;

    let source_query = insert
        .source
        .ok_or_else(|| DataBaseErrors::QueryError("INSERT source must be VALUES or query".into()))?;

    let value_rows = match source_query.body.as_ref() {
        SetExpr::Values(values) => &values.rows,
        _ => {
            return Err(DataBaseErrors::QueryError(
                "INSERT only supports VALUES sources".into(),
            ));
        }
    };

    if value_rows.is_empty() {
        return Err(DataBaseErrors::QueryError(
            "INSERT must provide at least one row".into(),
        ));
    }

    let mut rows = Vec::with_capacity(value_rows.len());
    for row in value_rows {
        rows.push(build_insert_data(
            &table,
            transaction,
            &insert.columns,
            row,
        )?);
    }

    Ok(LogicalPlan::Insert { table_name, rows })
}
