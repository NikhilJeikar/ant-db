use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::backend::errors::DataBaseErrors;

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, DataBaseErrors> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|err| DataBaseErrors::QueryError(err.to_string()))
}

pub fn parse_one(sql: &str) -> Result<Statement, DataBaseErrors> {
    let mut statements = parse_sql(sql)?;
    statements
        .pop()
        .ok_or_else(|| DataBaseErrors::QueryError("Empty SQL statement".into()))
}
