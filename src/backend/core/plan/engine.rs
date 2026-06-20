use sqlparser::ast::Statement;

use crate::backend::core::column::ColumnID;
use crate::backend::core::table::TableID;
use crate::backend::core::database::Database;
use crate::backend::core::plan::bind;
use crate::backend::core::plan::execute::{execute, ExecutionResult};
use crate::backend::core::plan::logical::LogicalPlan;
use crate::backend::core::plan::optimize;
use crate::backend::core::plan::parse;
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

/// Parse SQL text into sqlparser AST statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, DataBaseErrors> {
    parse::parse_sql(sql)
}

/// Full pipeline: bind → optimize → execute for relational plans.
pub fn plan_and_execute<F>(
    db: &Database,
    statement: Statement,
    transaction: &Transaction,
    fk_lookup: F,
) -> Result<ExecutionResult, DataBaseErrors>
where
    F: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
{
    let logical = bind::bind_statement(db, statement, transaction)?;
    match logical {
        LogicalPlan::Passthrough(_) | LogicalPlan::ShowTables { .. } => Err(DataBaseErrors::QueryError(
            "Statement must be executed by the wire handler".into(),
        )),
        _ => {
            let physical = optimize::optimize(db, logical, transaction)?;
            execute(db, physical, transaction, fk_lookup)
        }
    }
}
