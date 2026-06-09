use ahash::AHashMap;

use sqlparser::ast::ShowStatementFilter;

use crate::backend::core::column::ColumnID;
use crate::backend::core::plan::dml::UpdateAssignment;
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::search::{SearchExpression, SearchRequest};

/// Analyzer output: what the query means before optimization.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Select(SearchRequest),
    Delete {
        table_name: String,
        filter: Option<SearchExpression>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, UpdateAssignment)>,
        filter: Option<SearchExpression>,
    },
    Insert {
        table_name: String,
        rows: Vec<AHashMap<ColumnID, DataBaseDataEntry>>,
    },
    ShowTables {
        filter: Option<ShowStatementFilter>,
    },
    /// Statements handled outside the relational planner (DDL, txn control, COPY setup, etc.).
    Passthrough(sqlparser::ast::Statement),
}
