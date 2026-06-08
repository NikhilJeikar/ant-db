use std::collections::HashMap;

use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::search::SearchRequest;

/// Storage access method chosen by the optimizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessPath {
    SeqScan,
    IndexEquality {
        column_name: String,
        value: DataBaseDataEntry,
    },
}

#[derive(Debug, Clone)]
pub struct PhysicalSelectPlan {
    pub request: SearchRequest,
    /// Per-table access path keyed by normalized table name.
    pub access_paths: HashMap<String, AccessPath>,
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Select(PhysicalSelectPlan),
    Delete {
        table_name: String,
        filter: Option<crate::backend::core::search::SearchExpression>,
        access_path: AccessPath,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, DataBaseDataEntry)>,
        filter: Option<crate::backend::core::search::SearchExpression>,
        access_path: AccessPath,
    },
    Insert {
        table_name: String,
        rows: Vec<ahash::AHashMap<crate::backend::core::column::ColumnID, DataBaseDataEntry>>,
    },
}
