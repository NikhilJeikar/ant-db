use crate::backend::core::database::Database;
use crate::backend::core::plan::logical::LogicalPlan;
use crate::backend::core::plan::physical::{AccessPath, PhysicalPlan, PhysicalSelectPlan};
use crate::backend::core::search::{SearchExpression, SearchRequest};
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

pub fn optimize(
    db: &Database,
    plan: LogicalPlan,
    transaction: &Transaction,
) -> Result<PhysicalPlan, DataBaseErrors> {
    match plan {
        LogicalPlan::Select(request) => Ok(PhysicalPlan::Select(optimize_select(
            db, request, transaction,
        )?)),
        LogicalPlan::Delete {
            table_name,
            filter,
        } => {
            let access_path = choose_filter_access_path(db, &table_name, filter.as_ref(), transaction)?;
            Ok(PhysicalPlan::Delete {
                table_name,
                filter,
                access_path,
            })
        }
        LogicalPlan::Update {
            table_name,
            assignments,
            filter,
        } => {
            let access_path = choose_filter_access_path(db, &table_name, filter.as_ref(), transaction)?;
            Ok(PhysicalPlan::Update {
                table_name,
                assignments,
                filter,
                access_path,
            })
        }
        LogicalPlan::Insert { table_name, rows } => Ok(PhysicalPlan::Insert { table_name, rows }),
        other => Err(DataBaseErrors::QueryError(format!(
            "Optimizer does not handle this plan type yet: {other:?}",
        ))),
    }
}

pub fn optimize_select(
    db: &Database,
    request: SearchRequest,
    transaction: &Transaction,
) -> Result<PhysicalSelectPlan, DataBaseErrors> {
    let mut access_paths = std::collections::HashMap::new();

    let base_name = request.from.base.table_name.clone();
    access_paths.insert(
        base_name.clone(),
        choose_filter_access_path(db, &base_name, request.filter.as_ref(), transaction)?,
    );

    for join in &request.from.joins {
        access_paths
            .entry(join.table.table_name.clone())
            .or_insert(AccessPath::SeqScan);
    }

    Ok(PhysicalSelectPlan {
        request,
        access_paths,
    })
}

fn choose_filter_access_path(
    db: &Database,
    table_name: &str,
    filter: Option<&SearchExpression>,
    transaction: &Transaction,
) -> Result<AccessPath, DataBaseErrors> {
    let Some(filter) = filter else {
        return Ok(AccessPath::SeqScan);
    };

    let Some((column_name, value)) = filter.equality_lookup() else {
        return Ok(AccessPath::SeqScan);
    };

    let table = db
        .get_table(table_name.to_string())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.to_string()))?;
    let table = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;

    if table.column_is_indexed(column_name, transaction)? {
        Ok(AccessPath::IndexEquality {
            column_name: column_name.to_string(),
            value: value.clone(),
        })
    } else {
        Ok(AccessPath::SeqScan)
    }
}
