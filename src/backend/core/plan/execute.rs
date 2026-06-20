use crate::backend::core::column::ColumnID;
use crate::backend::core::table::TableID;
use crate::backend::core::database::Database;
use crate::backend::core::plan::physical::{AccessPath, PhysicalPlan, PhysicalSelectPlan};
use crate::backend::core::query;
use crate::backend::core::plan::dml::{apply_update_assignment, UpdateAssignment};
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::search::{SearchRequest, SearchResult};
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

#[derive(Debug)]
pub enum ExecutionResult {
    Select {
        column_names: Vec<String>,
        rows: Vec<SearchResult>,
    },
    RowsAffected {
        tag: &'static str,
        count: usize,
    },
    ShowTables(Vec<String>),
}

pub fn execute<F>(
    db: &Database,
    plan: PhysicalPlan,
    transaction: &Transaction,
    mut fk_lookup: F,
) -> Result<ExecutionResult, DataBaseErrors>
where
    F: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
{
    match plan {
        PhysicalPlan::Select(select_plan) => execute_select(db, &select_plan, transaction),
        PhysicalPlan::Delete {
            table_name,
            filter,
            access_path,
        } => {
            let count = execute_delete(db, &table_name, filter, &access_path, transaction)?;
            Ok(ExecutionResult::RowsAffected {
                tag: "DELETE",
                count,
            })
        }
        PhysicalPlan::Update {
            table_name,
            assignments,
            filter,
            access_path,
        } => {
            let count = execute_update(
                db,
                &table_name,
                assignments,
                filter,
                &access_path,
                transaction,
                &mut fk_lookup,
            )?;
            Ok(ExecutionResult::RowsAffected {
                tag: "UPDATE",
                count,
            })
        }
        PhysicalPlan::Insert { table_name, rows } => {
            let count = execute_insert(db, &table_name, rows, transaction, &mut fk_lookup)?;
            Ok(ExecutionResult::RowsAffected {
                tag: "INSERT",
                count,
            })
        }
    }
}

fn execute_select(
    db: &Database,
    plan: &PhysicalSelectPlan,
    transaction: &Transaction,
) -> Result<ExecutionResult, DataBaseErrors> {
    let rows = query::execute_physical_select(db, plan, transaction)?;

    let column_names = if let Some(crate::backend::core::search::AggregateProjection::CountStar {
        output_name,
    }) = &plan.request.aggregate
    {
        vec![output_name.clone()]
    } else if let Some(projection) = &plan.request.projection {
        projection
            .iter()
            .map(|column| column.output_name.clone())
            .collect()
    } else if plan.request.from.has_joins() {
        let mut names: Vec<String> = rows
            .first()
            .map(|row| row.values.keys().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    } else {
        let table = db
            .get_table(plan.request.table_name().to_string())
            .ok_or_else(|| DataBaseErrors::TableNotFound(plan.request.table_name().to_string()))?;
        table.read().unwrap().get_visible_column_names(transaction)
    };

    Ok(ExecutionResult::Select {
        column_names,
        rows,
    })
}

fn execute_delete(
    db: &Database,
    table_name: &str,
    filter: Option<crate::backend::core::search::SearchExpression>,
    access_path: &AccessPath,
    transaction: &Transaction,
) -> Result<usize, DataBaseErrors> {
    let table = db
        .get_table(table_name.to_string())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.to_string()))?;

    let search_request = SearchRequest::single_table(
        table_name.to_string(),
        None,
        filter,
        Vec::new(),
        None,
        None,
    );

    let rows = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?
        .search(&search_request, transaction, Some(access_path))?;

    let mut deleted_count = 0;
    for row in rows {
        table
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?
            .delete_row(row.row_id, transaction)?;
        deleted_count += 1;
    }

    Ok(deleted_count)
}

fn execute_update<F>(
    db: &Database,
    table_name: &str,
    assignments: Vec<(String, UpdateAssignment)>,
    filter: Option<crate::backend::core::search::SearchExpression>,
    access_path: &AccessPath,
    transaction: &Transaction,
    fk_lookup: &mut F,
) -> Result<usize, DataBaseErrors>
where
    F: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
{
    let table = db
        .get_table(table_name.to_string())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.to_string()))?;

    let search_request = SearchRequest::single_table(
        table_name.to_string(),
        None,
        filter,
        Vec::new(),
        None,
        None,
    );

    let visible_columns = {
        let table_read = table
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
        table_read.get_visible_column_map(transaction)?
    };

    let rows = {
        let table_read = table
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
        table_read.search(&search_request, transaction, Some(access_path))?
    };

    let mut updated_count = 0;
    for row in rows {
        let mut row_data_by_id = ahash::AHashMap::new();
        for (column_name, value) in row.values {
            let column_id = visible_columns
                .get(&column_name)
                .ok_or_else(|| {
                    DataBaseErrors::QueryError(format!(
                        "Column '{column_name}' not found during update",
                    ))
                })?;
            row_data_by_id.insert(*column_id, value);
        }

        for (column_name, assignment) in &assignments {
            let column_id = visible_columns.get(column_name).ok_or_else(|| {
                DataBaseErrors::QueryError(format!("Assignment column '{column_name}' not found"))
            })?;
            let expected_type = {
                let table_read = table
                    .read()
                    .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
                table_read
                    .column_data_type(*column_id, transaction)?
                    .ok_or_else(|| {
                        DataBaseErrors::QueryError(format!(
                            "Assignment column '{column_name}' not found",
                        ))
                    })?
            };
            let value = apply_update_assignment(
                assignment,
                &row_data_by_id,
                *column_id,
                &expected_type,
            )?;
            row_data_by_id.insert(*column_id, value);
        }

        {
            let table_read = table
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
            table_read.update_row(row.row_id, row_data_by_id, transaction, &mut *fk_lookup)?;
        }
        updated_count += 1;
    }

    Ok(updated_count)
}

fn execute_insert<F>(
    db: &Database,
    table_name: &str,
    rows: Vec<ahash::AHashMap<ColumnID, DataBaseDataEntry>>,
    transaction: &Transaction,
    fk_lookup: &mut F,
) -> Result<usize, DataBaseErrors>
where
    F: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
{
    let table = db
        .get_table(table_name.to_string())
        .ok_or_else(|| DataBaseErrors::TableNotFound(table_name.to_string()))?;
    let table_read = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;

    let mut inserted_count = 0;
    for row_data in rows {
        table_read.insert_row(row_data, transaction, &mut *fk_lookup)?;
        inserted_count += 1;
    }

    Ok(inserted_count)
}
