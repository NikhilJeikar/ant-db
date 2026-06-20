use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nohash_hasher::BuildNoHashHasher;

use ahash::AHashMap;

use crate::backend::core::database::Database;
use crate::backend::core::column::ColumnID;
use crate::backend::core::row::{DataBaseDataEntry, RowData, RowID};
use crate::backend::core::plan::physical::PhysicalSelectPlan;
use crate::backend::core::search::{
    FromClause, JoinSpec, JoinType, OrderBy, ProjectionColumn, RowEvaluationContext, SearchRequest,
    SearchResult, SortBy, TableSource,
};
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

#[derive(Clone)]
struct TableBinding {
    alias: String,
    table_name: String,
    row_id: RowID,
    column_map: Arc<HashMap<String, ColumnID>>,
    data: Arc<RowData>,
}

#[derive(Clone)]
struct JoinedRow {
    tables: Arc<[TableBinding]>,
}

pub fn execute_search(
    db: &Database,
    request: &SearchRequest,
    transaction: &Transaction,
) -> Result<Vec<SearchResult>, DataBaseErrors> {
    let physical = crate::backend::core::plan::optimize::optimize_select(
        db,
        request.clone(),
        transaction,
    )?;
    execute_physical_select(db, &physical, transaction)
}

pub fn execute_physical_select(
    db: &Database,
    plan: &PhysicalSelectPlan,
    transaction: &Transaction,
) -> Result<Vec<SearchResult>, DataBaseErrors> {
    let request = &plan.request;
    if !request.from.has_joins() {
        let table = db
            .get_table(request.table_name().to_string())
            .ok_or_else(|| DataBaseErrors::TableNotFound(request.table_name().to_string()))?;
        let access_path = plan
            .access_paths
            .get(&request.from.base.table_name)
            .map(|path| path as _);
        return table.read().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire table read lock".into())
        })?
        .search(request, transaction, access_path);
    }

    execute_join_search(db, plan, transaction)
}

fn execute_join_search(
    db: &Database,
    plan: &PhysicalSelectPlan,
    transaction: &Transaction,
) -> Result<Vec<SearchResult>, DataBaseErrors> {
    let request = &plan.request;
    let schema = resolve_join_schema(db, &request.from, transaction)?;
    let projection = resolve_projection(&request.from, &schema, &request.projection)?;

    let mut current = initial_rows(db, &request.from.base, transaction)?;
    for (join, right_schema) in request.from.joins.iter().zip(schema.join_tables.iter()) {
        current = apply_join(db, current, join, right_schema, transaction)?;
    }

    let mut results = Vec::new();
    for joined in current {
        let ctx = build_row_context(&joined);
        let matches = match &request.filter {
            Some(filter) => filter.evaluate(&ctx)?,
            None => true,
        };
        if !matches {
            continue;
        }

        let row_id = joined
            .tables
            .first()
            .map(|binding| binding.row_id)
            .unwrap_or(0);
        let mut values = AHashMap::new();
        for column in &projection {
            let value = resolve_projection_value(&joined, column)?;
            values.insert(column.output_name.clone(), value);
        }
        results.push(SearchResult { row_id, values });
    }

    if !request.order_by.is_empty() {
        results.sort_by(|left, right| {
            compare_rows(left, right, &request.order_by)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    let offset = request.offset.unwrap_or(0);
    Ok(results
        .into_iter()
        .skip(offset)
        .take(request.limit.unwrap_or(usize::MAX))
        .collect())
}

struct JoinTableSchema {
    source: TableSource,
    column_map: HashMap<String, ColumnID>,
    column_names: Vec<String>,
}

struct JoinSchema {
    base: JoinTableSchema,
    join_tables: Vec<JoinTableSchema>,
}

fn resolve_join_schema(
    db: &Database,
    from: &FromClause,
    transaction: &Transaction,
) -> Result<JoinSchema, DataBaseErrors> {
    let mut aliases = HashSet::new();
    let base = load_table_schema(db, &from.base, transaction)?;
    aliases.insert(base.source.alias_or_name().to_string());

    let mut join_tables = Vec::new();
    for join in &from.joins {
        let schema = load_table_schema(db, &join.table, transaction)?;
        let alias = schema.source.alias_or_name().to_string();
        if !aliases.insert(alias.clone()) {
            return Err(DataBaseErrors::QueryError(format!(
                "Duplicate table alias '{alias}' in JOIN",
            )));
        }
        join_tables.push(schema);
    }

    Ok(JoinSchema { base, join_tables })
}

fn load_table_schema(
    db: &Database,
    source: &TableSource,
    transaction: &Transaction,
) -> Result<JoinTableSchema, DataBaseErrors> {
    let table = db
        .get_table(source.table_name.clone())
        .ok_or_else(|| DataBaseErrors::TableNotFound(source.table_name.clone()))?;
    let column_map = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?
        .get_visible_column_map(transaction)?;
    let mut column_names: Vec<_> = column_map.keys().cloned().collect();
    column_names.sort();
    Ok(JoinTableSchema {
        source: source.clone(),
        column_map,
        column_names,
    })
}

fn resolve_projection(
    from: &FromClause,
    schema: &JoinSchema,
    projection: &Option<Vec<ProjectionColumn>>,
) -> Result<Vec<ProjectionColumn>, DataBaseErrors> {
    if let Some(columns) = projection {
        return Ok(columns.clone());
    }

    if !from.has_joins() {
        return Err(DataBaseErrors::QueryError(
            "Wildcard projection must be resolved by single-table search".into(),
        ));
    }

    let mut all = Vec::new();
    append_table_projection(&mut all, &schema.base)?;
    for table_schema in &schema.join_tables {
        append_table_projection(&mut all, table_schema)?;
    }
    Ok(all)
}

fn append_table_projection(
    projection: &mut Vec<ProjectionColumn>,
    table_schema: &JoinTableSchema,
) -> Result<(), DataBaseErrors> {
    let alias = table_schema.source.alias_or_name().to_string();
    for column_name in &table_schema.column_names {
        projection.push(ProjectionColumn {
            output_name: format!("{alias}.{column_name}"),
            table_alias: Some(alias.clone()),
            column_name: column_name.clone(),
        });
    }
    Ok(())
}

fn initial_rows(
    db: &Database,
    source: &TableSource,
    transaction: &Transaction,
) -> Result<Vec<JoinedRow>, DataBaseErrors> {
    let table = db
        .get_table(source.table_name.clone())
        .ok_or_else(|| DataBaseErrors::TableNotFound(source.table_name.clone()))?;
    let table = table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
    let column_map = Arc::new(table.get_visible_column_map(transaction)?);
    let alias = source.alias_or_name().to_string();
    let table_name = source.table_name.clone();

    let mut rows = Vec::new();
    table.for_each_visible_row(transaction, |row_id, data| {
        rows.push(JoinedRow {
            tables: Arc::from([TableBinding {
                alias: alias.clone(),
                table_name: table_name.clone(),
                row_id,
                column_map: column_map.clone(),
                data: Arc::new(data),
            }]),
        });
        Ok(())
    })?;
    Ok(rows)
}

fn apply_join(
    db: &Database,
    current: Vec<JoinedRow>,
    join: &JoinSpec,
    right_schema: &JoinTableSchema,
    transaction: &Transaction,
) -> Result<Vec<JoinedRow>, DataBaseErrors> {
    let right_table = db
        .get_table(join.table.table_name.clone())
        .ok_or_else(|| DataBaseErrors::TableNotFound(join.table.table_name.clone()))?;
    let right_table = right_table
        .read()
        .map_err(|_| DataBaseErrors::QueryError("Failed to acquire table read lock".into()))?;
    let right_alias = join.table.alias_or_name();
    let right_column_map = Arc::new(right_schema.column_map.clone());
    let right_table_name = join.table.table_name.clone();

    if join.join_type == JoinType::Cross {
        let mut next = Vec::new();
        for partial in current {
            right_table.for_each_visible_row(transaction, |row_id, data| {
                next.push(append_binding(
                    &partial,
                    right_alias,
                    &right_table_name,
                    row_id,
                    right_column_map.clone(),
                    Arc::new(data),
                ));
                Ok(())
            })?;
        }
        return Ok(next);
    }

    let mut next = Vec::new();
    for partial in current {
        let ctx = build_row_context(&partial);
        let use_index_probe = join
            .on
            .as_ref()
            .and_then(|on| on.join_equality_lookup(&ctx, right_alias));

        if let Some((column_name, value)) = use_index_probe {
            let column_id = right_schema
                .column_map
                .get(&column_name.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    DataBaseErrors::QueryError(format!(
                        "Join column '{column_name}' not found on table '{}'",
                        join.table.table_name
                    ))
                })?;
            let right_candidates =
                right_table.lookup_rows_by_column_value(column_id, &value, transaction)?;

            let mut matched = false;
            for right_row_id in right_candidates {
                let Some(right_data) =
                    right_table.lookup_visible_row_data(right_row_id, transaction)?
                else {
                    continue;
                };
                let candidate = append_binding(
                    &partial,
                    right_alias,
                    &right_table_name,
                    right_row_id,
                    right_column_map.clone(),
                    right_data,
                );

                if join_matches(&candidate, join)? {
                    matched = true;
                    next.push(candidate);
                }
            }

            if !matched && join.join_type == JoinType::LeftOuter {
                next.push(append_binding(
                    &partial,
                    right_alias,
                    &right_table_name,
                    0,
                    right_column_map.clone(),
                    Arc::new(RowData::with_hasher(BuildNoHashHasher::default())),
                ));
            }
            continue;
        }

        let mut matched = false;
        right_table.for_each_visible_row(transaction, |right_row_id, data| {
            let candidate = append_binding(
                &partial,
                right_alias,
                &right_table_name,
                right_row_id,
                right_column_map.clone(),
                Arc::new(data),
            );

            if join_matches(&candidate, join)? {
                matched = true;
                next.push(candidate);
            }
            Ok(())
        })?;

        if !matched && join.join_type == JoinType::LeftOuter {
            next.push(append_binding(
                &partial,
                right_alias,
                &right_table_name,
                0,
                right_column_map.clone(),
                Arc::new(RowData::with_hasher(BuildNoHashHasher::default())),
            ));
        }
    }

    Ok(next)
}

fn append_binding(
    joined: &JoinedRow,
    alias: &str,
    table_name: &str,
    row_id: RowID,
    column_map: Arc<HashMap<String, ColumnID>>,
    data: Arc<RowData>,
) -> JoinedRow {
    let mut tables = Vec::with_capacity(joined.tables.len() + 1);
    tables.extend_from_slice(&joined.tables);
    tables.push(TableBinding {
        alias: alias.to_string(),
        table_name: table_name.to_string(),
        row_id,
        column_map,
        data,
    });
    JoinedRow {
        tables: Arc::from(tables),
    }
}

fn join_matches(joined: &JoinedRow, join: &JoinSpec) -> Result<bool, DataBaseErrors> {
    match join.join_type {
        JoinType::Cross => Ok(true),
        JoinType::Inner | JoinType::LeftOuter => {
            if let Some(on) = &join.on {
                return on.evaluate(&build_row_context(joined));
            }
            if let Some(columns) = &join.using_columns {
                let right = joined.tables.last().expect("right table must exist");
                let left = joined
                    .tables
                    .iter()
                    .rev()
                    .nth(1)
                    .ok_or_else(|| DataBaseErrors::QueryError("Invalid JOIN state".into()))?;
                for column in columns {
                    let left_value = left
                        .data
                        .get(left.column_map.get(column).ok_or_else(|| {
                            DataBaseErrors::QueryError(format!(
                                "USING column '{column}' not found on '{}'",
                                left.table_name
                            ))
                        })?)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    let right_value = right
                        .data
                        .get(right.column_map.get(column).ok_or_else(|| {
                            DataBaseErrors::QueryError(format!(
                                "USING column '{column}' not found on '{}'",
                                right.table_name
                            ))
                        })?)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    if left_value != right_value {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Err(DataBaseErrors::QueryError(
                "JOIN requires an ON or USING clause".into(),
            ))
        }
    }
}

fn build_row_context(joined: &JoinedRow) -> RowEvaluationContext {
    let mut column_owners: HashMap<String, usize> = HashMap::new();
    for binding in joined.tables.iter() {
        for column_name in binding.column_map.keys() {
            *column_owners.entry(column_name.clone()).or_default() += 1;
        }
    }

    let mut values = HashMap::new();
    for binding in joined.tables.iter() {
        for (column_name, column_id) in binding.column_map.iter() {
            let value = binding
                .data
                .get(column_id)
                .cloned()
                .unwrap_or(DataBaseDataEntry::Null);
            values.insert(
                format!("{}.{}", binding.alias.to_ascii_lowercase(), column_name),
                value.clone(),
            );
            if column_owners.get(column_name) == Some(&1) {
                values.insert(column_name.clone(), value);
            }
        }
    }

    RowEvaluationContext { values }
}

fn resolve_projection_value(
    joined: &JoinedRow,
    column: &ProjectionColumn,
) -> Result<DataBaseDataEntry, DataBaseErrors> {
    let table_alias = column
        .table_alias
        .as_deref()
        .or_else(|| {
            if joined.tables.len() == 1 {
                Some(joined.tables[0].alias.as_str())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            DataBaseErrors::QueryError(format!(
                "Column '{}' is ambiguous; qualify it with a table alias",
                column.column_name
            ))
        })?;

    let binding = joined
        .tables
        .iter()
        .find(|table| table.alias == table_alias)
        .ok_or_else(|| {
            DataBaseErrors::QueryError(format!("Table alias '{table_alias}' not found in JOIN"))
        })?;
    let column_id = binding
        .column_map
        .get(&column.column_name)
        .ok_or_else(|| {
            DataBaseErrors::QueryError(format!(
                "Column '{}' not found on '{}'",
                column.column_name, binding.table_name
            ))
        })?;
    Ok(binding
        .data
        .get(column_id)
        .cloned()
        .unwrap_or(DataBaseDataEntry::Null))
}

fn compare_rows(
    left: &SearchResult,
    right: &SearchResult,
    sort_columns: &[SortBy],
) -> Result<std::cmp::Ordering, DataBaseErrors> {
    for sort_by in sort_columns {
        let left_key = sort_key(&sort_by.column_name, sort_by.table_alias.as_deref())?;
        let right_key = sort_key(&sort_by.column_name, sort_by.table_alias.as_deref())?;
        let left_value = left
            .values
            .get(&left_key)
            .or_else(|| left.values.get(&sort_by.column_name))
            .unwrap_or(&DataBaseDataEntry::Null);
        let right_value = right
            .values
            .get(&right_key)
            .or_else(|| right.values.get(&sort_by.column_name))
            .unwrap_or(&DataBaseDataEntry::Null);
        let order = left_value.cmp(right_value);
        if order != std::cmp::Ordering::Equal {
            return Ok(match sort_by.order_by {
                OrderBy::ASC => order,
                OrderBy::DESC => order.reverse(),
            });
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn sort_key(column_name: &str, table_alias: Option<&str>) -> Result<String, DataBaseErrors> {
    if let Some(alias) = table_alias {
        return Ok(format!(
            "{}.{}",
            alias.to_ascii_lowercase(),
            column_name.to_ascii_lowercase()
        ));
    }
    Ok(column_name.to_ascii_lowercase())
}
