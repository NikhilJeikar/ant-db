use ahash::AHashMap;
use ordered_float::NotNan;
use sqlparser::ast::{Expr as SqlExpr, Ident, UnaryOperator, Value as SqlValue};

use crate::backend::core::column::ColumnID;
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::table::Table;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

pub fn normalize_identifier(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

pub fn parse_sql_value(value: &SqlValue) -> Result<DataBaseDataEntry, DataBaseErrors> {
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

pub fn parse_sql_literal(expr: &SqlExpr) -> Result<DataBaseDataEntry, DataBaseErrors> {
    match expr {
        SqlExpr::Value(value) => parse_sql_value(value),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let literal = parse_sql_literal(expr)?;
            match literal {
                DataBaseDataEntry::IntegerU64(value) => {
                    Ok(DataBaseDataEntry::IntegerI64(-(value as i64)))
                }
                DataBaseDataEntry::IntegerI64(value) => Ok(DataBaseDataEntry::IntegerI64(-value)),
                DataBaseDataEntry::IntegerU128(value) => {
                    Ok(DataBaseDataEntry::IntegerI128(-(value as i128)))
                }
                DataBaseDataEntry::IntegerI128(value) => {
                    Ok(DataBaseDataEntry::IntegerI128(-value))
                }
                DataBaseDataEntry::FloatF64(value) => Ok(DataBaseDataEntry::FloatF64(
                    NotNan::new(-value.into_inner()).map_err(|_| {
                        DataBaseErrors::QueryError("Floating point literal must not be NaN".into())
                    })?,
                )),
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

pub fn build_insert_data(
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
            .ok_or_else(|| DataBaseErrors::QueryError(format!("Column '{column_name}' not found")))?;
        let value = parse_sql_literal(expr)?;
        row_data.insert(*column_id, value);
    }

    Ok(row_data)
}
