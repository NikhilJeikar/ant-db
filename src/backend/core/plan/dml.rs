use std::time::{SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use ordered_float::NotNan;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArguments, Ident, UnaryOperator,
    Value as SqlValue,
};

use crate::backend::core::column::{ColumnID, DataBaseDataType};
use crate::backend::core::row::DataBaseDataEntry;
use crate::backend::core::table::Table;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

pub fn normalize_identifier(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

#[derive(Debug, Clone)]
pub enum UpdateAssignment {
    Set(DataBaseDataEntry),
    AddColumn {
        column: String,
        delta: DataBaseDataEntry,
    },
}

pub fn current_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
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
            } else if let Ok(signed) = text.parse::<i32>() {
                Ok(DataBaseDataEntry::IntegerI32(signed))
            } else if let Ok(signed) = text.parse::<i64>() {
                Ok(DataBaseDataEntry::IntegerI64(signed))
            } else if let Ok(unsigned) = text.parse::<u64>() {
                Ok(DataBaseDataEntry::IntegerU64(unsigned))
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

fn function_name(function: &Function) -> String {
    function
        .name
        .0
        .iter()
        .map(|ident| ident.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_timestamp_function(name: &str) -> Result<DataBaseDataEntry, DataBaseErrors> {
    match name {
        "current_timestamp" | "now" | "localtimestamp" | "transaction_timestamp" => {
            Ok(DataBaseDataEntry::Timestamp(current_timestamp_micros()))
        }
        _ => Err(DataBaseErrors::QueryError(format!(
            "Unsupported function in INSERT/UPDATE expression: {name}",
        ))),
    }
}

pub fn parse_sql_expression(expr: &SqlExpr) -> Result<DataBaseDataEntry, DataBaseErrors> {
    match expr {
        SqlExpr::Value(value) => parse_sql_value(value),
        SqlExpr::Function(function) => {
            if !matches!(function.args, FunctionArguments::None) {
                return Err(DataBaseErrors::QueryError(
                    "Function arguments are unsupported in INSERT/UPDATE expressions".into(),
                ));
            }
            parse_timestamp_function(&function_name(function))
        }
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let literal = parse_sql_expression(expr)?;
            match literal {
                DataBaseDataEntry::IntegerI8(value) => Ok(DataBaseDataEntry::IntegerI8(-value)),
                DataBaseDataEntry::IntegerI16(value) => Ok(DataBaseDataEntry::IntegerI16(-value)),
                DataBaseDataEntry::IntegerI32(value) => Ok(DataBaseDataEntry::IntegerI32(-value)),
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
        SqlExpr::Nested(expr) => parse_sql_expression(expr),
        _ => Err(DataBaseErrors::QueryError(
            "Unsupported expression in INSERT/UPDATE values".into(),
        )),
    }
}

pub fn parse_sql_literal(expr: &SqlExpr) -> Result<DataBaseDataEntry, DataBaseErrors> {
    parse_sql_expression(expr)
}

fn parse_column_reference(expr: &SqlExpr) -> Result<String, DataBaseErrors> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(normalize_identifier(&ident.value)),
        SqlExpr::CompoundIdentifier(idents) => match idents.last() {
            Some(ident) => Ok(normalize_identifier(&ident.value)),
            None => Err(DataBaseErrors::QueryError(
                "Missing column name in UPDATE expression".into(),
            )),
        },
        _ => Err(DataBaseErrors::QueryError(
            "Expected a column identifier in UPDATE expression".into(),
        )),
    }
}

pub fn parse_update_assignment(
    target_column: &str,
    expr: &SqlExpr,
) -> Result<UpdateAssignment, DataBaseErrors> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Plus,
            right,
        } => {
            let (column_expr, delta_expr) = match (
                parse_column_reference(left),
                parse_sql_expression(right),
            ) {
                (Ok(column), Ok(delta)) => (column, delta),
                (Err(_), Ok(_)) => (
                    parse_column_reference(right)?,
                    parse_sql_expression(left)?,
                ),
                (Ok(column), Err(_)) => (column, parse_sql_expression(right)?),
                (Err(err), Err(_)) => return Err(err),
            };
            if column_expr != target_column {
                return Err(DataBaseErrors::QueryError(format!(
                    "UPDATE expression column '{column_expr}' must match assignment target '{target_column}'",
                )));
            }
            Ok(UpdateAssignment::AddColumn {
                column: target_column.to_string(),
                delta: delta_expr,
            })
        }
        _ => Ok(UpdateAssignment::Set(parse_sql_expression(expr)?)),
    }
}

pub fn entry_as_i64(value: &DataBaseDataEntry) -> Result<i64, DataBaseErrors> {
    match value {
        DataBaseDataEntry::Null => Ok(0),
        DataBaseDataEntry::IntegerI8(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerI16(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerI32(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerI64(v) => Ok(*v),
        DataBaseDataEntry::IntegerI128(v) => i64::try_from(*v).map_err(|_| {
            DataBaseErrors::QueryError("Integer value is out of range for arithmetic".into())
        }),
        DataBaseDataEntry::IntegerU8(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerU16(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerU32(v) => Ok(*v as i64),
        DataBaseDataEntry::IntegerU64(v) => i64::try_from(*v).map_err(|_| {
            DataBaseErrors::QueryError("Integer value is out of range for arithmetic".into())
        }),
        DataBaseDataEntry::IntegerU128(v) => i64::try_from(*v).map_err(|_| {
            DataBaseErrors::QueryError("Integer value is out of range for arithmetic".into())
        }),
        _ => Err(DataBaseErrors::QueryError(
            "Only integer values support additive UPDATE expressions".into(),
        )),
    }
}

pub fn add_numeric_entries(
    left: DataBaseDataEntry,
    right: DataBaseDataEntry,
) -> Result<DataBaseDataEntry, DataBaseErrors> {
    let sum = entry_as_i64(&left)?.saturating_add(entry_as_i64(&right)?);
    Ok(DataBaseDataEntry::IntegerI64(sum))
}

pub fn coerce_to_type(
    value: DataBaseDataEntry,
    expected: &DataBaseDataType,
) -> Result<DataBaseDataEntry, DataBaseErrors> {
    if value.data_type() == *expected || matches!(value, DataBaseDataEntry::Null) {
        return Ok(value);
    }

    match (&value, expected) {
        (DataBaseDataEntry::IntegerI64(v), DataBaseDataType::IntegerI32) => {
            i32::try_from(*v)
                .map(DataBaseDataEntry::IntegerI32)
                .map_err(|_| DataBaseErrors::QueryError("Integer value out of range for column".into()))
        }
        (DataBaseDataEntry::IntegerI64(v), DataBaseDataType::IntegerI64) => {
            Ok(DataBaseDataEntry::IntegerI64(*v))
        }
        (DataBaseDataEntry::IntegerI32(v), DataBaseDataType::IntegerI64) => {
            Ok(DataBaseDataEntry::IntegerI64(*v as i64))
        }
        (DataBaseDataEntry::IntegerU64(v), DataBaseDataType::IntegerI32) => {
            i32::try_from(*v)
                .map(DataBaseDataEntry::IntegerI32)
                .map_err(|_| DataBaseErrors::QueryError("Integer value out of range for column".into()))
        }
        (DataBaseDataEntry::IntegerU64(v), DataBaseDataType::IntegerI64) => {
            i64::try_from(*v)
                .map(DataBaseDataEntry::IntegerI64)
                .map_err(|_| DataBaseErrors::QueryError("Integer value out of range for column".into()))
        }
        (DataBaseDataEntry::Timestamp(v), DataBaseDataType::Timestamp) => {
            Ok(DataBaseDataEntry::Timestamp(*v))
        }
        _ => Err(DataBaseErrors::QueryError(format!(
            "Cannot coerce value type {} to {}",
            value.data_type().name(),
            expected.name(),
        ))),
    }
}

pub fn apply_update_assignment(
    assignment: &UpdateAssignment,
    row_values: &AHashMap<ColumnID, DataBaseDataEntry>,
    column_id: ColumnID,
    expected_type: &DataBaseDataType,
) -> Result<DataBaseDataEntry, DataBaseErrors> {
    let value = match assignment {
        UpdateAssignment::Set(value) => value.clone(),
        UpdateAssignment::AddColumn { column: _, delta } => {
            let current = row_values
                .get(&column_id)
                .cloned()
                .unwrap_or(DataBaseDataEntry::Null);
            add_numeric_entries(current, delta.clone())?
        }
    };
    coerce_to_type(value, expected_type)
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
        let column_type = table
            .column_data_type(*column_id, transaction)?
            .ok_or_else(|| DataBaseErrors::QueryError(format!("Column '{column_name}' not found")))?;
        let value = coerce_to_type(parse_sql_expression(expr)?, &column_type)?;
        row_data.insert(*column_id, value);
    }

    Ok(row_data)
}
