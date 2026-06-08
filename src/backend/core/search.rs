use std::collections::HashMap;
use std::hash::BuildHasher;

use ahash::AHashMap;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, OrderBy as SqlOrderBy, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::backend::core::row::{DataBaseDataEntry, RowID};
use crate::backend::core::types::DecodedData;
use crate::backend::errors::DataBaseErrors;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SearchOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OrderBy {
    ASC,
    DESC,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SearchExpression {
    Column(String),
    Literal(DecodedData),
    Comparison {
        left: Box<SearchExpression>,
        operator: SearchOperator,
        right: Box<SearchExpression>,
    },
    And(Box<SearchExpression>, Box<SearchExpression>),
    Or(Box<SearchExpression>, Box<SearchExpression>),
    Not(Box<SearchExpression>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortBy {
    pub column_name: String,
    pub order_by: OrderBy,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchRequest {
    pub table_name: String,
    pub projection: Option<Vec<String>>,
    pub filter: Option<SearchExpression>,
    pub order_by: Vec<SortBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub row_id: RowID,
    pub values: AHashMap<String, DataBaseDataEntry>,
}

impl SearchOperator {
    fn from_sql_operator(operator: &BinaryOperator) -> Result<Self, DataBaseErrors> {
        match operator {
            BinaryOperator::Eq => Ok(SearchOperator::Equal),
            BinaryOperator::NotEq => Ok(SearchOperator::NotEqual),
            BinaryOperator::Gt => Ok(SearchOperator::GreaterThan),
            BinaryOperator::Lt => Ok(SearchOperator::LessThan),
            BinaryOperator::GtEq => Ok(SearchOperator::GreaterThanOrEqual),
            BinaryOperator::LtEq => Ok(SearchOperator::LessThanOrEqual),
            _ => Err(DataBaseErrors::QueryError(format!("Unsupported operator: {operator:?}"))),
        }
    }

    fn compare(&self, left: &DecodedData, right: &DecodedData) -> bool {
        match self {
            SearchOperator::Equal => left == right,
            SearchOperator::NotEqual => left != right,
            SearchOperator::GreaterThan => left > right,
            SearchOperator::LessThan => left < right,
            SearchOperator::GreaterThanOrEqual => left >= right,
            SearchOperator::LessThanOrEqual => left <= right,
        }
    }
}

impl SearchExpression {
    fn normalize_identifier(identifier: &str) -> String {
        identifier.to_ascii_lowercase()
    }

    fn resolve_value<S>(
        &self,
        row: &AHashMap<u16, DataBaseDataEntry, S>,
        column_map: &HashMap<String, u16>,
    ) -> Result<DecodedData, DataBaseErrors>
    where
        S: BuildHasher,
    {
        match self {
            SearchExpression::Literal(value) => Ok(value.clone()),
            SearchExpression::Column(name) => {
                let normalized = Self::normalize_identifier(name);
                if let Some(column_id) = column_map.get(&normalized) {
                    Ok(row
                        .get(column_id)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null))
                } else {
                    Err(DataBaseErrors::QueryError(format!(
                        "Column '{name}' not found in table schema",
                    )))
                }
            }
            SearchExpression::Comparison { .. } => Err(DataBaseErrors::QueryError(
                "Comparison expressions cannot be resolved to a scalar value directly".into(),
            )),
            SearchExpression::And(_, _) | SearchExpression::Or(_, _) | SearchExpression::Not(_) => Err(
                DataBaseErrors::QueryError(
                    "Logical expressions cannot be resolved to a scalar value directly".into(),
                ),
            ),
        }
    }

    fn to_boolean(&self, value: &DecodedData) -> Result<bool, DataBaseErrors> {
        match value {
            DataBaseDataEntry::Boolean(flag) => Ok(*flag),
            _ => Err(DataBaseErrors::QueryError(
                "Expected boolean expression in WHERE clause".into(),
            )),
        }
    }

    pub fn evaluate<S>(
        &self,
        row: &AHashMap<u16, DataBaseDataEntry, S>,
        column_map: &HashMap<String, u16>,
    ) -> Result<bool, DataBaseErrors>
    where
        S: BuildHasher,
    {
        match self {
            SearchExpression::Literal(value) => self.to_boolean(value),
            SearchExpression::Column(_) => {
                let value = self.resolve_value(row, column_map)?;
                self.to_boolean(&value)
            }
            SearchExpression::Comparison { left, operator, right } => {
                let left_value = left.resolve_value(row, column_map)?;
                let right_value = right.resolve_value(row, column_map)?;
                Ok(operator.compare(&left_value, &right_value))
            }
            SearchExpression::And(left, right) => Ok(
                left.evaluate(row, column_map)? && right.evaluate(row, column_map)?,
            ),
            SearchExpression::Or(left, right) => Ok(
                left.evaluate(row, column_map)? || right.evaluate(row, column_map)?,
            ),
            SearchExpression::Not(inner) => Ok(!inner.evaluate(row, column_map)?),
        }
    }

    /// If this filter is a single `column = literal` (or `literal = column`), return the
    /// column name and literal value for index lookup.
    pub fn equality_lookup(&self) -> Option<(&str, &DataBaseDataEntry)> {
        match self {
            SearchExpression::Comparison {
                left,
                operator: SearchOperator::Equal,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (SearchExpression::Column(name), SearchExpression::Literal(value)) => {
                    Some((name, value))
                }
                (SearchExpression::Literal(value), SearchExpression::Column(name)) => {
                    Some((name, value))
                }
                _ => None,
            },
            _ => None,
        }
    }
}

impl SearchRequest {
    fn normalize_identifier(identifier: &str) -> String {
        identifier.to_ascii_lowercase()
    }

    fn parse_identifier(expr: &SqlExpr) -> Result<String, DataBaseErrors> {
        match expr {
            SqlExpr::Identifier(identifier) => Ok(Self::normalize_identifier(&identifier.value)),
            SqlExpr::CompoundIdentifier(parts) => parts
                .last()
                .map(|ident| Self::normalize_identifier(&ident.value))
                .ok_or_else(|| {
                    DataBaseErrors::QueryError("Expected a column identifier".into())
                }),
            _ => Err(DataBaseErrors::QueryError(
                "ORDER BY and projection expressions must be simple identifiers".into(),
            )),
        }
    }

    pub fn parse_sql_value(value: &SqlValue) -> Result<DecodedData, DataBaseErrors> {
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
                "Unsupported literal value in WHERE clause: {value:?}",
            ))),
        }
    }

    pub fn parse_sql_expression(expr: &SqlExpr) -> Result<SearchExpression, DataBaseErrors> {
        match expr {
            SqlExpr::Value(value) => Ok(SearchExpression::Literal(Self::parse_sql_value(value)?)),
            SqlExpr::Identifier(identifier) => Ok(SearchExpression::Column(
                Self::normalize_identifier(&identifier.value),
            )),
            SqlExpr::CompoundIdentifier(parts) => parts
                .last()
                .map(|ident| SearchExpression::Column(Self::normalize_identifier(&ident.value)))
                .ok_or_else(|| DataBaseErrors::QueryError("Invalid identifier in expression".into())),
            SqlExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(SearchExpression::And(
                    Box::new(Self::parse_sql_expression(left)?),
                    Box::new(Self::parse_sql_expression(right)?),
                )),
                BinaryOperator::Or => Ok(SearchExpression::Or(
                    Box::new(Self::parse_sql_expression(left)?),
                    Box::new(Self::parse_sql_expression(right)?),
                )),
                _ => Ok(SearchExpression::Comparison {
                    left: Box::new(Self::parse_sql_expression(left)?),
                    operator: SearchOperator::from_sql_operator(op)?,
                    right: Box::new(Self::parse_sql_expression(right)?),
                }),
            },
            SqlExpr::UnaryOp { op: UnaryOperator::Not, expr } => Ok(SearchExpression::Not(
                Box::new(Self::parse_sql_expression(expr)?),
            )),
            SqlExpr::Nested(expr) => Self::parse_sql_expression(expr),
            SqlExpr::IsNull(expr) => Ok(SearchExpression::Comparison {
                left: Box::new(Self::parse_sql_expression(expr)?),
                operator: SearchOperator::Equal,
                right: Box::new(SearchExpression::Literal(DataBaseDataEntry::Null)),
            }),
            SqlExpr::IsNotNull(expr) => Ok(SearchExpression::Comparison {
                left: Box::new(Self::parse_sql_expression(expr)?),
                operator: SearchOperator::NotEqual,
                right: Box::new(SearchExpression::Literal(DataBaseDataEntry::Null)),
            }),
            _ => Err(DataBaseErrors::QueryError(format!(
                "Unsupported SQL expression in WHERE clause: {expr:?}",
            ))),
        }
    }

    fn parse_order_by(order: &SqlOrderBy) -> Result<Vec<SortBy>, DataBaseErrors> {
        order
            .exprs
            .iter()
            .map(|order_by| {
                let column_name = Self::parse_identifier(&order_by.expr)?;
                let order_by_direction = match order_by.asc {
                    Some(false) => OrderBy::DESC,
                    _ => OrderBy::ASC,
                };
                Ok(SortBy {
                    column_name,
                    order_by: order_by_direction,
                })
            })
            .collect::<Result<Vec<_>, DataBaseErrors>>()
    }

    fn parse_limit_or_offset(expr: &SqlExpr) -> Result<usize, DataBaseErrors> {
        match expr {
            SqlExpr::Value(SqlValue::Number(text, _)) => text.parse::<usize>().map_err(|err| {
                DataBaseErrors::QueryError(format!(
                    "Unable to parse LIMIT/OFFSET value '{text}': {err}",
                ))
            }),
            _ => Err(DataBaseErrors::QueryError(
                "LIMIT and OFFSET must be numeric literals".into(),
            )),
        }
    }

    pub fn from_sql(sql: &str) -> Result<Self, DataBaseErrors> {
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)
            .map_err(|err| DataBaseErrors::QueryError(err.to_string()))?;
        let statement = statements
            .pop()
            .ok_or_else(|| DataBaseErrors::QueryError("Empty SQL query".into()))?;

        let query = match statement {
            Statement::Query(query) => *query,
            _ => return Err(DataBaseErrors::QueryError("Only SELECT queries are supported".into())),
        };

        let select = match *query.body {
            SetExpr::Select(select) => select,
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "Only plain SELECT queries are supported".into(),
                ))
            }
        };

        if select.from.len() != 1 {
            return Err(DataBaseErrors::QueryError(
                "SELECT queries must target exactly one table".into(),
            ));
        }

        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => name
                .0
                .last()
                .map(|ident| Self::normalize_identifier(&ident.value))
                .ok_or_else(|| DataBaseErrors::QueryError("Missing table name in FROM clause".into()))?,
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "FROM clause must name a table".into(),
                ))
            }
        };

        let projection = if select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)))
        {
            None
        } else {
            let mut columns = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(SqlExpr::Identifier(identifier)) => {
                        columns.push(Self::normalize_identifier(&identifier.value));
                    }
                    SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                        if let Some(ident) = parts.last() {
                            columns.push(Self::normalize_identifier(&ident.value));
                        }
                    }
                    item => {
                        return Err(DataBaseErrors::QueryError(format!(
                            "Unsupported select item: {item:?}",
                        )))
                    }
                }
            }
            Some(columns)
        };

        let filter = match &select.selection {
            Some(predicate) => Some(Self::parse_sql_expression(predicate)?),
            None => None,
        };

        let order_by = match &query.order_by {
            Some(order_by) => Self::parse_order_by(order_by)?,
            None => Vec::new(),
        };
        let limit = query
            .limit
            .as_ref()
            .map(|expr| Self::parse_limit_or_offset(expr))
            .transpose()?;
        let offset = query
            .offset
            .as_ref()
            .map(|offset| Self::parse_limit_or_offset(&offset.value))
            .transpose()?;

        Ok(SearchRequest {
            table_name,
            projection,
            filter,
            order_by,
            limit,
            offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::core::row::DataBaseDataEntry;

    #[test]
    fn parse_select_with_where_order_by_and_limits() {
        let request = SearchRequest::from_sql(
            "SELECT id, name FROM users WHERE age >= 18 AND active = true ORDER BY name DESC LIMIT 5 OFFSET 2",
        )
        .expect("Failed to parse query");

        assert_eq!(request.table_name, "users");
        assert_eq!(request.projection.unwrap(), vec!["id", "name"]);
        assert!(matches!(request.filter, Some(SearchExpression::And(_, _))));
        assert_eq!(request.order_by.len(), 1);
        assert_eq!(request.order_by[0].column_name, "name");
        assert_eq!(request.order_by[0].order_by, OrderBy::DESC);
        assert_eq!(request.limit, Some(5));
        assert_eq!(request.offset, Some(2));

        let mut row = AHashMap::new();
        row.insert(0, DataBaseDataEntry::IntegerU64(25));
        row.insert(1, DataBaseDataEntry::Boolean(true));

        let mut columns = HashMap::new();
        columns.insert("age".into(), 0);
        columns.insert("active".into(), 1);

        let filter = request.filter.unwrap();
        assert!(filter.evaluate(&row, &columns).unwrap());
    }
}
