use std::collections::HashMap;

use ahash::AHashMap;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    Join, JoinConstraint, JoinOperator, ObjectName, OrderBy as SqlOrderBy, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::backend::core::row::{DataBaseDataEntry, RowID};
use crate::backend::core::types::DecodedData;
use crate::backend::errors::DataBaseErrors;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    Cross,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TableSource {
    pub table_name: String,
    pub alias: Option<String>,
}

impl TableSource {
    pub fn alias_or_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table_name)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct JoinSpec {
    pub table: TableSource,
    pub join_type: JoinType,
    pub on: Option<SearchExpression>,
    pub using_columns: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub base: TableSource,
    pub joins: Vec<JoinSpec>,
}

impl FromClause {
    pub fn has_joins(&self) -> bool {
        !self.joins.is_empty()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SearchExpression {
    Column(String),
    QualifiedColumn { table: String, column: String },
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProjectionColumn {
    pub output_name: String,
    pub table_alias: Option<String>,
    pub column_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SortBy {
    pub table_alias: Option<String>,
    pub column_name: String,
    pub order_by: OrderBy,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AggregateProjection {
    CountStar { output_name: String },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub from: FromClause,
    pub projection: Option<Vec<ProjectionColumn>>,
    #[serde(default)]
    pub aggregate: Option<AggregateProjection>,
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

/// Column values keyed by `alias.column` and, when unambiguous, bare column name.
#[derive(Debug, Clone, Default)]
pub struct RowEvaluationContext {
    pub(crate) values: HashMap<String, DataBaseDataEntry>,
}

impl RowEvaluationContext {
    fn normalize_identifier(identifier: &str) -> String {
        identifier.to_ascii_lowercase()
    }

    pub fn resolve(
        &self,
        table: Option<&str>,
        column: &str,
    ) -> Result<&DataBaseDataEntry, DataBaseErrors> {
        let column = Self::normalize_identifier(column);
        if let Some(table) = table {
            let table = Self::normalize_identifier(table);
            return self
                .values
                .get(&format!("{table}.{column}"))
                .ok_or_else(|| {
                    DataBaseErrors::QueryError(format!(
                        "Column '{table}.{column}' not found in joined row",
                    ))
                });
        }

        self.values.get(&column).ok_or_else(|| {
            DataBaseErrors::QueryError(format!(
                "Column '{column}' is ambiguous or not found in joined row",
            ))
        })
    }
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

    fn resolve_value(&self, ctx: &RowEvaluationContext) -> Result<DecodedData, DataBaseErrors> {
        match self {
            SearchExpression::Literal(value) => Ok(value.clone()),
            SearchExpression::Column(name) => Ok(ctx.resolve(None, name)?.clone()),
            SearchExpression::QualifiedColumn { table, column } => {
                Ok(ctx.resolve(Some(table), column)?.clone())
            }
            SearchExpression::Comparison { .. } => Err(DataBaseErrors::QueryError(
                "Comparison expressions cannot be resolved to a scalar value directly".into(),
            )),
            SearchExpression::And(_, _) | SearchExpression::Or(_, _) | SearchExpression::Not(_) => {
                Err(DataBaseErrors::QueryError(
                    "Logical expressions cannot be resolved to a scalar value directly".into(),
                ))
            }
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

    pub fn evaluate(&self, ctx: &RowEvaluationContext) -> Result<bool, DataBaseErrors> {
        match self {
            SearchExpression::Literal(value) => self.to_boolean(value),
            SearchExpression::Column(_) | SearchExpression::QualifiedColumn { .. } => {
                let value = self.resolve_value(ctx)?;
                self.to_boolean(&value)
            }
            SearchExpression::Comparison { left, operator, right } => {
                let left_value = left.resolve_value(ctx)?;
                let right_value = right.resolve_value(ctx)?;
                Ok(operator.compare(&left_value, &right_value))
            }
            SearchExpression::And(left, right) => {
                Ok(left.evaluate(ctx)? && right.evaluate(ctx)?)
            }
            SearchExpression::Or(left, right) => Ok(left.evaluate(ctx)? || right.evaluate(ctx)?),
            SearchExpression::Not(inner) => Ok(!inner.evaluate(ctx)?),
        }
    }

    /// If this filter is a single `column = literal` (or `literal = column`), return the
    /// column name and literal value for index lookup on a single-table scan.
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

    /// For join index lookup: `left_alias.left_col = right_alias.right_col` with the
    /// left side already bound in `ctx`, return `(right_col, lookup_value)`.
    pub fn join_equality_lookup(
        &self,
        ctx: &RowEvaluationContext,
        right_alias: &str,
    ) -> Option<(&str, DataBaseDataEntry)> {
        let right_alias = Self::normalize_identifier(right_alias);
        match self {
            SearchExpression::Comparison {
                left,
                operator: SearchOperator::Equal,
                right,
            } => {
                let (qual_left, qual_right) = match (left.as_ref(), right.as_ref()) {
                    (
                        SearchExpression::QualifiedColumn { table, column },
                        SearchExpression::QualifiedColumn {
                            table: r_table,
                            column: r_col,
                        },
                    ) => Some(((table, column), (r_table, r_col))),
                    _ => None,
                }?;
                if Self::normalize_identifier(qual_right.0) != right_alias {
                    return None;
                }
                let value = ctx.resolve(Some(qual_left.0), qual_left.1).ok()?.clone();
                Some((qual_right.1, value))
            }
            _ => None,
        }
    }
}

impl SearchRequest {
    fn normalize_identifier(identifier: &str) -> String {
        identifier.to_ascii_lowercase()
    }

    pub fn table_name(&self) -> &str {
        &self.from.base.table_name
    }

    pub fn single_table(
        table_name: String,
        projection: Option<Vec<String>>,
        filter: Option<SearchExpression>,
        order_by: Vec<SortBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        let projection = projection.map(|columns| {
            columns
                .into_iter()
                .map(|column_name| ProjectionColumn {
                    output_name: column_name.clone(),
                    table_alias: None,
                    column_name,
                })
                .collect()
        });
        Self {
            from: FromClause {
                base: TableSource {
                    table_name,
                    alias: None,
                },
                joins: Vec::new(),
            },
            projection,
            aggregate: None,
            filter,
            order_by,
            limit,
            offset,
        }
    }

    fn parse_table_name(name: &ObjectName) -> Result<String, DataBaseErrors> {
        name.0
            .last()
            .map(|ident| Self::normalize_identifier(&ident.value))
            .ok_or_else(|| DataBaseErrors::QueryError("Missing table name in FROM clause".into()))
    }

    fn parse_table_source(factor: &TableFactor) -> Result<TableSource, DataBaseErrors> {
        match factor {
            TableFactor::Table { name, alias, .. } => Ok(TableSource {
                table_name: Self::parse_table_name(name)?,
                alias: alias
                    .as_ref()
                    .map(|alias| Self::normalize_identifier(&alias.name.value)),
            }),
            _ => Err(DataBaseErrors::QueryError(
                "FROM clause must name a base table".into(),
            )),
        }
    }

    fn parse_join(join: &Join) -> Result<JoinSpec, DataBaseErrors> {
        let table = Self::parse_table_source(&join.relation)?;
        let (join_type, on, using_columns) = match &join.join_operator {
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint) => {
                let join_type = match &join.join_operator {
                    JoinOperator::Inner(_) => JoinType::Inner,
                    JoinOperator::LeftOuter(_) => JoinType::LeftOuter,
                    JoinOperator::RightOuter(_) => {
                        return Err(DataBaseErrors::QueryError(
                            "RIGHT JOIN is not supported yet; swap the table order and use LEFT JOIN"
                                .into(),
                        ));
                    }
                    JoinOperator::FullOuter(_) => {
                        return Err(DataBaseErrors::QueryError(
                            "FULL OUTER JOIN is not supported".into(),
                        ));
                    }
                    _ => unreachable!(),
                };
                let (on, using_columns) = Self::parse_join_constraint(constraint)?;
                (join_type, on, using_columns)
            }
            JoinOperator::CrossJoin => (JoinType::Cross, None, None),
            other => {
                return Err(DataBaseErrors::QueryError(format!(
                    "Unsupported JOIN operator: {other:?}",
                )));
            }
        };
        Ok(JoinSpec {
            table,
            join_type,
            on,
            using_columns,
        })
    }

    fn parse_join_constraint(
        constraint: &JoinConstraint,
    ) -> Result<(Option<SearchExpression>, Option<Vec<String>>), DataBaseErrors> {
        match constraint {
            JoinConstraint::On(expr) => Ok((Some(Self::parse_sql_expression(expr)?), None)),
            JoinConstraint::Using(columns) => Ok((
                None,
                Some(
                    columns
                        .iter()
                        .map(|ident| Self::normalize_identifier(&ident.value))
                        .collect(),
                ),
            )),
            JoinConstraint::Natural => Err(DataBaseErrors::QueryError(
                "NATURAL JOIN is not supported".into(),
            )),
            JoinConstraint::None => Ok((None, None)),
        }
    }

    fn parse_from(table_with_joins: &TableWithJoins) -> Result<FromClause, DataBaseErrors> {
        let base = Self::parse_table_source(&table_with_joins.relation)?;
        let joins = table_with_joins
            .joins
            .iter()
            .map(Self::parse_join)
            .collect::<Result<Vec<_>, DataBaseErrors>>()?;
        Ok(FromClause { base, joins })
    }

    fn parse_column_reference(expr: &SqlExpr) -> Result<(Option<String>, String), DataBaseErrors> {
        match expr {
            SqlExpr::Identifier(identifier) => Ok((
                None,
                Self::normalize_identifier(&identifier.value),
            )),
            SqlExpr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    Ok((
                        None,
                        Self::normalize_identifier(&parts[0].value),
                    ))
                } else {
                    Ok((
                        Some(Self::normalize_identifier(&parts[0].value)),
                        Self::normalize_identifier(&parts[parts.len() - 1].value),
                    ))
                }
            }
            _ => Err(DataBaseErrors::QueryError(
                "Expected a column identifier".into(),
            )),
        }
    }

    fn parse_identifier(expr: &SqlExpr) -> Result<(Option<String>, String), DataBaseErrors> {
        Self::parse_column_reference(expr)
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
            SqlExpr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    Ok(SearchExpression::Column(Self::normalize_identifier(
                        &parts[0].value,
                    )))
                } else {
                    Ok(SearchExpression::QualifiedColumn {
                        table: Self::normalize_identifier(&parts[0].value),
                        column: Self::normalize_identifier(&parts[parts.len() - 1].value),
                    })
                }
            }
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
            SqlExpr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Ok(SearchExpression::Not(Box::new(Self::parse_sql_expression(
                expr,
            )?))),
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
                let (table_alias, column_name) = Self::parse_identifier(&order_by.expr)?;
                let order_by_direction = match order_by.asc {
                    Some(false) => OrderBy::DESC,
                    _ => OrderBy::ASC,
                };
                Ok(SortBy {
                    table_alias,
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

    fn function_name(function: &Function) -> String {
        function
            .name
            .0
            .iter()
            .map(|ident| ident.value.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".")
    }

    fn is_count_star(function: &Function) -> bool {
        if Self::function_name(function) != "count" {
            return false;
        }
        match &function.args {
            FunctionArguments::List(list) if list.args.len() == 1 => {
                matches!(list.args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
            }
            _ => false,
        }
    }

    fn parse_aggregate_projection(item: &SelectItem) -> Result<Option<AggregateProjection>, DataBaseErrors> {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Function(function)) if Self::is_count_star(function) => {
                Ok(Some(AggregateProjection::CountStar {
                    output_name: "count".to_string(),
                }))
            }
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Function(function),
                alias,
            } if Self::is_count_star(function) => Ok(Some(AggregateProjection::CountStar {
                output_name: Self::normalize_identifier(&alias.value),
            })),
            _ => Ok(None),
        }
    }

    fn parse_projection_item(item: &SelectItem) -> Result<ProjectionColumn, DataBaseErrors> {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (table_alias, column_name) = Self::parse_column_reference(expr)?;
                Ok(ProjectionColumn {
                    output_name: column_name.clone(),
                    table_alias,
                    column_name,
                })
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (table_alias, column_name) = Self::parse_column_reference(expr)?;
                Ok(ProjectionColumn {
                    output_name: Self::normalize_identifier(&alias.value),
                    table_alias,
                    column_name,
                })
            }
            item => Err(DataBaseErrors::QueryError(format!(
                "Unsupported select item: {item:?}",
            ))),
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
            Statement::Query(query) => query,
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "Only SELECT queries are supported".into(),
                ))
            }
        };

        Self::from_query(&query)
    }

    pub fn from_query(query: &sqlparser::ast::Query) -> Result<Self, DataBaseErrors> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => {
                return Err(DataBaseErrors::QueryError(
                    "Only plain SELECT queries are supported".into(),
                ))
            }
        };

        if select.from.len() != 1 {
            return Err(DataBaseErrors::QueryError(
                "SELECT must have exactly one FROM entry".into(),
            ));
        }

        let from = Self::parse_from(&select.from[0])?;

        let projection = if select.projection.len() == 1 {
            if let Some(aggregate) = Self::parse_aggregate_projection(&select.projection[0])? {
                (None, Some(aggregate))
            } else if select.projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            }) {
                (None, None)
            } else {
                (
                    Some(vec![Self::parse_projection_item(&select.projection[0])?]),
                    None,
                )
            }
        } else if select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        }) {
            (None, None)
        } else {
            let mut columns = Vec::new();
            for item in &select.projection {
                columns.push(Self::parse_projection_item(item)?);
            }
            (Some(columns), None)
        };
        let (projection, aggregate) = projection;

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
            from,
            projection,
            aggregate,
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

        assert_eq!(request.table_name(), "users");
        assert_eq!(
            request.projection.as_ref().unwrap()[0].column_name,
            "id"
        );
        assert!(matches!(request.filter, Some(SearchExpression::And(_, _))));
        assert_eq!(request.order_by.len(), 1);
        assert_eq!(request.order_by[0].column_name, "name");
        assert_eq!(request.order_by[0].order_by, OrderBy::DESC);
        assert_eq!(request.limit, Some(5));
        assert_eq!(request.offset, Some(2));

        let mut ctx = RowEvaluationContext::default();
        ctx.values
            .insert("age".into(), DataBaseDataEntry::IntegerI32(25));
        ctx.values
            .insert("active".into(), DataBaseDataEntry::Boolean(true));

        let filter = request.filter.unwrap();
        assert!(filter.evaluate(&ctx).unwrap());
    }

    #[test]
    fn parse_inner_join() {
        let request = SearchRequest::from_sql(
            "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.amount > 10",
        )
        .expect("Failed to parse join query");

        assert_eq!(request.from.base.table_name, "users");
        assert_eq!(request.from.base.alias.as_deref(), Some("u"));
        assert_eq!(request.from.joins.len(), 1);
        assert_eq!(request.from.joins[0].table.table_name, "orders");
        assert_eq!(request.from.joins[0].join_type, JoinType::Inner);
        assert!(request.from.joins[0].on.is_some());
        assert_eq!(request.projection.as_ref().unwrap().len(), 2);
    }
}
