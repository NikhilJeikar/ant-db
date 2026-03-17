use serde::{Deserialize, Serialize};

use crate::backend::schema::DecodedData;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SearchOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OrderBy {
    ASC,
    DESC,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchCriteria {
    pub column_id: u64,
    pub operator: SearchOperator,
    pub value: DecodedData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortBy {
    pub column_id: u64,
    pub order_by: OrderBy,
}

pub type Projection = Vec<u64>;
