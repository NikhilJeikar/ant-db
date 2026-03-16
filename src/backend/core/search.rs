enum SearchOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}
struct SearchCriteria {
    column_id: u64,
    operator: SearchOperator,
    value: Vec<u8>,
}
