use std::collections::HashMap;

use crate::backend::core::{
    column::{Column, ColumnID},
    row::{Row, RowID},
};

pub type TableID = u64;
struct Table {
    name: String,
    rows: HashMap<RowID, Row>,
    columns: HashMap<ColumnID, Column>,
}
