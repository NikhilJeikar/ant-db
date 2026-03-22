use std::{any::{type_name_of_val}, collections::{BTreeMap, BTreeSet, HashMap}};

use serde::{Deserialize, Serialize};

use crate::backend::{core::{row::{Row, RowID, TransactionID}, table::TableID}, errors::DataBaseErrors};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey {
        refered_table_id: TableID,
        refered_column_id: ColumnID,
    },
    Check(String),
    AutoIncrement,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DataType {
    IntegerU8,
    IntegerU16,
    IntegerU32,
    IntegerU64,
    IntegerU128,
    IntegerI8,
    IntegerI16,
    IntegerI32,
    IntegerI64,
    IntegerI128,
    FloatF32,
    FloatF64,
    String,
    Boolean,
    Bytes,
}

pub type ColumnID = u16;
pub type HashType = u64;

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    constraint: Vec<Constraint>,
    index: Option<BTreeMap<HashType, BTreeSet<RowID>>>,
    is_nullable: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, constraint: Vec<Constraint>) -> Self {
        let mut is_nullable = true;
        let mut index = None;
        if constraint.contains(&Constraint::NotNull) {
            is_nullable = false;
        }
        if constraint.contains(&Constraint::PrimaryKey)
            || constraint.contains(&Constraint::PrimaryKey)
        {
            index = Some(BTreeMap::new());
        }
        Self {
            name,
            data_type,
            constraint,
            index: index,
            is_nullable,
        }
    }

    pub fn create_index(&mut self, rows: &HashMap<RowID, Row>) -> Result<RowID, DataBaseErrors>{
        if !self.index.is_none() {
            return  Err(DataBaseErrors::IndexExist(type_name_of_val(self)));
        }
        for (row_id, row) in rows {
            for cell in row.get_raw_rows() {

            }
        }
        Ok((0))
    }

    pub fn drop_index(&mut self) {
        self.index = None;
    }

    pub fn prune(&mut self, transaction_id: TransactionID) {
        
    }
}
