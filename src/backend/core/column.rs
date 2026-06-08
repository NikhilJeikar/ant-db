use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::backend::core::row::{DataBaseDataEntry, Row, RowID};
use crate::backend::core::table::TableID;
use crate::backend::core::transaction::{Transaction, TransactionHeader, TransactionID};
use crate::backend::errors::DataBaseErrors;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey {
        refered_table_id: TableID,
        refered_column_id: ColumnID,
    },
    Check,
    AutoIncrement,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DataBaseDataType {
    Null,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalColumn {
    pub transaction_header: TransactionHeader,
    pub column_id: ColumnID,
    pub name: String,
    pub data_type: DataBaseDataType,
    constraints: BTreeSet<Constraint>,
    index: Option<BTreeMap<HashType, BTreeSet<RowID>>>,
    is_nullable: bool,
}

impl InternalColumn {
    pub fn new(
        column_id: ColumnID,
        name: String,
        data_type: DataBaseDataType,
        constraints: BTreeSet<Constraint>,
        transaction: &Transaction,
    ) -> Self {
        let mut is_nullable = true;
        let mut index = None;
        if constraints.contains(&Constraint::NotNull) {
            is_nullable = false;
        }
        if constraints.contains(&Constraint::PrimaryKey) || constraints.contains(&Constraint::Unique)
        {
            index = Some(BTreeMap::new());
        }
        Self {
            transaction_header: TransactionHeader {
                created_by: transaction.transaction_id,
                deleted_by: None,
            },
            column_id,
            name,
            data_type,
            constraints,
            index: index,
            is_nullable,
        }
    }

    pub fn is_indexed(&self) -> bool {
        self.index.is_some()
    }

    pub fn index_lookup(&self, value: &DataBaseDataEntry) -> Option<&BTreeSet<RowID>> {
        self.index
            .as_ref()
            .and_then(|index| index.get(&value.key(self.column_id)))
    }

    pub fn create_index(
        &mut self,
        rows: &HashMap<RowID, Row>,
        transaction: &Transaction,
    ) -> Result<usize, DataBaseErrors> {
        if let Some(index) = &self.index {
            if !index.is_empty() {
                return Err(DataBaseErrors::IndexExist(self.name.clone()));
            }
        }

        let mut index: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
        let mut indexed = 0usize;

        for (row_id, row) in rows {
            if let Some(version) = row.get_versioned_row(transaction) {
                if let Some(value) = version.data.get(&self.column_id) {
                    index
                        .entry(value.key(self.column_id))
                        .or_default()
                        .insert(*row_id);
                    indexed += 1;
                }
            }
        }

        self.index = Some(index);

        Ok(indexed)
    }

    pub fn drop_index(&mut self) {
        self.index = None;
    }

    pub fn update_index(
        &mut self,
        column_id: ColumnID,
        value: DataBaseDataEntry,
        row_id: RowID,
    ) {
        match self.index.as_mut() {
            Some(index) => {
                index.entry(value.key(column_id)).or_default().insert(row_id);
            }
            None => {}
        }
    }

    pub fn remove_from_index(
        &mut self,
        column_id: ColumnID,
        value: &DataBaseDataEntry,
        row_id: RowID,
    ) {
        let Some(index) = self.index.as_mut() else {
            return;
        };
        let key = value.key(column_id);
        if let Some(row_ids) = index.get_mut(&key) {
            row_ids.remove(&row_id);
            if row_ids.is_empty() {
                index.remove(&key);
            }
        }
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID, rows: &HashMap<RowID, Row>) {
        let Some(index) = &mut self.index else {
            return;
        };

        let mut empty_keys = Vec::new();

        for (key, row_ids) in index.iter_mut() {
            row_ids.retain(|row_id| {
                if let Some(row) = rows.get(row_id) {
                    row.get_raw_rows().iter().any(|version| {
                        match version.transaction_header.deleted_by {
                            None => true, // still alive
                            Some(del) => del >= oldest_active_txn,
                        }
                    })
                } else {
                    false // row missing
                }
            });

            if row_ids.is_empty() {
                empty_keys.push(*key);
            }
        }

        for key in empty_keys {
            index.remove(&key);
        }
    }

    pub fn schema_validation(
        &self,
        value: &DataBaseDataEntry,
        rows: &HashMap<RowID, Row>,
        transaction: &Transaction,
        exclude_row_id: Option<RowID>,
    ) -> Result<(), DataBaseErrors> {
        for constraints in &self.constraints {
            match constraints {
                Constraint::NotNull => {
                    if value.is_null() {
                        return Err(DataBaseErrors::NullValue());
                    }
                }
                Constraint::Unique | Constraint::PrimaryKey => {
                    match &self.index {
                        Some(index) => {
                            let key = value.key(self.column_id);
                            if let Some(row_ids) = index.get(&key) {
                                for row_id in row_ids {
                                    if Some(*row_id) == exclude_row_id {
                                        continue;
                                    }
                                    match rows.get(row_id) {
                                        Some(versioned_row) => {
                                            match versioned_row.get_versioned_row(transaction) {
                                                Some(row) => {
                                                    if let Some(lookup_value) =
                                                        row.data.get(&self.column_id)
                                                    {
                                                        if lookup_value == value {
                                                            return Err(
                                                                DataBaseErrors::UniqueConstraint(
                                                                    self.name.clone(),
                                                                ),
                                                            );
                                                        }
                                                    }
                                                }
                                                None => {
                                                    return Err(DataBaseErrors::RowNotFound(
                                                        *row_id,
                                                    ));
                                                }
                                            }
                                        }
                                        None => {
                                            return Err(DataBaseErrors::RowNotFound(*row_id));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            return Err(DataBaseErrors::IndexNotFound(self.name.clone()));
                        }
                    }
                }
                Constraint::ForeignKey {
                    refered_table_id: _,
                    refered_column_id: _,
                } => {
                    //TODO: Implement it as part of the Database overhaul
                }
                Constraint::Check => {
                    //TODO: Implement it as part of the Check logic sepratly
                }
                Constraint::AutoIncrement => {
                    //NOTE: This is not validation this is a property so this won't be implented here rather create a preprocessing before schema validation and enter it through there
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Column {
    versions: Vec<InternalColumn>,
}

impl Column {
    pub fn new(
        column_id: ColumnID,
        name: String,
        data_type: DataBaseDataType,
        constraints: BTreeSet<Constraint>,
        transaction: &Transaction,
    ) -> Self {
        let mut column = Column {
            versions: Vec::new(),
        };
        column.versions.push(InternalColumn::new(
            column_id,
            name,
            data_type,
            constraints,
            transaction,
        ));
        column
    }

    pub fn remove(&mut self, transaction: &Transaction) {
        for column in self.versions.iter_mut().rev() {
            if column.transaction_header.is_visible(transaction) {
                column.transaction_header.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }
    }

    pub fn update(
        &mut self,
        name: Option<String>,
        data_type: Option<DataBaseDataType>,
        constraints: Option<BTreeSet<Constraint>>,
        transaction: &Transaction,
    ) {
        let current = match self
            .versions
            .iter()
            .rev()
            .find(|col| col.transaction_header.is_visible(transaction))
            .cloned()
        {
            Some(col) => col,
            None => return,
        };

        let new_constraints = constraints.unwrap_or_else(|| current.constraints.clone());

        let mut is_nullable = true;
        let mut index = current.index.clone();

        if new_constraints.contains(&Constraint::NotNull) {
            is_nullable = false;
        }

        if new_constraints.contains(&Constraint::PrimaryKey)
            || new_constraints.contains(&Constraint::Unique)
        {
            if index.is_none() {
                index = Some(BTreeMap::new());
            }
        } else {
            index = None;
        }

        for col in self.versions.iter_mut().rev() {
            if col.transaction_header.is_visible(transaction) {
                col.transaction_header.deleted_by = Some(transaction.transaction_id);
                break;
            }
        }

        let new_column = InternalColumn {
            transaction_header: TransactionHeader {
                created_by: transaction.transaction_id,
                deleted_by: None,
            },
            column_id: current.column_id,
            name: name.unwrap_or(current.name.clone()),
            data_type: data_type.unwrap_or(current.data_type.clone()),
            constraints: new_constraints,
            index,
            is_nullable,
        };

        self.versions.push(new_column);
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID) {
        self.versions
            .retain(|row| match row.transaction_header.deleted_by {
                None => true,
                Some(del) => del >= oldest_active_txn,
            });
    }

    pub fn rollback_transaction(&mut self, transaction: &Transaction) {
        for version in self.versions.iter_mut() {
            if version.transaction_header.created_by == transaction.transaction_id {
                if version.transaction_header.deleted_by.is_none() {
                    version.transaction_header.deleted_by = Some(transaction.transaction_id);
                }
            }

            if version.transaction_header.deleted_by == Some(transaction.transaction_id)
                && version.transaction_header.created_by != transaction.transaction_id
            {
                version.transaction_header.deleted_by = None;
            }
        }
    }

    pub fn get_raw_columns(&self) -> &Vec<InternalColumn> {
        &self.versions
    }

    pub fn get_versioned_column(&self, transaction: &Transaction) -> Option<&InternalColumn> {
        self.versions
            .iter()
            .rev()
            .find(|column| column.transaction_header.is_visible(transaction))
    }

    pub fn get_versioned_constraints(
        &self,
        transaction: &Transaction,
    ) -> Option<BTreeSet<Constraint>> {
        self.get_versioned_column(transaction)
            .map(|version| version.constraints.clone())
    }

    fn get_versioned_column_mut(
        &mut self,
        transaction: &Transaction,
    ) -> Option<&mut InternalColumn> {
        self.versions
            .iter_mut()
            .rev()
            .find(|column| column.transaction_header.is_visible(transaction))
    }

    pub fn create_index(
        &mut self,
        rows: &HashMap<RowID, Row>,
        transaction: &Transaction,
    ) -> Result<usize, DataBaseErrors> {
        let Some(version) = self.get_versioned_column_mut(transaction) else {
            return Err(DataBaseErrors::QueryError(
                "No visible column version found while building index".into(),
            ));
        };
        version.create_index(rows, transaction)
    }

    pub fn update_index(
        &mut self,
        column_id: ColumnID,
        value: DataBaseDataEntry,
        row_id: RowID,
        transaction: &Transaction,
    ) {
        if let Some(version) = self.get_versioned_column_mut(transaction) {
            version.update_index(column_id, value, row_id);
        }
    }

    pub fn remove_from_index(
        &mut self,
        column_id: ColumnID,
        value: &DataBaseDataEntry,
        row_id: RowID,
        transaction: &Transaction,
    ) {
        if let Some(version) = self.get_versioned_column_mut(transaction) {
            version.remove_from_index(column_id, value, row_id);
        }
    }

    pub fn schema_validation(
        &self,
        value: &DataBaseDataEntry,
        rows: &HashMap<RowID, Row>,
        transaction: &Transaction,
        exclude_row_id: Option<RowID>,
    ) -> Result<(), DataBaseErrors> {
        let Some(version) = self.get_versioned_column(transaction) else {
            return Ok(());
        };
        version.schema_validation(value, rows, transaction, exclude_row_id)
    }

    pub fn is_indexed(&self, transaction: &Transaction) -> bool {
        self.get_versioned_column(transaction)
            .map(|version| version.is_indexed())
            .unwrap_or(false)
    }

    pub fn index_lookup(
        &self,
        value: &DataBaseDataEntry,
        transaction: &Transaction,
    ) -> Option<Vec<RowID>> {
        self.get_versioned_column(transaction).and_then(|version| {
            version
                .index_lookup(value)
                .map(|row_ids| row_ids.iter().copied().collect())
        })
    }
}
