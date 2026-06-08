use ahash::AHashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::error;

use crate::backend::config::InternalStateManager;
use crate::backend::core::column::Constraint;
use crate::backend::core::column::{Column, ColumnID, DataBaseDataType};
use crate::backend::core::row::{DataBaseDataEntry, Row, RowID};
use crate::backend::core::search::{OrderBy, SearchRequest, SearchResult, SortBy};
use crate::backend::core::transaction::{Transaction, TransactionID};
use crate::backend::errors::DataBaseErrors;

const BUFFER_PAGE_SIZE: u64 = 1024 * 1024;

pub type TableID = u64;
pub type PageID = u64;

fn serialize_columns<S>(
    columns: &BTreeMap<ColumnID, Arc<RwLock<Column>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let export: BTreeMap<ColumnID, Column> = columns
        .iter()
        .filter_map(|(k, v)| match v.read() {
            Ok(guard) => Some((*k, guard.clone())),
            Err(e) => {
                error!(
                    "Failed to acquire read lock on column {} during serialization: {}",
                    k, e
                );
                None
            }
        })
        .collect();
    export.serialize(serializer)
}

fn deserialize_columns<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<ColumnID, Arc<RwLock<Column>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<ColumnID, Column>::deserialize(deserializer)?;
    Ok(intermediate
        .into_iter()
        .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
        .collect())
}

fn serialize_row_space<S>(
    row_space: &BTreeMap<PageID, Arc<RwLock<Page>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let export: BTreeMap<PageID, Page> = row_space
        .iter()
        .filter_map(|(k, v)| match v.read() {
            Ok(guard) => Some((*k, guard.clone())),
            Err(e) => {
                error!(
                    "Failed to acquire read lock on page {} during serialization: {}",
                    k, e
                );
                None
            }
        })
        .collect();
    export.serialize(serializer)
}

fn deserialize_row_space<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<PageID, Arc<RwLock<Page>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<PageID, Page>::deserialize(deserializer)?;
    Ok(intermediate
        .into_iter()
        .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
        .collect())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Page {
    size: u64,
    rows: BTreeMap<RowID, Row>,
}

impl Page {
    pub fn new() -> Self {
        Page {
            size: 0,
            rows: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    table_id: TableID,

    name: String,
    #[serde(
        serialize_with = "serialize_row_space",
        deserialize_with = "deserialize_row_space"
    )]
    row_space: BTreeMap<PageID, Arc<RwLock<Page>>>,

    #[serde(
        serialize_with = "serialize_columns",
        deserialize_with = "deserialize_columns"
    )]
    columns: BTreeMap<ColumnID, Arc<RwLock<Column>>>,

    next_page_id: AtomicU64,
    next_column_id: AtomicU16,

    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            table_id: self.table_id,
            name: self.name.clone(),
            row_space: self.row_space.clone(),
            columns: self.columns.clone(),
            next_page_id: AtomicU64::new(self.next_page_id.load(Ordering::SeqCst)),
            next_column_id: AtomicU16::new(self.next_column_id.load(Ordering::SeqCst)),
            internal_state_manager: self.internal_state_manager.clone(),
        }
    }
}

impl Table {
    pub fn new(
        table_id: TableID,
        name: String,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
    ) -> Self {
        let mut row_space = BTreeMap::new();
        for i in 0..BUFFER_PAGE_SIZE {
            let page = Page::new();
            row_space.insert(i, Arc::new(RwLock::new(page)));
        }
        Table {
            table_id,
            name,
            columns: BTreeMap::new(),
            row_space: row_space,
            next_page_id: AtomicU64::new(0),
            next_column_id: AtomicU16::new(0),
            internal_state_manager,
        }
    }

    pub fn inject_contexts(&mut self, internal_state_manager: Arc<RwLock<InternalStateManager>>) {
        self.internal_state_manager = internal_state_manager;
    }

    fn validate_data_type(
        data: &DataBaseDataEntry,
        expected_type: &DataBaseDataType,
        is_nullable: bool,
    ) -> bool {
        let data_type = &data.data_type();
        data_type == expected_type || (is_nullable && *data_type == DataBaseDataType::Null)
    }

    pub fn create_column(
        &mut self,
        column_name: String,
        data_type: DataBaseDataType,
        constraints: BTreeSet<Constraint>,
        transaction: &Transaction,
    ) -> Result<ColumnID, DataBaseErrors> {
        let column_id = self.next_column_id.fetch_add(1, Ordering::SeqCst);
        self.columns.insert(
            column_id,
            Arc::new(RwLock::new(Column::new(
                column_id,
                column_name,
                data_type,
                constraints,
                transaction,
            ))),
        );
        Ok(column_id)
    }

    pub fn add_column_constraints(
        &mut self,
        column_id: ColumnID,
        extra_constraints: BTreeSet<Constraint>,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let column = self.columns.get(&column_id).ok_or_else(|| {
            DataBaseErrors::QueryError("Column not found while applying table constraints".into())
        })?;
        let mut column = column.write().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire column write lock".into())
        })?;

        let mut merged_constraints = column
            .get_versioned_constraints(transaction)
            .unwrap_or_default();
        merged_constraints.extend(extra_constraints);

        column.update(None, None, Some(merged_constraints), transaction);
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        column_id: ColumnID,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        self.columns
            .get(&column_id)
            .unwrap()
            .write()
            .unwrap()
            .remove(transaction);
        Ok(())
    }

    pub fn insert_row(
        &self,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
    ) -> Result<RowID, DataBaseErrors> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst) % BUFFER_PAGE_SIZE;

        let mut page = self.row_space.get(&page_id).unwrap().write().unwrap();
        let local_row_id = page.rows.len() as u64 + 1;

        page.rows.insert(local_row_id, Row::new(transaction, data));

        Ok(local_row_id)
    }

    pub fn delete_row(
        &self,
        row_id: RowID,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let page_id = row_id % BUFFER_PAGE_SIZE;
        self.row_space
            .get(&page_id)
            .unwrap()
            .write()
            .unwrap()
            .rows
            .get_mut(&row_id)
            .unwrap()
            .remove(transaction);
        Ok(())
    }

    pub fn update_row(
        &self,
        row_id: RowID,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let page_id = row_id % BUFFER_PAGE_SIZE;
        self.row_space
            .get(&page_id)
            .unwrap()
            .write()
            .unwrap()
            .rows
            .get_mut(&row_id)
            .unwrap()
            .update(transaction, data);
        Ok(())
    }

    pub fn get_row(&self, row_id: RowID) -> Result<Row, DataBaseErrors> {
        let page_id = row_id % BUFFER_PAGE_SIZE;
        match self
            .row_space
            .get(&page_id)
            .unwrap()
            .read()
            .unwrap()
            .rows
            .get(&row_id)
        {
            Some(row) => Ok(row.clone()),
            None => Err(DataBaseErrors::RowNotFound(row_id)),
        }
    }

    pub fn get_size(&self) -> usize {
        let mut size = 0;
        for page in self.row_space.values() {
            size += page.read().unwrap().rows.len();
        }
        size as usize
    }

    fn normalize_column_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    pub fn get_visible_column_names(&self, transaction: &Transaction) -> Vec<String> {
        let mut names: Vec<String> = self
            .columns
            .values()
            .filter_map(|column| {
                column
                    .read()
                    .ok()
                    .and_then(|guard| guard.get_versioned_column(transaction).map(|col| col.name.clone()))
            })
            .collect();
        names.sort();
        names
    }

    pub fn get_visible_column_map(
        &self,
        transaction: &Transaction,
    ) -> Result<HashMap<String, ColumnID>, DataBaseErrors> {
        let mut visible_columns: HashMap<String, ColumnID> = HashMap::new();
        for (&column_id, column) in self.columns.iter() {
            let guard = column
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
            if let Some(version) = guard.get_versioned_column(transaction) {
                visible_columns.insert(version.name.to_ascii_lowercase(), column_id);
            }
        }
        Ok(visible_columns)
    }

    pub fn search(
        &self,
        request: &SearchRequest,
        transaction: &Transaction,
    ) -> Result<Vec<SearchResult>, DataBaseErrors> {
        let visible_columns: HashMap<String, ColumnID> = self
            .columns
            .iter()
            .filter_map(|(&column_id, column)| {
                column.read().ok().and_then(|guard| {
                    guard
                        .get_versioned_column(transaction)
                        .map(|version| (version.name.to_ascii_lowercase(), column_id))
                })
            })
            .collect();

        let projection_columns: Vec<(String, ColumnID)> = match &request.projection {
            Some(projection) => projection
                .iter()
                .map(|column_name| {
                    visible_columns
                        .get(column_name)
                        .cloned()
                        .map(|id| (column_name.clone(), id))
                        .ok_or_else(|| {
                            DataBaseErrors::QueryError(format!(
                                "Projection column '{column_name}' not found",
                            ))
                        })
                })
                .collect::<Result<Vec<_>, DataBaseErrors>>()?,
            None => {
                let mut columns: Vec<_> = visible_columns
                    .iter()
                    .map(|(name, id)| (name.clone(), *id))
                    .collect();
                columns.sort_by(|(a, _), (b, _)| a.cmp(b));
                columns
            }
        };

        let sort_columns: Vec<(ColumnID, &SortBy)> = request
            .order_by
            .iter()
            .map(|sort_by| {
                let column_id = visible_columns.get(&sort_by.column_name).cloned().ok_or_else(|| {
                    DataBaseErrors::QueryError(format!(
                        "ORDER BY column '{}' not found",
                        sort_by.column_name
                    ))
                })?;
                Ok((column_id, sort_by))
            })
            .collect::<Result<Vec<_>, DataBaseErrors>>()?;

        let mut rows: Vec<(RowID, AHashMap<ColumnID, DataBaseDataEntry>)> = Vec::new();

        for page in self.row_space.values() {
            let page_guard = page.read().unwrap();
            for (row_id, row) in page_guard.rows.iter() {
                if let Some(version) = row.get_versioned_row(transaction) {
                    let matches = match &request.filter {
                        Some(filter) => filter.evaluate(&version.data, &visible_columns)?,
                        None => true,
                    };
                    if matches {
                        rows.push((*row_id, version.data.clone()));
                    }
                }
            }
        }

        if !sort_columns.is_empty() {
            rows.sort_by(|a, b| {
                for (column_id, sort_by) in sort_columns.iter() {
                    let left = a.1.get(column_id).unwrap_or(&DataBaseDataEntry::Null);
                    let right = b.1.get(column_id).unwrap_or(&DataBaseDataEntry::Null);
                    let order = left.cmp(right);
                    if order != std::cmp::Ordering::Equal {
                        return match sort_by.order_by {
                            OrderBy::ASC => order,
                            OrderBy::DESC => order.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let offset = request.offset.unwrap_or(0);
        let results = rows
            .into_iter()
            .skip(offset)
            .take(request.limit.unwrap_or(usize::MAX))
            .map(|(row_id, row_data)| {
                let mut values = AHashMap::new();
                for (column_name, column_id) in &projection_columns {
                    let value = row_data
                        .get(column_id)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    values.insert(column_name.clone(), value);
                }
                SearchResult { row_id, values }
            })
            .collect();

        Ok(results)
    }

    pub fn prune(&mut self, oldest_active_txn: TransactionID) {
        for column in self.columns.values() {
            column.write().unwrap().prune(oldest_active_txn);
        }
        for page in self.row_space.values() {
            page.write().unwrap().rows.retain(|_, row| {
                row.prune(oldest_active_txn);
                true
            });
        }
    }

    pub fn rollback_transaction(&mut self, transaction: &Transaction) {
        for column in self.columns.values() {
            column.write().unwrap().rollback_transaction(transaction);
        }
        for page in self.row_space.values() {
            for row in page.write().unwrap().rows.values_mut() {
                row.rollback_transaction(transaction);
            }
        }
    }

    pub fn replay(self) {
        
    }
}
