use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::backend::config::InternalStateManager;
use crate::backend::core::search::{OrderBy, Projection, SearchCriteria, SearchOperator, SortBy};
use crate::backend::core::types::{
    ColumnId, ColumnSchema, Constraint, DataType, DecodedData, Index, InternalTableSchema, Row,
    RowId, TableId, TableSchema,
};
use crate::backend::errors::DataBaseErrors;
use crate::backend::storage::wal::{DataBaseOperation, WriteAheadLogBase, WriteAheadLogManager};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{error, info};

pub trait TableManager {
    fn create_column(
        &mut self,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<ColumnId, DataBaseErrors>;
    fn drop_column(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors>;

    fn create_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors>;
    fn drop_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors>;

    fn pre_insert_rows(
        &self,
        rows: &[Row],
        start_row_id: RowId,
    ) -> Result<Vec<(RowId, Row)>, DataBaseErrors>;
    fn insert_rows(&mut self, rows: Vec<(RowId, Row)>) -> Result<(), DataBaseErrors>;
    fn pre_delete_rows(&self, row_ids: &Vec<RowId>) -> Result<(), DataBaseErrors>;
    fn delete_rows(&mut self, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors>;
    fn pre_update_rows(&self, row_ids: &Vec<RowId>, row: &Row) -> Result<(), DataBaseErrors>;
    fn update_rows(&mut self, row_ids: Vec<RowId>, row: Row) -> Result<(), DataBaseErrors>;

    fn get_rows(&self, row_ids: &Vec<RowId>) -> Result<Vec<Row>, DataBaseErrors>;
    fn find_row(
        &self,
        search_criteria: Vec<SearchCriteria>,
        projection: Option<Projection>,
        sort_by: Option<SortBy>,
    ) -> Result<Vec<Row>, DataBaseErrors>;

    fn get_size(&self) -> usize;
    fn get_schema(&self) -> Result<TableSchema, DataBaseErrors>;
}
pub trait TableWriteAheadLog: WriteAheadLogBase {
    // replay operations for WAL recovery
    fn wal_create_column(
        &mut self,
        column_id: ColumnId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: Option<Index>,
    ) -> Result<(), DataBaseErrors>;
    fn wal_drop_column(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors>;
    fn wal_create_index(&mut self, column_id: ColumnId, index: Index)
    -> Result<(), DataBaseErrors>;
    fn wal_drop_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors>;

    fn wal_insert_rows(&mut self, rows: Vec<(RowId, Row)>) -> Result<(), DataBaseErrors>;
    fn wal_delete_rows(&mut self, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors>;
    fn wal_update_rows(&mut self, row_ids: Vec<RowId>, row: Row) -> Result<(), DataBaseErrors>;
}

impl InternalTableSchema {
    pub fn new(
        table_id: TableId,
        name: String,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) -> Self {
        InternalTableSchema {
            table_id,
            name,
            columns: BTreeMap::new(),
            rows: BTreeMap::new(),
            next_row_id: 0,
            next_column_id: 0,
            internal_state_manager,
            wal_manager,
        }
    }

    pub fn inject_contexts(
        &mut self,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
        wal_manager: Arc<Mutex<WriteAheadLogManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        self.wal_manager = wal_manager;
    }

    fn duplicate_cells(cells: &Row) -> Option<ColumnId> {
        let mut present_columns: HashSet<ColumnId> = HashSet::new();
        for cell in cells {
            if present_columns.contains(&cell.column_id) {
                return Some(cell.column_id);
            }
            present_columns.insert(cell.column_id);
        }
        return None;
    }

    fn validate_data_type(data: &DecodedData, expected_type: &DataType) -> bool {
        &data.data_type() == expected_type
    }

    fn should_create_index(constraints: &Vec<Constraint>) -> bool {
        constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique | Constraint::PrimaryKey))
    }

    fn internal_insert_column(&mut self, column_id: ColumnId, column_schema: ColumnSchema) {
        self.columns.insert(column_id, column_schema);
    }

    fn internal_drop_column(&mut self, column_id: ColumnId) {
        self.columns.remove(&column_id);

        for row in self.rows.values_mut() {
            if let Some(pos) = row.iter().position(|c| c.column_id == column_id) {
                row.remove(pos);
            }
        }
    }

    fn build_index_for_column(&self, column_id: ColumnId) -> Result<Index, DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        let mut new_index = Index::new();

        for (row_id, cells) in self.rows.iter() {
            if let Some(cell) = cells.iter().find(|c| c.column_id == column_id) {
                let key = cell.get_key();
                new_index.entry(key).or_default().insert(*row_id);
            }
        }

        info!(
            "Built index for column {} with {} entries",
            column_id,
            new_index.len()
        );
        Ok(new_index)
    }

    fn apply_index_to_column(
        &mut self,
        column_id: ColumnId,
        index: Index,
    ) -> Result<(), DataBaseErrors> {
        let column = self
            .columns
            .get_mut(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;

        if column.index.is_some() {
            info!("Index already exists for column {}", column_id);
            return Ok(());
        }

        column.index = Some(index);
        info!("Applied index to column {}", column_id);
        Ok(())
    }

    fn add_row_to_indexes(&mut self, row_id: RowId, row: &Row) -> Result<(), DataBaseErrors> {
        for cell in row {
            let column = self
                .columns
                .get_mut(&cell.column_id)
                .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;

            if let Some(index) = column.index.as_mut() {
                index.entry(cell.get_key()).or_default().insert(row_id);
            }
        }

        Ok(())
    }

    fn remove_row_from_indexes(&mut self, row_id: RowId, row: &Row) -> Result<(), DataBaseErrors> {
        for cell in row {
            let column = self
                .columns
                .get_mut(&cell.column_id)
                .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;

            if let Some(index) = column.index.as_mut() {
                let key = cell.get_key();
                if let Entry::Occupied(mut entry) = index.entry(key) {
                    let ids = entry.get_mut();
                    ids.remove(&row_id);
                    if ids.is_empty() {
                        entry.remove();
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_row_update(&mut self, row_id: RowId, row: &Row) -> Result<(), DataBaseErrors> {
        let existing_row = self
            .rows
            .remove(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?;

        if let Err(err) = self.remove_row_from_indexes(row_id, &existing_row) {
            let _ = self.add_row_to_indexes(row_id, &existing_row);
            self.rows.insert(row_id, existing_row);
            return Err(err);
        }

        if let Err(err) = self.add_row_to_indexes(row_id, row) {
            let _ = self.remove_row_from_indexes(row_id, row);
            let _ = self.add_row_to_indexes(row_id, &existing_row);
            self.rows.insert(row_id, existing_row);
            return Err(err);
        }

        self.rows.insert(row_id, row.clone());

        Ok(())
    }

    fn indexed_row_candidates(
        &self,
        search_criterias: &[SearchCriteria],
    ) -> Option<BTreeSet<RowId>> {
        let mut candidates: Option<BTreeSet<RowId>> = None;

        for criteria in search_criterias {
            if !matches!(criteria.operator, SearchOperator::Equal) {
                continue;
            }

            let Some(column) = self.columns.get(&criteria.column_id) else {
                continue;
            };
            let Some(index) = column.index.as_ref() else {
                continue;
            };

            let bucket = index
                .get(&criteria.value.hash_key())
                .cloned()
                .unwrap_or_default();

            match candidates.as_mut() {
                Some(existing) => {
                    existing.retain(|row_id| bucket.contains(row_id));
                    if existing.is_empty() {
                        break;
                    }
                }
                None => candidates = Some(bucket),
            }
        }

        candidates
    }

    fn row_matches_criteria(row: &Row, search_criterias: &[SearchCriteria]) -> bool {
        search_criterias.iter().all(|criteria| {
            row.iter()
                .find(|cell| cell.column_id == criteria.column_id)
                .map(|cell| match criteria.operator {
                    SearchOperator::Equal => cell.data == criteria.value,
                    SearchOperator::NotEqual => cell.data != criteria.value,
                    SearchOperator::GreaterThan => cell.data > criteria.value,
                    SearchOperator::LessThan => cell.data < criteria.value,
                    SearchOperator::GreaterThanOrEqual => cell.data >= criteria.value,
                    SearchOperator::LessThanOrEqual => cell.data <= criteria.value,
                })
                .unwrap_or(false)
        })
    }
}

impl WriteAheadLogBase for InternalTableSchema {
    fn log_operation(&mut self, operation: DataBaseOperation) -> Result<(), DataBaseErrors> {
        let mut wal = self.wal_manager.lock().map_err(|e| {
            error!("Failed to acquire WAL lock: {}", e);
            DataBaseErrors::WalLockError
        })?;

        let is_replaying = self
            .internal_state_manager
            .read()
            .map(|guard| guard.is_wal_replaying)
            .unwrap_or(false);

        if !is_replaying {
            wal.append(&operation);
        }
        Ok(())
    }
}

impl TableManager for InternalTableSchema {
    fn create_column(
        &mut self,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
    ) -> Result<ColumnId, DataBaseErrors> {
        if self.columns.iter().any(|c| c.1.name == column_name) {
            return Err(DataBaseErrors::ColumnAlreadyExists(column_name));
        }

        let column_id = self.next_column_id;
        self.next_column_id += 1;

        // Only create index if column has Unique or PrimaryKey constraint
        let index = if Self::should_create_index(&constraints) {
            info!("Creating index for column {} with constraints", column_id);
            Some(Index::new())
        } else {
            info!(
                "Column {} created without index (no Unique/PrimaryKey constraint)",
                column_id
            );
            None
        };

        self.log_operation(DataBaseOperation::CreateColumn {
            table_id: self.table_id,
            column_id,
            name: column_name.clone(),
            data_type: data_type.clone(),
            constraints: constraints.clone(),
            index: index.clone(),
        })?;

        self.internal_insert_column(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
                index,
            },
        );
        Ok(column_id)
    }

    fn drop_column(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        self.log_operation(DataBaseOperation::DropColumn {
            table_id: self.table_id,
            column_id,
        })?;

        self.internal_drop_column(column_id);

        Ok(())
    }

    fn create_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        // Validate column exists
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        // Check if index already exists
        let column = self
            .columns
            .get(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;

        if column.index.is_some() {
            info!("Index already exists for column {}", column_id);
            return Ok(());
        }

        // Build the index WITHOUT modifying database state
        let index = self.build_index_for_column(column_id)?;

        // Log the operation to WAL FIRST
        self.log_operation(DataBaseOperation::CreateIndex {
            table_id: self.table_id,
            column_id,
            index: index.clone(),
        })?;

        // Apply the index to the column AFTER logging
        self.apply_index_to_column(column_id, index)?;

        Ok(())
    }

    fn drop_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        // Validate column exists
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        // Check if index exists
        let column = self
            .columns
            .get(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;

        if column.index.is_none() {
            return Err(DataBaseErrors::IndexNotFound(column_id));
        }

        // Log the operation to WAL BEFORE modifying state
        self.log_operation(DataBaseOperation::DropIndex {
            table_id: self.table_id,
            column_id,
        })?;

        // Remove the index after logging
        let column = self
            .columns
            .get_mut(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;
        column.index = None;
        info!("Index dropped for column {}", column_id);

        Ok(())
    }

    fn pre_insert_rows(
        &self,
        rows: &[Row],
        mut start_row_id: RowId,
    ) -> Result<Vec<(RowId, Row)>, DataBaseErrors> {
        let mut processed_rows = Vec::with_capacity(rows.len());

        info!("Pre-insert rows is called");
        for row in rows {
            if let Some(dup_col) = Self::duplicate_cells(row) {
                return Err(DataBaseErrors::RowColumnDuplicate(row.clone(), dup_col));
            }

            for cell in row {
                let column = self
                    .columns
                    .get(&cell.column_id)
                    .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;

                if !Self::validate_data_type(&cell.data, &column.data_type) {
                    return Err(DataBaseErrors::DataTypeMismatch(
                        cell.column_id,
                        column.data_type.type_name(),
                        cell.data.type_name(),
                    ));
                }
            }

            processed_rows.push((start_row_id, row.clone()));
            start_row_id += 1;
        }

        Ok(processed_rows)
    }

    fn insert_rows(&mut self, rows: Vec<(RowId, Row)>) -> Result<(), DataBaseErrors> {
        // Step 1: Accumulate row_ids per column/data
        info!("Insert row is called");
        let mut index_updates: BTreeMap<ColumnId, Index> = BTreeMap::new();

        for (row_id, row) in &rows {
            for cell in row {
                index_updates
                    .entry(cell.column_id)
                    .or_insert_with(BTreeMap::new)
                    .entry(cell.get_key())
                    .or_default()
                    .insert(*row_id);
            }
        }

        // Step 2: Log operation to WAL before applying changes
        info!("Logging operation for {} rows", rows.len());
        self.log_operation(DataBaseOperation::InsertRow {
            table_id: self.table_id,
            rows: rows.clone(),
        })?;

        // Step 3: Apply accumulated updates to column indexes
        for (col_id, data_map) in index_updates {
            if let Some(column) = self.columns.get_mut(&col_id) {
                if let Some(index) = column.index.as_mut() {
                    for (data, ids) in data_map {
                        index.entry(data).or_default().extend(ids);
                    }
                }
            }
        }

        // Step 4: Insert all rows into self.rows
        self.rows.extend(rows);
        Ok(())
    }

    fn pre_delete_rows(&self, row_ids: &Vec<RowId>) -> Result<(), DataBaseErrors> {
        for row_id in row_ids {
            if !self.rows.contains_key(row_id) {
                return Err(DataBaseErrors::RowNotFound(*row_id));
            }
        }
        Ok(())
    }

    fn delete_rows(&mut self, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors> {
        info!("Delete rows is called for {} rows", row_ids.len());
        self.log_operation(DataBaseOperation::DeleteRow {
            table_id: self.table_id,
            row_ids: row_ids.clone(),
        })?;

        for row_id in &row_ids {
            let row = self
                .rows
                .remove(&row_id)
                .ok_or(DataBaseErrors::RowNotFound(*row_id))?;

            self.remove_row_from_indexes(*row_id, &row)?;
        }
        Ok(())
    }

    fn pre_update_rows(&self, row_ids: &Vec<RowId>, row: &Row) -> Result<(), DataBaseErrors> {
        if let Some(dup_col) = Self::duplicate_cells(row) {
            return Err(DataBaseErrors::RowColumnDuplicate(row.clone(), dup_col));
        }

        for row_id in row_ids {
            if !self.rows.contains_key(row_id) {
                return Err(DataBaseErrors::RowNotFound(*row_id));
            }
        }

        for cell in row {
            let column = self
                .columns
                .get(&cell.column_id)
                .ok_or(DataBaseErrors::ColumnNotFound(cell.column_id))?;

            if !Self::validate_data_type(&cell.data, &column.data_type) {
                return Err(DataBaseErrors::DataTypeMismatch(
                    cell.column_id,
                    column.data_type.type_name(),
                    cell.data.type_name(),
                ));
            }
        }

        Ok(())
    }

    fn update_rows(&mut self, row_ids: Vec<RowId>, row: Row) -> Result<(), DataBaseErrors> {
        info!("Update rows is called for {} rows", row_ids.len());
        info!("Logging update operation for {} rows", row_ids.len());
        self.log_operation(DataBaseOperation::UpdateRows {
            table_id: self.table_id,
            row_ids: row_ids.clone(),
            row: row.clone(),
        })?;

        for row_id in row_ids {
            self.apply_row_update(row_id, &row)?;
        }

        Ok(())
    }

    fn find_row(
        &self,
        search_criterias: Vec<SearchCriteria>,
        projection: Option<Projection>,
        sort_by: Option<SortBy>,
    ) -> Result<Vec<Row>, DataBaseErrors> {
        let mut matched_rows: Vec<Row> = Vec::new();

        if let Some(candidate_row_ids) = self.indexed_row_candidates(&search_criterias) {
            for row_id in candidate_row_ids {
                if let Some(row) = self.rows.get(&row_id) {
                    if Self::row_matches_criteria(row, &search_criterias) {
                        matched_rows.push(row.clone());
                    }
                }
            }
        } else {
            for row in self.rows.values() {
                if Self::row_matches_criteria(row, &search_criterias) {
                    matched_rows.push(row.clone());
                }
            }
        }

        // Phase 2: Apply sorting (on CellSchema/Vec<u8> before decoding)
        if let Some(sort) = sort_by {
            matched_rows.sort_by(|row_a, row_b| {
                let cell_a = row_a.iter().find(|cell| cell.column_id == sort.column_id);
                let cell_b = row_b.iter().find(|cell| cell.column_id == sort.column_id);

                match (cell_a, cell_b) {
                    (Some(a), Some(b)) => {
                        let cmp = a.data.cmp(&b.data);
                        match sort.order_by {
                            OrderBy::ASC => cmp,
                            OrderBy::DESC => cmp.reverse(),
                        }
                    }
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }

        // Phase 3: Apply projection (select specific columns) BEFORE decoding
        if let Some(ref proj) = projection {
            matched_rows = matched_rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .filter(|cell| proj.contains(&cell.column_id))
                        .collect::<Vec<_>>()
                })
                .collect();
        }

        Ok(matched_rows)
    }

    fn get_rows(&self, row_ids: &Vec<RowId>) -> Result<Vec<Row>, DataBaseErrors> {
        let mut filtered_rows = Vec::new();
        for row_id in row_ids {
            let value = match self.rows.get(&row_id) {
                Some(value) => value.clone(),
                None => return Err(DataBaseErrors::RowNotFound(*row_id)),
            };
            filtered_rows.push(value);
        }
        Ok(filtered_rows)
    }

    fn get_size(&self) -> usize {
        self.rows.len()
    }
    fn get_schema(&self) -> Result<TableSchema, DataBaseErrors> {
        let columns = self.columns.values().cloned().collect();
        Ok(TableSchema {
            name: self.name.clone(),
            columns,
        })
    }
}

impl TableWriteAheadLog for InternalTableSchema {
    fn wal_create_column(
        &mut self,
        column_id: ColumnId,
        column_name: String,
        data_type: DataType,
        constraints: Vec<Constraint>,
        index: Option<Index>,
    ) -> Result<(), DataBaseErrors> {
        if self.columns.iter().any(|c| c.1.name == column_name) {
            return Err(DataBaseErrors::ColumnAlreadyExists(column_name));
        }

        // During WAL replay, also apply the constraint-based index logic for consistency
        let final_index = if Self::should_create_index(&constraints) {
            // If constraints require an index, use the provided index or create empty one
            if let Some(existing_index) = index {
                info!(
                    "WAL replay: Restoring index for column {} with constraints",
                    column_id
                );
                Some(existing_index)
            } else {
                info!(
                    "WAL replay: Creating empty index for column {} with constraints",
                    column_id
                );
                Some(Index::new())
            }
        } else {
            // If constraints don't require an index, discard any index from WAL
            info!(
                "WAL replay: Column {} created without index (no Unique/PrimaryKey constraint)",
                column_id
            );
            None
        };

        self.internal_insert_column(
            column_id,
            ColumnSchema {
                name: column_name,
                data_type,
                constraints,
                index: final_index,
            },
        );
        Ok(())
    }

    fn wal_drop_column(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        if !self.columns.contains_key(&column_id) {
            return Err(DataBaseErrors::ColumnNotFound(column_id));
        }

        self.internal_drop_column(column_id);

        Ok(())
    }

    fn wal_create_index(
        &mut self,
        column_id: ColumnId,
        index: Index,
    ) -> Result<(), DataBaseErrors> {
        let column = self
            .columns
            .get_mut(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;

        if column.index.is_some() {
            info!("WAL replay: Index already exists for column {}", column_id);
            return Ok(());
        }

        column.index = Some(index);
        info!("WAL replay: Index restored for column {}", column_id);
        Ok(())
    }

    fn wal_drop_index(&mut self, column_id: ColumnId) -> Result<(), DataBaseErrors> {
        let column = self
            .columns
            .get_mut(&column_id)
            .ok_or(DataBaseErrors::ColumnNotFound(column_id))?;

        if column.index.is_none() {
            info!("WAL replay: Index does not exist for column {}", column_id);
            return Ok(());
        }

        column.index = None;
        info!("WAL replay: Index dropped for column {}", column_id);
        Ok(())
    }

    fn wal_insert_rows(&mut self, rows: Vec<(RowId, Row)>) -> Result<(), DataBaseErrors> {
        for (row_id, _) in &rows {
            if *row_id >= self.next_row_id {
                self.next_row_id = *row_id + 1;
            }
        }
        self.insert_rows(rows)
    }

    fn wal_delete_rows(&mut self, row_ids: Vec<RowId>) -> Result<(), DataBaseErrors> {
        self.delete_rows(row_ids)
    }

    fn wal_update_rows(&mut self, row_ids: Vec<RowId>, row: Row) -> Result<(), DataBaseErrors> {
        for row_id in row_ids {
            self.apply_row_update(row_id, &row)?;
        }

        Ok(())
    }
}
