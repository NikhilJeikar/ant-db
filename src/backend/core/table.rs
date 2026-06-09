use ahash::AHashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::error;

use crate::backend::config::{Config, InternalStateManager};
use crate::backend::core::column::Constraint;
use crate::backend::core::column::{Column, ColumnID, DataBaseDataType};
use crate::backend::core::page_store::PageStore;
use crate::backend::core::row::{DataBaseDataEntry, Row, RowData, RowID};
use crate::backend::core::plan::physical::AccessPath;
use crate::backend::core::search::{
    OrderBy, RowEvaluationContext, SearchRequest, SearchResult, SortBy,
};
use crate::backend::core::transaction::{Transaction, TransactionID};
use crate::backend::errors::DataBaseErrors;

pub type TableID = u64;
pub type PageID = u64;

/// Estimate the on-page footprint of a row in bytes. Used to decide which page a
/// row should be appended to and whether it needs an overflow chain.
fn row_size_bytes(row: &Row) -> u64 {
    bincode::serialized_size(row).unwrap_or(0)
}

fn serialize_columns<S>(
    columns: &BTreeMap<ColumnID, Arc<RwLock<Column>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(columns.len()))?;
    for (column_id, column) in columns.iter() {
        match column.read() {
            Ok(guard) => {
                map.serialize_entry(column_id, &*guard)?;
            }
            Err(e) => {
                error!(
                    "Failed to acquire read lock on column {} during serialization: {}",
                    column_id, e
                );
            }
        }
    }
    map.end()
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
    _row_space: &RwLock<BTreeMap<PageID, Arc<RwLock<Page>>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Page data is flushed to the table page file before snapshot save.
    BTreeMap::<PageID, Page>::new().serialize(serializer)
}

fn deserialize_row_space<'de, D>(
    deserializer: D,
) -> Result<RwLock<BTreeMap<PageID, Arc<RwLock<Page>>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<PageID, Page>::deserialize(deserializer)?;
    Ok(RwLock::new(
        intermediate
            .into_iter()
            .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
            .collect(),
    ))
}

fn serialize_row_locations<S>(
    _row_locations: &RwLock<BTreeMap<RowID, PageID>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Rebuilt from the table page file on load.
    BTreeMap::<RowID, PageID>::new().serialize(serializer)
}

fn deserialize_row_locations<'de, D>(
    deserializer: D,
) -> Result<RwLock<BTreeMap<RowID, PageID>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(RwLock::new(BTreeMap::<RowID, PageID>::deserialize(
        deserializer,
    )?))
}

/// A page is a size-bounded, append-only container of rows.
///
/// Rows are appended in insertion order and a page only accepts new rows while
/// the accumulated `used_bytes` stays within `capacity`. We never insert into
/// the middle of a page: deletions leave holes that are reclaimed later by
/// compaction (see `Table::compact`).
///
/// A single row larger than `capacity` cannot fit a normal page, so it is given
/// its own overflow chain: a head page holding the row followed by `next_page`
/// links to continuation pages that reserve the remaining page-sized slots the
/// oversized entry spans. Continuation pages are flagged with `is_overflow` and
/// are skipped when looking for a page to append to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Page {
    page_id: PageID,
    capacity: u64,
    used_bytes: u64,
    rows: BTreeMap<RowID, Row>,
    next_page: Option<PageID>,
    is_overflow: bool,
    is_dirty: bool,
}

impl Page {
    pub(crate) fn id(&self) -> PageID {
        self.page_id
    }

    fn new(page_id: PageID, capacity: u64) -> Self {
        Page {
            page_id,
            capacity,
            used_bytes: 0,
            rows: BTreeMap::new(),
            next_page: None,
            is_overflow: false,
            is_dirty: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(page_id: PageID, capacity: u64) -> Self {
        Self::new(page_id, capacity)
    }

    #[cfg(test)]
    pub(crate) fn has_rows(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Whether a row of `size` bytes can still be appended to this page.
    fn has_room_for(&self, size: u64) -> bool {
        !self.is_overflow && self.used_bytes.saturating_add(size) <= self.capacity
    }

    fn append(&mut self, row_id: RowID, row: Row, size: u64) {
        self.used_bytes = self.used_bytes.saturating_add(size);
        self.rows.insert(row_id, row);
        self.is_dirty = true;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    table_id: TableID,
    database_name: String,
    name: String,
    #[serde(
        serialize_with = "serialize_row_space",
        deserialize_with = "deserialize_row_space"
    )]
    row_space: RwLock<BTreeMap<PageID, Arc<RwLock<Page>>>>,

    /// Maps every row to the page that currently stores it. Rebuilt on every
    /// compaction and kept in sync with `row_space` on insert.
    #[serde(
        serialize_with = "serialize_row_locations",
        deserialize_with = "deserialize_row_locations"
    )]
    row_locations: RwLock<BTreeMap<RowID, PageID>>,

    #[serde(
        serialize_with = "serialize_columns",
        deserialize_with = "deserialize_columns"
    )]
    columns: BTreeMap<ColumnID, Arc<RwLock<Column>>>,

    /// Maps user-created index names (normalized) to the indexed column.
    #[serde(default)]
    index_names: BTreeMap<String, ColumnID>,

    next_page_id: AtomicU64,
    next_row_id: AtomicU64,
    next_column_id: AtomicU16,

    /// Per-page byte capacity for this table, taken from the configuration when
    /// the table is created.
    page_size: u64,

    /// Page IDs that currently live on disk rather than in `row_space`.
    #[serde(skip)]
    evicted_pages: RwLock<BTreeSet<PageID>>,

    /// Page IDs known to be present in the on-disk table file.
    #[serde(skip)]
    on_disk_pages: RwLock<BTreeSet<PageID>>,

    /// Tracks last-access ordering for in-memory pages.
    #[serde(skip)]
    lru: RwLock<BTreeMap<PageID, u64>>,

    #[serde(skip)]
    lru_counter: AtomicU64,

    #[serde(skip)]
    memory_used: AtomicU64,

    #[serde(skip)]
    page_store: PageStore,

    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        let row_space = self
            .row_space
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let row_locations = self
            .row_locations
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let evicted_pages = self
            .evicted_pages
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let on_disk_pages = self
            .on_disk_pages
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let lru = self.lru.read().map(|guard| guard.clone()).unwrap_or_default();
        Self {
            table_id: self.table_id,
            database_name: self.database_name.clone(),
            name: self.name.clone(),
            row_space: RwLock::new(row_space),
            row_locations: RwLock::new(row_locations),
            columns: self.columns.clone(),
            index_names: self.index_names.clone(),
            next_page_id: AtomicU64::new(self.next_page_id.load(Ordering::SeqCst)),
            next_row_id: AtomicU64::new(self.next_row_id.load(Ordering::SeqCst)),
            next_column_id: AtomicU16::new(self.next_column_id.load(Ordering::SeqCst)),
            page_size: self.page_size,
            evicted_pages: RwLock::new(evicted_pages),
            on_disk_pages: RwLock::new(on_disk_pages),
            lru: RwLock::new(lru),
            lru_counter: AtomicU64::new(self.lru_counter.load(Ordering::SeqCst)),
            memory_used: AtomicU64::new(self.memory_used.load(Ordering::SeqCst)),
            page_store: PageStore::new(self.page_store.path().to_path_buf()),
            internal_state_manager: self.internal_state_manager.clone(),
        }
    }
}

impl Table {
    fn table_data_file_path(
        config: &Config,
        database_name: &str,
        table_name: &str,
    ) -> PathBuf {
        PathBuf::from(&config.table_data_path)
            .join(format!("{database_name}-{table_name}"))
    }

    fn refresh_page_store(&mut self) {
        if let Ok(state) = self.internal_state_manager.read() {
            self.page_store = PageStore::new(Self::table_data_file_path(
                &state.config,
                &self.database_name,
                &self.name,
            ));
        }
    }

    #[cfg(test)]
    pub fn row_location_count(&self) -> usize {
        self.row_locations
            .read()
            .map(|locations| locations.len())
            .unwrap_or(0)
    }

    pub fn bind_page_store(
        &mut self,
        database_name: &str,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
    ) {
        self.internal_state_manager = internal_state_manager;
        if self.database_name.is_empty() {
            self.database_name = database_name.to_string();
        }
        self.refresh_page_store();
        if let Err(err) = self.restore_page_index_from_disk() {
            error!(
                "Failed to restore page index for table '{}': {err}",
                self.name
            );
        }
    }

    fn sync_on_disk_pages_from_store(&self) -> Result<(), DataBaseErrors> {
        let page_ids = self.page_store.list_page_ids()?;
        if let Ok(mut on_disk) = self.on_disk_pages.write() {
            *on_disk = page_ids.into_iter().collect();
        }
        Ok(())
    }

    /// Reconcile in-memory page state with the on-disk table page file.
    ///
    /// When a page file exists it is treated as the source of truth: row locations
    /// are rebuilt by scanning every page on disk and in-memory pages are cleared
    /// so they are loaded on demand. Legacy snapshots that still embed row data
    /// keep their deserialized in-memory state when no page file is present.
    fn restore_page_index_from_disk(&self) -> Result<(), DataBaseErrors> {
        self.sync_on_disk_pages_from_store()?;
        let page_ids = self.page_store.list_page_ids()?;
        if page_ids.is_empty() {
            self.rebuild_runtime_state();
            return Ok(());
        }

        let mut row_locations = self.page_store.read_row_locations()?;
        if row_locations.is_empty() {
            for page_id in page_ids {
                let page = self.load_page_from_disk(page_id)?;
                for row_id in page.rows.keys() {
                    row_locations.insert(*row_id, page_id);
                }
            }
        }

        if let Ok(mut guard) = self.row_locations.write() {
            *guard = row_locations;
        }
        if let Ok(mut guard) = self.row_space.write() {
            guard.clear();
        }
        self.rebuild_runtime_state();
        Ok(())
    }

    fn memory_limit(&self) -> u64 {
        self.internal_state_manager
            .read()
            .map(|state| state.config.table_memory_limit_bytes)
            .unwrap_or(0)
    }

    fn page_memory_bytes(page: &Page) -> u64 {
        bincode::serialized_size(page).unwrap_or(page.used_bytes.max(1))
    }

    fn touch_page(&self, page_id: PageID) {
        let seq = self.lru_counter.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut lru) = self.lru.write() {
            lru.insert(page_id, seq);
        }
    }

    fn track_page_in_memory(&self, page_id: PageID, page: &Page) {
        let bytes = Self::page_memory_bytes(page);
        self.memory_used.fetch_add(bytes, Ordering::SeqCst);
        self.touch_page(page_id);
    }

    fn untrack_page_in_memory(&self, page_id: PageID, page: &Page) {
        let bytes = Self::page_memory_bytes(page);
        self.memory_used.fetch_sub(bytes, Ordering::SeqCst);
        if let Ok(mut lru) = self.lru.write() {
            lru.remove(&page_id);
        }
    }

    fn rebuild_runtime_state(&self) {
        let Ok(row_space) = self.row_space.read() else {
            return;
        };
        let Ok(row_locations) = self.row_locations.read() else {
            return;
        };

        let mut memory_used = 0u64;
        let mut lru = BTreeMap::new();
        let mut seq = 0u64;
        for (page_id, page) in row_space.iter() {
            if let Ok(page) = page.read() {
                memory_used = memory_used.saturating_add(Self::page_memory_bytes(&page));
                lru.insert(*page_id, seq);
                seq += 1;
            }
        }
        self.memory_used.store(memory_used, Ordering::SeqCst);
        self.lru_counter.store(seq, Ordering::SeqCst);
        if let Ok(mut guard) = self.lru.write() {
            *guard = lru;
        }

        let resident: BTreeSet<PageID> = row_space.keys().copied().collect();
        let referenced: BTreeSet<PageID> = row_locations.values().copied().collect();
        let evicted: BTreeSet<PageID> = referenced
            .into_iter()
            .filter(|page_id| !resident.contains(page_id))
            .collect();
        if let Ok(mut guard) = self.evicted_pages.write() {
            *guard = evicted;
        }
    }

    fn page_is_on_disk(&self, page_id: PageID) -> bool {
        self.on_disk_pages
            .read()
            .map(|pages| pages.contains(&page_id))
            .unwrap_or(false)
    }

    fn mark_pages_on_disk(&self, page_ids: impl IntoIterator<Item = PageID>) {
        if let Ok(mut on_disk) = self.on_disk_pages.write() {
            on_disk.extend(page_ids);
        }
    }

    fn write_page_to_disk(&self, page: &Page) -> Result<(), DataBaseErrors> {
        let row_locations = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .clone();
        self.page_store
            .merge_page(page.id(), page, &row_locations)?;
        self.mark_pages_on_disk(std::iter::once(page.id()));
        Ok(())
    }

    fn load_page_from_disk(&self, page_id: PageID) -> Result<Page, DataBaseErrors> {
        self.page_store
            .read_page(page_id)?
            .ok_or_else(|| DataBaseErrors::QueryError(format!("Page {page_id} not found on disk")))
    }

    fn pick_lru_victim(
        &self,
        row_space: &BTreeMap<PageID, Arc<RwLock<Page>>>,
        pinned: Option<PageID>,
    ) -> Option<PageID> {
        let lru = self.lru.read().ok()?;
        row_space
            .keys()
            .filter(|page_id| pinned.map_or(true, |pinned_id| pinned_id != **page_id))
            .filter_map(|page_id| lru.get(page_id).map(|seq| (*page_id, *seq)))
            .min_by_key(|(_, seq)| *seq)
            .map(|(page_id, _)| page_id)
    }

    fn flush_and_evict_page(
        &self,
        page_id: PageID,
        page_arc: Arc<RwLock<Page>>,
    ) -> Result<(), DataBaseErrors> {
        let page = page_arc
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page read lock".into()))?;
        if page.is_dirty || !self.page_is_on_disk(page_id) {
            self.write_page_to_disk(&page)?;
        }
        self.untrack_page_in_memory(page_id, &page);
        drop(page);
        if let Ok(mut evicted) = self.evicted_pages.write() {
            evicted.insert(page_id);
        }
        Ok(())
    }

    fn evict_page(
        &self,
        page_id: PageID,
        row_space: &mut BTreeMap<PageID, Arc<RwLock<Page>>>,
    ) -> Result<(), DataBaseErrors> {
        let Some(page_arc) = row_space.remove(&page_id) else {
            return Ok(());
        };
        self.flush_and_evict_page(page_id, page_arc)
    }

    fn maybe_evict_pages(&self, pinned: Option<PageID>) -> Result<(), DataBaseErrors> {
        let limit = self.memory_limit();
        if limit == 0 {
            return Ok(());
        }

        loop {
            if self.memory_used.load(Ordering::SeqCst) <= limit {
                return Ok(());
            }

            let victim = {
                let row_space = self
                    .row_space
                    .read()
                    .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;
                self.pick_lru_victim(&row_space, pinned)
            };

            let Some(victim) = victim else {
                return Ok(());
            };

            let page_arc = {
                let mut row_space = self
                    .row_space
                    .write()
                    .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
                if self.memory_used.load(Ordering::SeqCst) <= limit {
                    return Ok(());
                }
                row_space.remove(&victim)
            };

            let Some(page_arc) = page_arc else {
                continue;
            };
            self.flush_and_evict_page(victim, page_arc)?;
        }
    }

    fn ensure_page_resident(
        &self,
        page_id: PageID,
    ) -> Result<Arc<RwLock<Page>>, DataBaseErrors> {
        if let Ok(row_space) = self.row_space.read() {
            if let Some(page) = row_space.get(&page_id) {
                self.touch_page(page_id);
                return Ok(page.clone());
            }
        }

        let page = self.load_page_from_disk(page_id)?;
        let page_arc = Arc::new(RwLock::new(page));
        let memory_bytes = {
            let page = page_arc
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page read lock".into()))?;
            Self::page_memory_bytes(&page)
        };
        {
            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
            if let Some(existing) = row_space.get(&page_id) {
                self.touch_page(page_id);
                return Ok(existing.clone());
            }
            row_space.insert(page_id, page_arc.clone());
        }
        self.memory_used.fetch_add(memory_bytes, Ordering::SeqCst);
        self.touch_page(page_id);
        if let Ok(mut evicted) = self.evicted_pages.write() {
            evicted.remove(&page_id);
        }
        self.maybe_evict_pages(Some(page_id))?;
        Ok(page_arc)
    }

    pub fn persist_dirty_pages(&self) -> Result<(), DataBaseErrors> {
        let row_locations = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .clone();
        let row_space = self
            .row_space
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;
        let mut flushed = Vec::new();
        for page_arc in row_space.values() {
            let page = page_arc
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page read lock".into()))?;
            if page.is_dirty {
                self.page_store
                    .merge_page(page.id(), &page, &row_locations)?;
                flushed.push(page.id());
            }
        }
        self.mark_pages_on_disk(flushed);
        Ok(())
    }

    pub fn flush_all_pages(&self) -> Result<(), DataBaseErrors> {
        let row_locations = self
            .row_locations
            .read()
            .map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into())
            })?
            .clone();

        let row_space = self
            .row_space
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;

        let in_memory: BTreeSet<PageID> = row_space.keys().copied().collect();
        let mut pages: Vec<(PageID, Vec<u8>)> = Vec::new();

        for page_id in self.page_store.list_page_ids()? {
            if in_memory.contains(&page_id) {
                continue;
            }
            if let Some(page) = self.page_store.read_page(page_id)? {
                pages.push((
                    page_id,
                    bincode::serialize(&page)
                        .map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?,
                ));
            }
        }

        for (page_id, page_arc) in row_space.iter() {
            let mut page = page_arc
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page write lock".into()))?;
            page.is_dirty = false;
            pages.push((
                *page_id,
                bincode::serialize(&*page)
                    .map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?,
            ));
        }

        pages.sort_by_key(|(page_id, _)| *page_id);
        self.page_store
            .write_all_serialized(pages.iter().cloned(), &row_locations)?;
        if let Ok(mut on_disk) = self.on_disk_pages.write() {
            *on_disk = pages.into_iter().map(|(page_id, _)| page_id).collect();
        }
        Ok(())
    }

    pub fn new(
        table_id: TableID,
        database_name: String,
        name: String,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
    ) -> Self {
        let page_size = internal_state_manager
            .read()
            .map(|state| state.config.page_size_bytes)
            .unwrap_or(crate::backend::config::DEFAULT_PAGE_SIZE_BYTES)
            .max(1);
        let page_store = internal_state_manager
            .read()
            .map(|state| {
                PageStore::new(Self::table_data_file_path(
                    &state.config,
                    &database_name,
                    &name,
                ))
            })
            .unwrap_or_else(|_| PageStore::default());
        let table = Table {
            table_id,
            database_name,
            name,
            columns: BTreeMap::new(),
            index_names: BTreeMap::new(),
            row_space: RwLock::new(BTreeMap::new()),
            row_locations: RwLock::new(BTreeMap::new()),
            next_page_id: AtomicU64::new(0),
            next_row_id: AtomicU64::new(0),
            next_column_id: AtomicU16::new(0),
            page_size,
            evicted_pages: RwLock::new(BTreeSet::new()),
            on_disk_pages: RwLock::new(BTreeSet::new()),
            lru: RwLock::new(BTreeMap::new()),
            lru_counter: AtomicU64::new(0),
            memory_used: AtomicU64::new(0),
            page_store,
            internal_state_manager,
        };
        table.rebuild_runtime_state();
        table
    }

    pub fn inject_contexts(
        &mut self,
        database_name: &str,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
    ) {
        self.bind_page_store(database_name, internal_state_manager);
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_id(&self) -> TableID {
        self.table_id
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
        self.build_column_index_if_needed(column_id, transaction)?;
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
        drop(column);
        self.build_column_index_if_needed(column_id, transaction)?;
        Ok(())
    }

    fn with_row<F, R>(&self, row_id: RowID, f: F) -> Result<R, DataBaseErrors>
    where
        F: FnOnce(Option<&Row>) -> R,
    {
        let page = self.page_for_row(row_id)?;
        let page_guard = page.read().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire page read lock".into())
        })?;
        Ok(f(page_guard.rows.get(&row_id)))
    }

    fn row_id_iter(&self) -> Result<Vec<RowID>, DataBaseErrors> {
        Ok(self
            .row_locations
            .read()
            .map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into())
            })?
            .keys()
            .copied()
            .collect())
    }

    pub fn for_each_visible_row<F>(
        &self,
        transaction: &Transaction,
        mut f: F,
    ) -> Result<(), DataBaseErrors>
    where
        F: FnMut(RowID, RowData) -> Result<(), DataBaseErrors>,
    {
        let page_ids: BTreeSet<PageID> = self
            .row_locations
            .read()
            .map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into())
            })?
            .values()
            .copied()
            .collect();

        for page_id in page_ids {
            let page = self.ensure_page_resident(page_id)?;
            let page_guard = page.read().map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire page read lock".into())
            })?;
            for (row_id, row) in page_guard.rows.iter() {
                if let Some(version) = row.get_versioned_row(transaction) {
                    f(*row_id, version.data.clone())?;
                }
            }
        }
        Ok(())
    }

    pub fn lookup_visible_row_data(
        &self,
        row_id: RowID,
        transaction: &Transaction,
    ) -> Result<Option<Arc<RowData>>, DataBaseErrors> {
        let page = self.page_for_row(row_id)?;
        let page_guard = page.read().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire page read lock".into())
        })?;
        Ok(page_guard
            .rows
            .get(&row_id)
            .and_then(|row| row.get_versioned_row(transaction))
            .map(|version| Arc::new(version.data.clone())))
    }

    fn row_is_alive_for_prune(
        &self,
        row_id: RowID,
        oldest_active_txn: TransactionID,
    ) -> bool {
        self.with_row(row_id, |row| {
            row.map(|row| {
                row.get_raw_rows().iter().any(|version| {
                    match version.transaction_header.deleted_by {
                        None => true,
                        Some(del) => del >= oldest_active_txn,
                    }
                })
            })
            .unwrap_or(false)
        })
        .unwrap_or(false)
    }

    fn build_column_index_from_rows(
        &self,
        column_id: ColumnID,
        transaction: &Transaction,
    ) -> Result<usize, DataBaseErrors> {
        {
            let mut column = self.columns.get(&column_id).ok_or_else(|| {
                DataBaseErrors::QueryError("Column not found while building index".into())
            })?;
            let mut column = column
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column write lock".into()))?;
            if !column.is_indexed(transaction) {
                return Ok(0);
            }
            column.ensure_index_ready(transaction)?;
        }

        let mut indexed = 0usize;
        for row_id in self.row_id_iter()? {
            self.with_row(row_id, |row| -> Result<(), DataBaseErrors> {
                if let Some(row) = row {
                    let mut column = self.columns.get(&column_id).ok_or_else(|| {
                        DataBaseErrors::QueryError("Column not found while building index".into())
                    })?;
                    let mut column = column.write().map_err(|_| {
                        DataBaseErrors::QueryError("Failed to acquire column write lock".into())
                    })?;
                    if column.index_row(row_id, row, transaction) {
                        indexed += 1;
                    }
                }
                Ok(())
            })??;
        }
        Ok(indexed)
    }

    fn materialize_all_pages(&self) -> Result<(), DataBaseErrors> {
        let page_ids: BTreeSet<PageID> = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .values()
            .copied()
            .collect();
        for page_id in page_ids {
            self.ensure_page_resident(page_id)?;
        }
        Ok(())
    }

    fn build_column_index_if_needed(
        &self,
        column_id: ColumnID,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        self.build_column_index_from_rows(column_id, transaction)?;
        Ok(())
    }

    fn validate_row_data<G>(
        &self,
        data: &AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
        exclude_row_id: Option<RowID>,
        mut fk_lookup: G,
    ) -> Result<(), DataBaseErrors>
    where
        G: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
    {
        for (&column_id, column) in self.columns.iter() {
            let guard = column
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
            let value = data
                .get(&column_id)
                .unwrap_or(&DataBaseDataEntry::Null);
            let Some(version) = guard.get_versioned_column(transaction) else {
                continue;
            };
            if !Self::validate_data_type(value, &version.data_type, version.is_nullable()) {
                return Err(DataBaseErrors::DataTypeMismatch(
                    column_id as u64,
                    version.data_type.name(),
                    value.data_type().name(),
                ));
            }
            guard.schema_validation(
                value,
                |row_id| {
                    self.with_versioned_column_entry(row_id, column_id, transaction, |entry| {
                        entry.cloned()
                    })
                },
                &mut fk_lookup,
                transaction,
                exclude_row_id,
            )?;
        }
        Ok(())
    }

    pub fn referenced_value_exists(
        &self,
        column_id: ColumnID,
        value: &DataBaseDataEntry,
        transaction: &Transaction,
    ) -> Result<bool, DataBaseErrors> {
        Ok(!self
            .lookup_rows_by_column_value(column_id, value, transaction)?
            .is_empty())
    }

    fn filter_visible_index_candidates(
        &self,
        row_ids: Vec<RowID>,
        transaction: &Transaction,
    ) -> Result<Vec<RowID>, DataBaseErrors> {
        let mut visible = Vec::new();
        for row_id in row_ids {
            let page = self.page_for_row(row_id)?;
            let page_guard = page.read().map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire page read lock".into())
            })?;
            if let Some(row) = page_guard.rows.get(&row_id) {
                if row.get_versioned_row(transaction).is_some() {
                    visible.push(row_id);
                }
            }
        }
        Ok(visible)
    }

    fn update_indexes_for_row(
        &self,
        row_id: RowID,
        data: &AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        for (&column_id, column) in self.columns.iter() {
            let mut guard = column.write().map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire column write lock".into())
            })?;
            if !guard.is_indexed(transaction) {
                continue;
            }
            if let Some(value) = data.get(&column_id) {
                guard.update_index(column_id, value.clone(), row_id, transaction);
            }
        }
        Ok(())
    }

    pub fn is_column_indexed(
        &self,
        column_name: &str,
        transaction: &Transaction,
    ) -> Result<bool, DataBaseErrors> {
        let normalized = Self::normalize_column_name(column_name);
        let column_map = self.get_visible_column_map(transaction)?;
        let Some(column_id) = column_map.get(&normalized) else {
            return Ok(false);
        };
        let column = self.columns.get(column_id).ok_or_else(|| {
            DataBaseErrors::QueryError("Column not found while checking index".into())
        })?;
        let column = column
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
        Ok(column.is_indexed(transaction))
    }

    pub fn create_column_index(
        &mut self,
        index_name: Option<String>,
        column_name: &str,
        transaction: &Transaction,
    ) -> Result<usize, DataBaseErrors> {
        let normalized_column = Self::normalize_column_name(column_name);
        let column_map = self.get_visible_column_map(transaction)?;
        let column_id = *column_map.get(&normalized_column).ok_or_else(|| {
            DataBaseErrors::QueryError(format!("Column '{normalized_column}' not found"))
        })?;

        let indexed = self.build_column_index_from_rows(column_id, transaction)?;

        if let Some(name) = index_name {
            self.index_names
                .insert(Self::normalize_column_name(&name), column_id);
        }

        Ok(indexed)
    }

    pub fn drop_column_index(
        &mut self,
        index_name: &str,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let normalized = Self::normalize_column_name(index_name);
        let column_id = if let Some(column_id) = self.index_names.get(&normalized).copied() {
            column_id
        } else {
            let column_map = self.get_visible_column_map(transaction)?;
            *column_map.get(&normalized).ok_or_else(|| {
                DataBaseErrors::IndexNotFound(index_name.to_string())
            })?
        };

        let column = self.columns.get(&column_id).ok_or_else(|| {
            DataBaseErrors::QueryError("Column not found while dropping index".into())
        })?;
        let mut column = column
            .write()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column write lock".into()))?;
        column.drop_index(transaction)?;
        self.index_names.remove(&normalized);
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        column_id: ColumnID,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let column = self.columns.get(&column_id).ok_or(DataBaseErrors::ColumnNotFound(
            column_id as u64,
        ))?;
        column
            .write()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column write lock".into()))?
            .remove(transaction);
        Ok(())
    }

    /// Register a fresh empty page in `row_space`. Holds the write lock only
    /// for the map insert, not for the subsequent row append.
    fn register_page(&self) -> Result<(PageID, Arc<RwLock<Page>>), DataBaseErrors> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let page = Arc::new(RwLock::new(Page::new(page_id, self.page_size)));
        let memory_bytes = {
            let page_guard = page
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page read lock".into()))?;
            Self::page_memory_bytes(&page_guard)
        };
        {
            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
            row_space.insert(page_id, page.clone());
        }
        self.memory_used.fetch_add(memory_bytes, Ordering::SeqCst);
        self.touch_page(page_id);
        self.maybe_evict_pages(None)?;
        Ok((page_id, page))
    }

    /// Append when the caller has already confirmed the page has room (or when
    /// the row is stored in a dedicated overflow head page that may exceed capacity).
    fn force_append_to_page(
        page: &Arc<RwLock<Page>>,
        row_id: RowID,
        row: Row,
        size: u64,
    ) -> Result<(), DataBaseErrors> {
        page.write()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page write lock".into()))?
            .append(row_id, row, size);
        Ok(())
    }

    /// Try to append under a page write lock. Returns the row back when the page
    /// is full so the caller can retry on a different page.
    fn try_append_to_page(
        page: &Arc<RwLock<Page>>,
        row_id: RowID,
        row: Row,
        size: u64,
    ) -> Result<(), Row> {
        let mut page = match page.write() {
            Ok(guard) => guard,
            Err(_) => return Err(row),
        };
        if !page.has_room_for(size) {
            return Err(row);
        }
        page.append(row_id, row, size);
        Ok(())
    }

    /// Try to append to the current tail page without holding `row_space` write.
    fn try_append_to_tail_page(
        &self,
        row_id: RowID,
        row: &mut Row,
        size: u64,
    ) -> Result<Option<PageID>, DataBaseErrors> {
        let tail = {
            let row_space = self
                .row_space
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;
            row_space
                .iter()
                .next_back()
                .map(|(id, page)| (*id, page.clone()))
        };

        let Some((tail_id, tail_page)) = tail else {
            return Ok(None);
        };

        match Self::try_append_to_page(&tail_page, row_id, row.clone(), size) {
            Ok(()) => {
                self.touch_page(tail_id);
                Ok(Some(tail_id))
            }
            Err(returned_row) => {
                *row = returned_row;
                Ok(None)
            }
        }
    }

    /// Append a row to the tail page, allocating a new page only when needed.
    /// Page write locks are never taken while holding `row_space` write.
    fn insert_row_into_page(
        &self,
        row_id: RowID,
        mut row: Row,
        size: u64,
    ) -> Result<PageID, DataBaseErrors> {
        if let Some(tail_id) = self.try_append_to_tail_page(row_id, &mut row, size)? {
            return Ok(tail_id);
        }

        // Retry once: another inserter may have appended or extended the tail.
        if let Some(tail_id) = self.try_append_to_tail_page(row_id, &mut row, size)? {
            return Ok(tail_id);
        }

        let (page_id, page) = self.register_page()?;
        Self::force_append_to_page(&page, row_id, row, size)?;
        Ok(page_id)
    }

    /// Store an oversized row (larger than a single page) in its own overflow
    /// chain: a head page holding the row followed by continuation pages that
    /// reserve the remaining page-sized slots it spans.
    fn insert_overflow_row(
        &self,
        row_id: RowID,
        row: Row,
        size: u64,
    ) -> Result<PageID, DataBaseErrors> {
        let (head_id, head) = self.register_page()?;
        Self::force_append_to_page(&head, row_id, row, size)?;

        // Number of extra page-sized slots beyond the head page that this entry
        // occupies. ceil(size / page_size) - 1.
        let extra_pages = size.div_ceil(self.page_size).saturating_sub(1);
        let mut previous = head;
        for _ in 0..extra_pages {
            let (continuation_id, continuation) = self.register_page()?;
            {
                let mut continuation = continuation.write().map_err(|_| {
                    DataBaseErrors::QueryError("Failed to acquire page write lock".into())
                })?;
                continuation.is_overflow = true;
                continuation.used_bytes = continuation.capacity;
                continuation.is_dirty = true;
            }
            previous
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page write lock".into()))?
                .next_page = Some(continuation_id);
            previous = continuation;
        }

        Ok(head_id)
    }

    pub fn insert_row<G>(
        &self,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
        fk_lookup: G,
    ) -> Result<RowID, DataBaseErrors>
    where
        G: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
    {
        self.validate_row_data(&data, transaction, None, fk_lookup)?;

        let row_id = self.next_row_id.fetch_add(1, Ordering::SeqCst) + 1;
        let row = Row::new(transaction, data.clone());
        let size = row_size_bytes(&row);

        let page_id = if size > self.page_size {
            self.insert_overflow_row(row_id, row, size)?
        } else {
            self.insert_row_into_page(row_id, row, size)?
        };

        self.row_locations
            .write()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations write lock".into()))?
            .insert(row_id, page_id);

        self.update_indexes_for_row(row_id, &data, transaction)?;

        Ok(row_id)
    }

    fn page_for_row(&self, row_id: RowID) -> Result<Arc<RwLock<Page>>, DataBaseErrors> {
        let page_id = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .get(&row_id)
            .copied()
            .ok_or(DataBaseErrors::RowNotFound(row_id))?;
        self.ensure_page_resident(page_id)
    }

    fn with_versioned_column_entry<F, R>(
        &self,
        row_id: RowID,
        column_id: ColumnID,
        transaction: &Transaction,
        f: F,
    ) -> Result<R, DataBaseErrors>
    where
        F: FnOnce(Option<&DataBaseDataEntry>) -> R,
    {
        let page = self.page_for_row(row_id)?;
        let page_guard = page.read().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire page read lock".into())
        })?;
        let entry = page_guard
            .rows
            .get(&row_id)
            .and_then(|row| row.get_versioned_row(transaction))
            .and_then(|version| version.data.get(&column_id));
        Ok(f(entry))
    }

    pub fn delete_row(
        &self,
        row_id: RowID,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let page = self.page_for_row(row_id)?;
        let mut page = page.write().unwrap();
        page.rows
            .get_mut(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?
            .remove(transaction);
        page.is_dirty = true;
        Ok(())
    }

    pub fn update_row<G>(
        &self,
        row_id: RowID,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
        fk_lookup: G,
    ) -> Result<(), DataBaseErrors>
    where
        G: FnMut(TableID, ColumnID, &DataBaseDataEntry) -> Result<bool, DataBaseErrors>,
    {
        let page = self.page_for_row(row_id)?;
        let mut page = page.write().unwrap();
        let row = page
            .rows
            .get(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?;

        let mut effective_data: AHashMap<ColumnID, DataBaseDataEntry> = row
            .get_versioned_row(transaction)
            .map(|version| version.data.iter().map(|(&k, v)| (k, v.clone())).collect())
            .unwrap_or_default();
        effective_data.extend(data.iter().map(|(&k, v)| (k, v.clone())));

        self.validate_row_data(&effective_data, transaction, Some(row_id), fk_lookup)?;

        page.rows
            .get_mut(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?
            .update(transaction, data.clone());
        page.is_dirty = true;
        drop(page);

        self.update_indexes_for_row(row_id, &effective_data, transaction)?;

        Ok(())
    }

    pub fn get_row(&self, row_id: RowID) -> Result<Row, DataBaseErrors> {
        let page = self.page_for_row(row_id)?;
        let page = page.read().unwrap();
        match page.rows.get(&row_id) {
            Some(row) => Ok(row.clone()),
            None => Err(DataBaseErrors::RowNotFound(row_id)),
        }
    }


    fn normalize_column_name(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    pub fn get_copy_column_specs(
        &self,
        transaction: &Transaction,
        requested_columns: &[String],
    ) -> Result<Vec<(String, ColumnID, DataBaseDataType)>, DataBaseErrors> {
        let visible_map = self.get_visible_column_map(transaction)?;

        let column_names: Vec<String> = if !requested_columns.is_empty() {
            requested_columns.to_vec()
        } else {
            let mut columns: Vec<(ColumnID, String)> = self
                .columns
                .iter()
                .filter_map(|(&column_id, column)| {
                    column.read().ok().and_then(|guard| {
                        guard
                            .get_versioned_column(transaction)
                            .map(|version| (column_id, version.name.to_ascii_lowercase()))
                    })
                })
                .collect();
            columns.sort_by_key(|(column_id, _)| *column_id);
            columns.into_iter().map(|(_, name)| name).collect()
        };

        let mut specs = Vec::with_capacity(column_names.len());
        for column_name in column_names {
            let column_id = visible_map.get(&column_name).copied().ok_or_else(|| {
                DataBaseErrors::QueryError(format!("Column '{column_name}' not found"))
            })?;
            let column = self.columns.get(&column_id).ok_or_else(|| {
                DataBaseErrors::QueryError(format!("Column '{column_name}' not found"))
            })?;
            let guard = column
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
            let version = guard.get_versioned_column(transaction).ok_or_else(|| {
                DataBaseErrors::QueryError(format!("Column '{column_name}' not found"))
            })?;
            specs.push((
                column_name,
                column_id,
                version.data_type.clone(),
            ));
        }

        Ok(specs)
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

    pub fn collect_visible_rows(
        &self,
        transaction: &Transaction,
    ) -> Result<Vec<(RowID, RowData)>, DataBaseErrors> {
        let mut rows = Vec::new();
        self.for_each_visible_row(transaction, |row_id, data| {
            rows.push((row_id, data));
            Ok(())
        })?;
        Ok(rows)
    }

    pub fn lookup_rows_by_column_value(
        &self,
        column_id: ColumnID,
        value: &DataBaseDataEntry,
        transaction: &Transaction,
    ) -> Result<Vec<RowID>, DataBaseErrors> {
        let column = self.columns.get(&column_id).ok_or_else(|| {
            DataBaseErrors::QueryError("Column not found while resolving join lookup".into())
        })?;
        let guard = column.read().map_err(|_| {
            DataBaseErrors::QueryError("Failed to acquire column read lock".into())
        })?;

        if let Some(row_ids) = guard.index_lookup(value, transaction) {
            return self.filter_visible_index_candidates(row_ids, transaction);
        }

        let mut matches = Vec::new();
        self.for_each_visible_row(transaction, |row_id, data| {
            if data
                .get(&column_id)
                .map(|entry| entry == value)
                .unwrap_or(false)
            {
                matches.push(row_id);
            }
            Ok(())
        })?;
        Ok(matches)
    }

    pub fn column_is_indexed(
        &self,
        column_name: &str,
        transaction: &Transaction,
    ) -> Result<bool, DataBaseErrors> {
        let column_map = self.get_visible_column_map(transaction)?;
        let column_id = column_map.get(&column_name.to_ascii_lowercase()).ok_or_else(|| {
            DataBaseErrors::QueryError(format!("Column '{column_name}' not found"))
        })?;
        let column = self.columns.get(column_id).ok_or_else(|| {
            DataBaseErrors::QueryError(format!("Column '{column_name}' not found"))
        })?;
        let guard = column
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
        Ok(guard.is_indexed(transaction))
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

    pub fn column_data_type(
        &self,
        column_id: ColumnID,
        transaction: &Transaction,
    ) -> Result<Option<DataBaseDataType>, DataBaseErrors> {
        let column = self.columns.get(&column_id).ok_or_else(|| {
            DataBaseErrors::QueryError(format!("Column {column_id} not found"))
        })?;
        let guard = column
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
        Ok(guard
            .get_versioned_column(transaction)
            .map(|version| version.data_type.clone()))
    }

    fn index_lookup_candidates(
        &self,
        column_name: &str,
        value: &DataBaseDataEntry,
        transaction: &Transaction,
        visible_columns: &HashMap<String, ColumnID>,
    ) -> Result<Option<BTreeSet<RowID>>, DataBaseErrors> {
        let Some(column_id) = visible_columns.get(column_name) else {
            return Ok(None);
        };
        let column = self.columns.get(column_id).ok_or_else(|| {
            DataBaseErrors::QueryError(format!(
                "Column '{column_name}' not found while resolving index lookup",
            ))
        })?;
        let guard = column
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column read lock".into()))?;
        Ok(guard
            .index_lookup(value, transaction)
            .map(|ids| ids.into_iter().collect::<BTreeSet<RowID>>())
            .map(|ids| {
                self.filter_visible_index_candidates(ids.into_iter().collect(), transaction)
            })
            .transpose()?
            .map(|ids| ids.into_iter().collect()))
    }

    pub fn search(
        &self,
        request: &SearchRequest,
        transaction: &Transaction,
        access_path: Option<&AccessPath>,
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

        if let Some(crate::backend::core::search::AggregateProjection::CountStar { output_name }) =
            &request.aggregate
        {
            if !request.order_by.is_empty() {
                return Err(DataBaseErrors::QueryError(
                    "ORDER BY is unsupported with COUNT(*)".into(),
                ));
            }

            let mut count = 0usize;
            let mut count_row = |row_id: RowID,
                                 version: &crate::backend::core::row::InternalRow|
             -> Result<(), DataBaseErrors> {
                let mut ctx = RowEvaluationContext::default();
                for (column_name, column_id) in &visible_columns {
                    let value = version
                        .data
                        .get(column_id)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    ctx.values.insert(column_name.clone(), value);
                }
                let matches = match &request.filter {
                    Some(filter) => filter.evaluate(&ctx)?,
                    None => true,
                };
                if matches {
                    count += 1;
                }
                let _ = row_id;
                Ok(())
            };

            if let Some(candidate_row_ids) = match &request.filter {
                Some(filter) if filter.equality_lookup().is_some() => {
                    if let Some((column_name, value)) = filter.equality_lookup() {
                        self.index_lookup_candidates(
                            column_name,
                            value,
                            transaction,
                            &visible_columns,
                        )?
                    } else {
                        None
                    }
                }
                _ => None,
            } {
                for row_id in candidate_row_ids {
                    let page = self.page_for_row(row_id)?;
                    let page_guard = page.read().unwrap();
                    if let Some(row) = page_guard.rows.get(&row_id) {
                        if let Some(version) = row.get_versioned_row(transaction) {
                            count_row(row_id, version)?;
                        }
                    }
                }
            } else {
                let page_ids: BTreeSet<PageID> = self
                    .row_locations
                    .read()
                    .map_err(|_| {
                        DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into())
                    })?
                    .values()
                    .copied()
                    .collect();

                for page_id in page_ids {
                    let page = self.ensure_page_resident(page_id)?;
                    let page_guard = page.read().unwrap();
                    for (row_id, row) in page_guard.rows.iter() {
                        if let Some(version) = row.get_versioned_row(transaction) {
                            count_row(*row_id, version)?;
                        }
                    }
                }
            }

            let mut values = AHashMap::new();
            values.insert(
                output_name.clone(),
                DataBaseDataEntry::IntegerI64(count as i64),
            );
            return Ok(vec![SearchResult { row_id: 0, values }]);
        }

        let projection_columns: Vec<(String, ColumnID)> = match &request.projection {
            Some(projection) => projection
                .iter()
                .map(|column| {
                    if column.table_alias.is_some() {
                        return Err(DataBaseErrors::QueryError(format!(
                            "Qualified column '{}' is only supported in JOIN queries",
                            column.column_name
                        )));
                    }
                    visible_columns
                        .get(&column.column_name)
                        .cloned()
                        .map(|id| (column.output_name.clone(), id))
                        .ok_or_else(|| {
                            DataBaseErrors::QueryError(format!(
                                "Projection column '{}' not found",
                                column.column_name
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

        let mut matching_row_ids: Vec<RowID> = Vec::new();

        let candidate_row_ids: Option<BTreeSet<RowID>> = match access_path {
            Some(AccessPath::SeqScan) => None,
            Some(AccessPath::IndexEquality { column_name, value }) => {
                self.index_lookup_candidates(column_name, value, transaction, &visible_columns)?
            }
            None => {
                if let Some(filter) = &request.filter {
                    if let Some((column_name, value)) = filter.equality_lookup() {
                        self.index_lookup_candidates(
                            column_name,
                            value,
                            transaction,
                            &visible_columns,
                        )?
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        let mut push_matching_row =
            |row_id: RowID, version: &crate::backend::core::row::InternalRow| -> Result<(), DataBaseErrors> {
                let mut ctx = RowEvaluationContext::default();
                for (column_name, column_id) in &visible_columns {
                    let value = version
                        .data
                        .get(column_id)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    ctx.values.insert(column_name.clone(), value);
                }
                let matches = match &request.filter {
                    Some(filter) => filter.evaluate(&ctx)?,
                    None => true,
                };
                if matches {
                    matching_row_ids.push(row_id);
                }
                Ok(())
            };

        if let Some(candidate_row_ids) = candidate_row_ids {
            for row_id in candidate_row_ids {
                let page = self.page_for_row(row_id)?;
                let page_guard = page.read().unwrap();
                if let Some(row) = page_guard.rows.get(&row_id) {
                    if let Some(version) = row.get_versioned_row(transaction) {
                        push_matching_row(row_id, version)?;
                    }
                }
            }
        } else {
            let page_ids: BTreeSet<PageID> = self
                .row_locations
                .read()
                .map_err(|_| {
                    DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into())
                })?
                .values()
                .copied()
                .collect();

            for page_id in page_ids {
                let page = self.ensure_page_resident(page_id)?;
                let page_guard = page.read().unwrap();
                for (row_id, row) in page_guard.rows.iter() {
                    if let Some(version) = row.get_versioned_row(transaction) {
                        push_matching_row(*row_id, version)?;
                    }
                }
            }
        }

        if !sort_columns.is_empty() {
            matching_row_ids.sort_by(|&a, &b| {
                for (column_id, sort_by) in sort_columns.iter() {
                    let order = self
                        .with_versioned_column_entry(a, *column_id, transaction, |left| {
                            self.with_versioned_column_entry(b, *column_id, transaction, |right| {
                                left.unwrap_or(&DataBaseDataEntry::Null)
                                    .cmp(right.unwrap_or(&DataBaseDataEntry::Null))
                            })
                            .expect("matching row must remain readable during sort")
                        })
                        .expect("matching row must remain readable during sort");
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
        let results = matching_row_ids
            .into_iter()
            .skip(offset)
            .take(request.limit.unwrap_or(usize::MAX))
            .map(|row_id| {
                let page = self.page_for_row(row_id)?;
                let page_guard = page.read().map_err(|_| {
                    DataBaseErrors::QueryError("Failed to acquire page read lock".into())
                })?;
                let version = page_guard
                    .rows
                    .get(&row_id)
                    .and_then(|row| row.get_versioned_row(transaction))
                    .ok_or(DataBaseErrors::RowNotFound(row_id))?;
                let mut values = AHashMap::new();
                for (column_name, column_id) in &projection_columns {
                    let value = version
                        .data
                        .get(column_id)
                        .cloned()
                        .unwrap_or(DataBaseDataEntry::Null);
                    values.insert(column_name.clone(), value);
                }
                Ok(SearchResult { row_id, values })
            })
            .collect::<Result<Vec<_>, DataBaseErrors>>()?;

        Ok(results)
    }

    /// Drop MVCC versions and index entries that are no longer visible to any
    /// active transaction. Does not repack pages.
    pub fn prune_versions(&mut self, oldest_active_txn: TransactionID) {
        if let Ok(row_space) = self.row_space.read() {
            for page in row_space.values() {
                page.write().unwrap().rows.retain(|_, row| {
                    row.prune(oldest_active_txn);
                    !row.get_raw_rows().is_empty()
                });
            }
        }

        let column_handles: Vec<_> = self.columns.values().cloned().collect();
        for column in column_handles {
            column.write().unwrap().prune(oldest_active_txn, &mut |row_id| {
                self.row_is_alive_for_prune(row_id, oldest_active_txn)
            });
        }
    }

    /// Full vacuum: prune stale versions/index entries, then compact pages and flush.
    pub fn prune(&mut self, oldest_active_txn: TransactionID) {
        if let Err(err) = self.materialize_all_pages() {
            error!(
                "Failed to materialize pages for table '{}' before vacuum: {err}",
                self.name
            );
            return;
        }

        self.prune_versions(oldest_active_txn);

        // Vacuum reclaims the holes left behind by deletes/pruned versions by
        // repacking the surviving rows densely, in insertion order, into fresh
        // pages. This is the only place rows move between pages.
        self.compact();
        if let Err(err) = self.flush_all_pages() {
            error!(
                "Failed to flush table '{}' during vacuum: {err}",
                self.name
            );
        }
    }

    /// Repack every surviving row into a fresh, densely packed sequence of
    /// pages. Rows that no longer have any versions are dropped, reclaiming the
    /// space they occupied. Insertion order (RowID order) is preserved and the
    /// row -> page index is rebuilt to match.
    fn compact(&self) {
        let Ok(mut row_space) = self.row_space.write() else {
            return;
        };
        let Ok(mut row_locations) = self.row_locations.write() else {
            return;
        };

        // Drain every live row in insertion order. `BTreeMap` iteration over the
        // pages (PageID order) and their rows (RowID order) is already ordered.
        let mut live_rows: Vec<(RowID, Row, u64)> = Vec::new();
        for page in row_space.values() {
            let mut page = page.write().unwrap();
            for (row_id, row) in std::mem::take(&mut page.rows) {
                if row.get_raw_rows().is_empty() {
                    continue;
                }
                let size = row_size_bytes(&row);
                live_rows.push((row_id, row, size));
            }
        }
        live_rows.sort_by_key(|(row_id, _, _)| *row_id);

        // Rebuild the page space from scratch.
        let mut new_space: BTreeMap<PageID, Arc<RwLock<Page>>> = BTreeMap::new();
        let mut new_locations: BTreeMap<RowID, PageID> = BTreeMap::new();
        let mut next_page_id: PageID = 0;

        let mut alloc_page = |space: &mut BTreeMap<PageID, Arc<RwLock<Page>>>| -> PageID {
            let id = next_page_id;
            next_page_id += 1;
            space.insert(id, Arc::new(RwLock::new(Page::new(id, self.page_size))));
            id
        };

        for (row_id, row, size) in live_rows {
            if size > self.page_size {
                // Oversized row: rebuild its dedicated overflow chain.
                let head_id = alloc_page(&mut new_space);
                new_space
                    .get(&head_id)
                    .unwrap()
                    .write()
                    .unwrap()
                    .append(row_id, row, size);
                new_locations.insert(row_id, head_id);

                let extra_pages = size.div_ceil(self.page_size).saturating_sub(1);
                let mut previous_id = head_id;
                for _ in 0..extra_pages {
                    let continuation_id = alloc_page(&mut new_space);
                    {
                        let continuation = new_space.get(&continuation_id).unwrap();
                        let mut continuation = continuation.write().unwrap();
                        continuation.is_overflow = true;
                        continuation.used_bytes = continuation.capacity;
                    }
                    new_space.get(&previous_id).unwrap().write().unwrap().next_page =
                        Some(continuation_id);
                    previous_id = continuation_id;
                }
                continue;
            }

            // Regular row: append to the current tail page or open a new one.
            let tail_with_room = new_space
                .iter()
                .next_back()
                .filter(|(_, page)| page.read().unwrap().has_room_for(size))
                .map(|(id, _)| *id);
            let target_id = match tail_with_room {
                Some(id) => id,
                None => alloc_page(&mut new_space),
            };
            new_space
                .get(&target_id)
                .unwrap()
                .write()
                .unwrap()
                .append(row_id, row, size);
            new_locations.insert(row_id, target_id);
        }

        drop(alloc_page);
        self.next_page_id.store(next_page_id, Ordering::SeqCst);
        *row_space = new_space;
        *row_locations = new_locations;
        drop(row_space);
        drop(row_locations);
        if let Ok(mut evicted) = self.evicted_pages.write() {
            evicted.clear();
        }
        if let Ok(mut on_disk) = self.on_disk_pages.write() {
            on_disk.clear();
        }
        self.rebuild_runtime_state();
    }

    pub fn rollback_transaction(&mut self, transaction: &Transaction) {
        for column in self.columns.values() {
            column
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire column write lock".into()))
                .expect("column rollback lock poisoned")
                .rollback_transaction(transaction);
        }
        let row_space = self
            .row_space
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))
            .expect("row_space rollback lock poisoned");
        for page in row_space.values() {
            let mut page_guard = page
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page write lock".into()))
                .expect("page rollback lock poisoned");
            for row in page_guard.rows.values_mut() {
                row.rollback_transaction(transaction);
            }
        }
    }

    pub fn replay(self) {
        
    }
}
