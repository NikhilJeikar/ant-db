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
use crate::backend::core::row::{DataBaseDataEntry, Row, RowID};
use crate::backend::core::search::{OrderBy, SearchRequest, SearchResult, SortBy};
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
    row_space: &RwLock<BTreeMap<PageID, Arc<RwLock<Page>>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let guard = row_space
        .read()
        .map_err(|_| serde::ser::Error::custom("Failed to acquire row_space read lock"))?;
    let export: BTreeMap<PageID, Page> = guard
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
    row_locations: &RwLock<BTreeMap<RowID, PageID>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let guard = row_locations
        .read()
        .map_err(|_| serde::ser::Error::custom("Failed to acquire row_locations read lock"))?;
    guard.serialize(serializer)
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

    pub fn bind_page_store(&mut self, database_name: &str) {
        if self.database_name.is_empty() {
            self.database_name = database_name.to_string();
        }
        self.refresh_page_store();
        self.rebuild_runtime_state();
        if let Err(err) = self.sync_on_disk_pages_from_store() {
            error!(
                "Failed to sync on-disk page index for table '{}': {err}",
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

    fn write_pages_to_disk(&self, pages: &BTreeMap<PageID, Page>) -> Result<(), DataBaseErrors> {
        if pages.is_empty() {
            return Ok(());
        }
        self.page_store.merge_pages(pages)?;
        self.mark_pages_on_disk(pages.keys().copied());
        Ok(())
    }

    fn write_page_to_disk(&self, page: &Page) -> Result<(), DataBaseErrors> {
        let mut pages = BTreeMap::new();
        pages.insert(page.id(), page.clone());
        self.write_pages_to_disk(&pages)
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

    fn evict_page(
        &self,
        page_id: PageID,
        row_space: &mut BTreeMap<PageID, Arc<RwLock<Page>>>,
    ) -> Result<(), DataBaseErrors> {
        let Some(page_arc) = row_space.remove(&page_id) else {
            return Ok(());
        };
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

            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
            if self.memory_used.load(Ordering::SeqCst) <= limit {
                return Ok(());
            }
            self.evict_page(victim, &mut row_space)?;
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
        {
            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
            if let Some(existing) = row_space.get(&page_id) {
                self.touch_page(page_id);
                return Ok(existing.clone());
            }
            self.track_page_in_memory(page_id, &page_arc.read().unwrap());
            row_space.insert(page_id, page_arc.clone());
        }
        if let Ok(mut evicted) = self.evicted_pages.write() {
            evicted.remove(&page_id);
        }
        self.maybe_evict_pages(Some(page_id))?;
        Ok(page_arc)
    }

    pub fn persist_dirty_pages(&self) -> Result<(), DataBaseErrors> {
        let row_space = self
            .row_space
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;
        let mut updates = BTreeMap::new();
        for page_arc in row_space.values() {
            let page = page_arc
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page read lock".into()))?;
            if page.is_dirty {
                updates.insert(page.id(), page.clone());
            }
        }
        self.write_pages_to_disk(&updates)
    }

    pub fn flush_all_pages(&self) -> Result<(), DataBaseErrors> {
        let page_ids: BTreeSet<PageID> = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .values()
            .copied()
            .collect();

        let mut pages_on_disk = BTreeMap::new();
        for page_id in page_ids {
            let page_arc = self.ensure_page_resident(page_id)?;
            let mut page = page_arc
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire page write lock".into()))?;
            page.is_dirty = false;
            pages_on_disk.insert(page_id, page.clone());
        }

        self.page_store.write_all(&pages_on_disk)?;
        if let Ok(mut on_disk) = self.on_disk_pages.write() {
            *on_disk = pages_on_disk.keys().copied().collect();
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
        self.internal_state_manager = internal_state_manager;
        if self.database_name.is_empty() {
            self.database_name = database_name.to_string();
        }
        self.refresh_page_store();
        self.rebuild_runtime_state();
    }

    pub fn name(&self) -> &str {
        &self.name
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

    /// Register a fresh empty page in `row_space`. Holds the write lock only
    /// for the map insert, not for the subsequent row append.
    fn register_page(&self) -> Result<(PageID, Arc<RwLock<Page>>), DataBaseErrors> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let page = Arc::new(RwLock::new(Page::new(page_id, self.page_size)));
        {
            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;
            self.track_page_in_memory(page_id, &page.read().unwrap());
            row_space.insert(page_id, page.clone());
        }
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

    /// Append a row to the tail page, allocating a new page only when needed.
    /// Concurrent inserters synchronize on individual pages; `row_space` write
    /// locks are held only briefly during page registration.
    fn insert_row_into_page(
        &self,
        row_id: RowID,
        mut row: Row,
        size: u64,
    ) -> Result<PageID, DataBaseErrors> {
        // Fast path: shared lock on the page map, exclusive lock on one page.
        {
            let row_space = self
                .row_space
                .read()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space read lock".into()))?;
            if let Some((tail_id, tail_page)) = row_space
                .iter()
                .next_back()
                .map(|(id, page)| (*id, page.clone()))
            {
                match Self::try_append_to_page(&tail_page, row_id, row, size) {
                    Ok(()) => {
                        self.touch_page(tail_id);
                        return Ok(tail_id);
                    }
                    Err(returned_row) => row = returned_row,
                }
            }
        }

        // Slow path: re-check under write lock, then register a page if needed.
        let (page_id, page) = {
            let mut row_space = self
                .row_space
                .write()
                .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_space write lock".into()))?;

            if let Some((tail_id, tail_page)) = row_space
                .iter()
                .next_back()
                .map(|(id, page)| (*id, page.clone()))
            {
                match Self::try_append_to_page(&tail_page, row_id, row, size) {
                    Ok(()) => {
                        self.touch_page(tail_id);
                        return Ok(tail_id);
                    }
                    Err(returned_row) => row = returned_row,
                }
            }

            let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
            let page = Arc::new(RwLock::new(Page::new(page_id, self.page_size)));
            self.track_page_in_memory(page_id, &page.read().unwrap());
            row_space.insert(page_id, page.clone());
            (page_id, page)
        };

        Self::force_append_to_page(&page, row_id, row, size)?;
        self.maybe_evict_pages(None)?;
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

    pub fn insert_row(
        &self,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
    ) -> Result<RowID, DataBaseErrors> {
        let row_id = self.next_row_id.fetch_add(1, Ordering::SeqCst) + 1;
        let row = Row::new(transaction, data);
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

    pub fn update_row(
        &self,
        row_id: RowID,
        data: AHashMap<ColumnID, DataBaseDataEntry>,
        transaction: &Transaction,
    ) -> Result<(), DataBaseErrors> {
        let page = self.page_for_row(row_id)?;
        let mut page = page.write().unwrap();
        page.rows
            .get_mut(&row_id)
            .ok_or(DataBaseErrors::RowNotFound(row_id))?
            .update(transaction, data);
        page.is_dirty = true;
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

        let page_ids: BTreeSet<PageID> = self
            .row_locations
            .read()
            .map_err(|_| DataBaseErrors::QueryError("Failed to acquire row_locations read lock".into()))?
            .values()
            .copied()
            .collect();

        for page_id in page_ids {
            let page = self.ensure_page_resident(page_id)?;
            let page_guard = page.read().unwrap();
            for (row_id, row) in page_guard.rows.iter() {
                if let Some(version) = row.get_versioned_row(transaction) {
                    let matches = match &request.filter {
                        Some(filter) => filter.evaluate(&version.data, &visible_columns)?,
                        None => true,
                    };
                    if matches {
                        let row_clone: AHashMap<ColumnID, DataBaseDataEntry> =
                            version.data.iter().map(|(&k, v)| (k, v.clone())).collect();
                        rows.push((*row_id, row_clone));
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

        if let Ok(row_space) = self.row_space.read() {
            for page in row_space.values() {
                page.write().unwrap().rows.retain(|_, row| {
                    row.prune(oldest_active_txn);
                    true
                });
            }
        }

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
            column.write().unwrap().rollback_transaction(transaction);
        }
        if let Ok(row_space) = self.row_space.read() {
            for page in row_space.values() {
                for row in page.write().unwrap().rows.values_mut() {
                    row.rollback_transaction(transaction);
                }
            }
        }
    }

    pub fn replay(self) {
        
    }
}
