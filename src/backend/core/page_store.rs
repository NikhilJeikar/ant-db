use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::backend::core::table::{Page, PageID};
use crate::backend::errors::DataBaseErrors;

const MAGIC: &[u8; 5] = b"ANTPG";
const VERSION: u32 = 1;
/// Total on-disk header size: magic(5) + version(4) + index_offset(8) + index_length(8) + append_offset(8)
const HEADER_SIZE: u64 = 33;

/// Byte range of a single page record in the data section.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct PageLocation {
    offset: u64,
    length: u32,
}

type PageIndex = BTreeMap<PageID, PageLocation>;

/// On-disk header for a page-addressable table file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageFileHeader {
    index_offset: u64,
    index_length: u64,
    append_offset: u64,
}

impl PageFileHeader {
    fn write_to(&self, file: &mut File) -> Result<(), DataBaseErrors> {
        file.seek(SeekFrom::Start(0))
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(MAGIC)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(&VERSION.to_le_bytes())
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(&self.index_offset.to_le_bytes())
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(&self.index_length.to_le_bytes())
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(&self.append_offset.to_le_bytes())
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok(())
    }

    fn read_from(file: &mut File) -> Result<Option<Self>, DataBaseErrors> {
        file.seek(SeekFrom::Start(0))
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let mut magic = [0u8; 5];
        if file.read_exact(&mut magic).is_err() {
            return Ok(None);
        }
        if &magic != MAGIC {
            return Ok(None);
        }
        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let version = u32::from_le_bytes(version_bytes);
        if version != VERSION {
            return Err(DataBaseErrors::DeserializationError(format!(
                "Unsupported page file version {version}"
            )));
        }
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let index_offset = u64::from_le_bytes(buf);
        file.read_exact(&mut buf)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let index_length = u64::from_le_bytes(buf);
        file.read_exact(&mut buf)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let append_offset = u64::from_le_bytes(buf);
        Ok(Some(Self {
            index_offset,
            index_length,
            append_offset,
        }))
    }
}

#[derive(Debug, Default)]
struct PageStoreState {
    index: PageIndex,
    index_offset: u64,
    index_length: u64,
    append_offset: u64,
    loaded: bool,
}

/// On-disk store for evicted table pages. Each table uses a single file named
/// `{database_name}-{table_name}` under the configured table data directory.
///
/// Format (v1):
/// - 33-byte header (magic, version, index location, append cursor)
/// - Page index: bincode `BTreeMap<PageID, PageLocation>`
/// - Data section: append-only bincode-encoded `Page` records
#[derive(Debug)]
pub struct PageStore {
    path: PathBuf,
    state: Mutex<PageStoreState>,
}

impl Default for PageStore {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            state: Mutex::new(PageStoreState::default()),
        }
    }
}

impl PageStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: Mutex::new(PageStoreState::default()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn open_file(read: bool, write: bool) -> OpenOptions {
        let mut options = OpenOptions::new();
        options.create(true).read(read).write(write);
        options
    }

    fn serialize_index(index: &PageIndex) -> Result<Vec<u8>, DataBaseErrors> {
        bincode::serialize(index).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))
    }

    fn deserialize_index(bytes: &[u8]) -> Result<PageIndex, DataBaseErrors> {
        if bytes.is_empty() {
            return Ok(BTreeMap::new());
        }
        bincode::deserialize(bytes).map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))
    }

    fn read_index_bytes(
        file: &mut File,
        header: &PageFileHeader,
    ) -> Result<Vec<u8>, DataBaseErrors> {
        if header.index_length == 0 {
            return Ok(Vec::new());
        }
        file.seek(SeekFrom::Start(header.index_offset))
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let mut bytes = vec![0u8; header.index_length as usize];
        file.read_exact(&mut bytes)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok(bytes)
    }

    fn read_header_and_index(path: &Path) -> Result<Option<(PageFileHeader, PageIndex)>, DataBaseErrors> {
        if !path.exists() {
            return Ok(None);
        }
        let mut file = File::open(path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let Some(header) = PageFileHeader::read_from(&mut file)? else {
            return Ok(None);
        };
        let index_bytes = Self::read_index_bytes(&mut file, &header)?;
        let index = Self::deserialize_index(&index_bytes)?;
        Ok(Some((header, index)))
    }

    fn populate_state(state: &mut PageStoreState, header: PageFileHeader, index: PageIndex) {
        state.index = index;
        state.index_offset = header.index_offset;
        state.index_length = header.index_length;
        state.append_offset = header.append_offset;
        state.loaded = true;
    }

    fn load_state(&self, state: &mut PageStoreState) -> Result<(), DataBaseErrors> {
        if state.loaded {
            return Ok(());
        }

        if !self.path.exists() {
            state.index_offset = HEADER_SIZE;
            state.append_offset = HEADER_SIZE;
            state.loaded = true;
            return Ok(());
        }

        let file_len = fs::metadata(&self.path)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?
            .len();
        if file_len == 0 {
            state.index_offset = HEADER_SIZE;
            state.append_offset = HEADER_SIZE;
            state.loaded = true;
            return Ok(());
        }

        let Some((header, index)) = Self::read_header_and_index(&self.path)? else {
            return Err(DataBaseErrors::DeserializationError(format!(
                "Invalid page file '{}': missing or corrupt ANTPG header",
                self.path.display()
            )));
        };
        Self::populate_state(state, header, index);
        Ok(())
    }

    fn with_state<T>(
        &self,
        f: impl FnOnce(&mut PageStoreState) -> Result<T, DataBaseErrors>,
    ) -> Result<T, DataBaseErrors> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DataBaseErrors::IOError("Page store lock poisoned".into()))?;
        self.load_state(&mut state)?;
        f(&mut state)
    }

    fn read_page_bytes(
        file: &mut File,
        location: PageLocation,
    ) -> Result<Vec<u8>, DataBaseErrors> {
        file.seek(SeekFrom::Start(location.offset))
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let mut bytes = vec![0u8; location.length as usize];
        file.read_exact(&mut bytes)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok(bytes)
    }

    fn write_compact_file(
        path: &Path,
        pages: &BTreeMap<PageID, Page>,
    ) -> Result<(PageFileHeader, PageIndex), DataBaseErrors> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        }

        let tmp_path = path.with_extension("tmp");
        let mut file = Self::open_file(true, true)
            .open(&tmp_path)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;

        PageFileHeader {
            index_offset: HEADER_SIZE,
            index_length: 0,
            append_offset: HEADER_SIZE,
        }
        .write_to(&mut file)?;

        let mut index = PageIndex::new();
        let mut offset = HEADER_SIZE;
        for (page_id, page) in pages {
            let page_bytes =
                bincode::serialize(page).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
            file.write_all(&page_bytes)
                .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
            index.insert(
                *page_id,
                PageLocation {
                    offset,
                    length: page_bytes.len() as u32,
                },
            );
            offset += page_bytes.len() as u64;
        }

        let index_bytes = Self::serialize_index(&index)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        file.write_all(&index_bytes)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;

        let header = PageFileHeader {
            index_offset: offset,
            index_length: index_bytes.len() as u64,
            append_offset: offset + index_bytes.len() as u64,
        };
        header.write_to(&mut file)?;
        file.sync_all()
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        fs::rename(&tmp_path, path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok((header, index))
    }

    /// Return all page IDs known to exist on disk (index only, no page I/O).
    pub fn list_page_ids(&self) -> Result<Vec<PageID>, DataBaseErrors> {
        self.with_state(|state| Ok(state.index.keys().copied().collect()))
    }

    pub fn read_all(&self) -> Result<BTreeMap<PageID, Page>, DataBaseErrors> {
        let page_ids = self.list_page_ids()?;
        let mut pages = BTreeMap::new();
        for page_id in page_ids {
            if let Some(page) = self.read_page(page_id)? {
                pages.insert(page_id, page);
            }
        }
        Ok(pages)
    }

    pub fn write_all(&self, pages: &BTreeMap<PageID, Page>) -> Result<(), DataBaseErrors> {
        let (header, index) = Self::write_compact_file(&self.path, pages)?;
        self.with_state(|state| {
            Self::populate_state(state, header, index);
            Ok(())
        })
    }

    /// Merge one or more pages into the on-disk file using append-only writes.
    pub fn merge_pages(&self, updates: &BTreeMap<PageID, Page>) -> Result<(), DataBaseErrors> {
        if updates.is_empty() {
            return Ok(());
        }

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        }

        let mut file = Self::open_file(true, true)
            .open(&self.path)
            .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;

        self.with_state(|state| {
            let mut append_offset = state.append_offset.max(HEADER_SIZE);

            for (page_id, page) in updates {
                let page_bytes = bincode::serialize(page)
                    .map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
                file.seek(SeekFrom::Start(append_offset))
                    .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
                file.write_all(&page_bytes)
                    .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
                state.index.insert(
                    *page_id,
                    PageLocation {
                        offset: append_offset,
                        length: page_bytes.len() as u32,
                    },
                );
                append_offset += page_bytes.len() as u64;
            }

            let index_bytes = Self::serialize_index(&state.index)?;
            file.seek(SeekFrom::Start(append_offset))
                .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
            file.write_all(&index_bytes)
                .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;

            let header = PageFileHeader {
                index_offset: append_offset,
                index_length: index_bytes.len() as u64,
                append_offset: append_offset + index_bytes.len() as u64,
            };
            header.write_to(&mut file)?;
            file.sync_all()
                .map_err(|e| DataBaseErrors::IOError(e.to_string()))?;

            state.index_offset = header.index_offset;
            state.index_length = header.index_length;
            state.append_offset = header.append_offset;
            state.loaded = true;
            Ok(())
        })
    }

    pub fn read_page(&self, page_id: PageID) -> Result<Option<Page>, DataBaseErrors> {
        let location = self.with_state(|state| Ok(state.index.get(&page_id).copied()))?;
        let Some(location) = location else {
            return Ok(None);
        };

        let mut file = File::open(&self.path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        let page_bytes = Self::read_page_bytes(&mut file, location)?;
        let page: Page = bincode::deserialize(&page_bytes)
            .map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))?;
        Ok(Some(page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::core::table::Page;

    fn temp_page_store(name: &str) -> (PageStore, PathBuf) {
        let dir = std::env::temp_dir().join(format!("ant-db-page-store-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test-table");
        (PageStore::new(path.clone()), path)
    }

    fn sample_page(page_id: PageID) -> Page {
        Page::new_for_test(page_id, 8192)
    }

    #[test]
    fn read_page_fetches_single_record() {
        let (store, path) = temp_page_store("single-read");
        let mut pages = BTreeMap::new();
        for page_id in 1..=20 {
            pages.insert(page_id, sample_page(page_id));
        }
        store.write_all(&pages).unwrap();

        let file_len = fs::metadata(&path).unwrap().len();
        assert!(file_len > 1024, "expected non-trivial page file");

        let page = store.read_page(5).unwrap().expect("page 5 should exist");
        assert_eq!(page.id(), 5);

        let ids = store.list_page_ids().unwrap();
        assert_eq!(ids.len(), 20);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn merge_pages_appends_without_rewriting_existing_records() {
        let (store, path) = temp_page_store("merge");
        let mut first = BTreeMap::new();
        first.insert(1, sample_page(1));
        store.merge_pages(&first).unwrap();

        let mut second = BTreeMap::new();
        second.insert(2, sample_page(2));
        store.merge_pages(&second).unwrap();

        assert_eq!(store.read_page(1).unwrap().unwrap().id(), 1);
        assert_eq!(store.read_page(2).unwrap().unwrap().id(), 2);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
}
