use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::backend::core::table::{Page, PageID};
use crate::backend::errors::DataBaseErrors;

/// On-disk store for evicted table pages. Each table uses a single file named
/// `{database_name}-{table_name}` under the configured table data directory.
#[derive(Debug, Default)]
pub struct PageStore {
    path: PathBuf,
}

impl PageStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_all(&self) -> Result<BTreeMap<PageID, Page>, DataBaseErrors> {
        if !self.path.exists() {
            return Ok(BTreeMap::new());
        }
        let bytes = fs::read(&self.path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        if bytes.is_empty() {
            return Ok(BTreeMap::new());
        }
        bincode::deserialize(&bytes)
            .map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))
    }

    pub fn write_all(&self, pages: &BTreeMap<PageID, Page>) -> Result<(), DataBaseErrors> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        }
        let bytes =
            bincode::serialize(pages).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, bytes).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        fs::rename(&tmp_path, &self.path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok(())
    }

    pub fn upsert_page(&self, page: &Page) -> Result<(), DataBaseErrors> {
        let mut pages = self.read_all()?;
        pages.insert(page.id(), page.clone());
        self.write_all(&pages)
    }

    pub fn read_page(&self, page_id: PageID) -> Result<Option<Page>, DataBaseErrors> {
        Ok(self.read_all()?.get(&page_id).cloned())
    }
}
