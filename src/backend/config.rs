use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Default per-page capacity in bytes used when the config does not specify one.
pub const DEFAULT_PAGE_SIZE_BYTES: u64 = 8 * 1024;

/// Default in-memory budget per table. Zero disables eviction.
pub const DEFAULT_TABLE_MEMORY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// Default interval between automatic vacuum passes, in seconds.
pub const DEFAULT_AUTO_VACUUM_INTERVAL_SECS: u64 = 300;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_path: String,
    pub snapshot_path: String,
    pub wal_path: String,
    pub wal_threshold: usize,
    pub wal_sync_interval: u64,
    /// Name of the database instance served by this process.
    pub database: String,
    /// Directory where per-table page files are stored.
    #[serde(default = "default_table_data_path")]
    pub table_data_path: String,
    /// Maximum number of bytes a single data page may hold. Rows are appended to
    /// a page until adding the next row would exceed this budget, at which point
    /// a fresh page is opened. A single row larger than this value is stored in a
    /// dedicated overflow chain.
    #[serde(default = "default_page_size_bytes")]
    pub page_size_bytes: u64,
    /// Maximum in-memory footprint for a single table's resident pages. When
    /// exceeded, least-recently-used pages are evicted to disk. Zero disables
    /// eviction.
    #[serde(default = "default_table_memory_limit_bytes")]
    pub table_memory_limit_bytes: u64,
    /// How often the background auto-vacuum task runs, in seconds.
    #[serde(default = "default_auto_vacuum_interval_secs")]
    pub auto_vacuum_interval_secs: u64,
}

fn default_table_data_path() -> String {
    "db/tables".to_string()
}

fn default_page_size_bytes() -> u64 {
    DEFAULT_PAGE_SIZE_BYTES
}

fn default_table_memory_limit_bytes() -> u64 {
    DEFAULT_TABLE_MEMORY_LIMIT_BYTES
}

fn default_auto_vacuum_interval_secs() -> u64 {
    DEFAULT_AUTO_VACUUM_INTERVAL_SECS
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal_path: "wal.log".to_string(),
            snapshot_path: "snapshot.db".to_string(),
            wal_threshold: 1024 * 1024,
            log_path: "app1.log".to_string(),
            wal_sync_interval: 60,
            database: String::new(),
            table_data_path: default_table_data_path(),
            page_size_bytes: DEFAULT_PAGE_SIZE_BYTES,
            table_memory_limit_bytes: DEFAULT_TABLE_MEMORY_LIMIT_BYTES,
            auto_vacuum_interval_secs: DEFAULT_AUTO_VACUUM_INTERVAL_SECS,
        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> Self {
        let config = match fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(e) => {
                eprintln!(
                    "Warning: Failed to read config file '{}': {}, using default configuration",
                    path, e
                );
                Config::default()
            }
        };

        // Create parent directories for log_path, wal_path, and snapshot_path
        config.ensure_paths_exist();
        config
    }

    pub fn ensure_paths_exist(&self) {
        // Create parent directory for wal_path
        if let Some(parent) = Path::new(&self.wal_path).parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(parent) {
                    eprintln!(
                        "Warning: Failed to create WAL directory '{}': {}",
                        parent.display(),
                        e
                    );
                }
            }
        }

        // Create parent directory for snapshot_path
        if let Some(parent) = Path::new(&self.snapshot_path).parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(parent) {
                    eprintln!(
                        "Warning: Failed to create snapshot directory '{}': {}",
                        parent.display(),
                        e
                    );
                }
            }
        }

        // Create parent directory for log_path
        if let Some(parent) = Path::new(&self.log_path).parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = fs::create_dir_all(parent) {
                    eprintln!(
                        "Warning: Failed to create log directory '{}': {}",
                        parent.display(),
                        e
                    );
                }
            }
        }

        if let Err(e) = fs::create_dir_all(&self.table_data_path) {
            eprintln!(
                "Warning: Failed to create table data directory '{}': {}",
                self.table_data_path, e
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct InternalStateManager {
    pub config: Config,
    pub is_wal_replaying: bool,
}

impl Default for InternalStateManager {
    fn default() -> Self {
        let config = Config::default();
        Self {
            config,
            is_wal_replaying: false,
        }
    }
}

impl InternalStateManager {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            is_wal_replaying: false,
        }
    }
}
