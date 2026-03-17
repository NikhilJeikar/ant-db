use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_path: String,
    pub snapshot_path: String,
    pub wal_path: String,
    pub wal_threshold: usize,
    pub wal_sync_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal_path: "wal.log".to_string(),
            snapshot_path: "snapshot.db".to_string(),
            wal_threshold: 1024 * 1024,
            log_path: "app1.log".to_string(),
            wal_sync_interval: 60,
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
