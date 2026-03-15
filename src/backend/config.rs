use serde::Deserialize;
use std::fs;


#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub wal_path: String,
    pub snapshot_path: String,
    pub wal_threshold: usize,
    pub log_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal_path: "wal.log".to_string(),
            snapshot_path: "snapshot.db".to_string(),
            wal_threshold: 1024 * 1024,
            log_path: "app.log".to_string(),
        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> Self {
        match fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => Config::default(),
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