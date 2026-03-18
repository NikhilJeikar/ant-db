use crate::backend::config::InternalStateManager;
use crate::backend::core::database::InternalDatabaseSchema;
use crate::backend::errors::DataBaseErrors;
use crate::backend::storage::wal::WriteAheadLogManager;
use rmp_serde::from_slice;
use rmp_serde::to_vec;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info};

pub fn write_snapshot(db: &InternalDatabaseSchema, path: &str) -> Result<(), DataBaseErrors> {
    info!("Serializing database snapshot to {}", path);
    let bytes = to_vec(db).map_err(|e| {
        error!("Failed to serialize database: {}", e);
        DataBaseErrors::SerializationError(e.to_string())
    })?;
    info!("Serialized database to {} bytes", bytes.len());

    fs::write(path, bytes).map_err(|e| {
        error!("Failed to write snapshot file {}: {}", path, e);
        DataBaseErrors::IOError(e.to_string())
    })?;

    info!("Snapshot successfully written to {}", path);
    Ok(())
}

pub fn read_snapshot_with_context(
    path: &str,
    internal_state_manager: Arc<RwLock<InternalStateManager>>,
    wal_manager: Arc<Mutex<WriteAheadLogManager>>,
) -> Result<InternalDatabaseSchema, DataBaseErrors> {
    info!("Reading snapshot from {}", path);

    let snapshot_bytes = std::fs::read(path).map_err(|e| {
        error!("Failed to read snapshot file {}: {}", path, e);
        DataBaseErrors::IOError(e.to_string())
    })?;
    info!("Snapshot file read: {} bytes", snapshot_bytes.len());

    let mut db: InternalDatabaseSchema = from_slice(&snapshot_bytes).map_err(|e| {
        error!("Failed to deserialize snapshot: {}", e);
        DataBaseErrors::DeserializationError(e.to_string())
    })?;
    debug!("Snapshot deserialized successfully");

    // Inject the proper contexts into the deserialized database AND all its tables
    info!("Injecting contexts into database schema");
    db.inject_contexts(internal_state_manager, wal_manager);
    info!("Snapshot restored with contexts");

    Ok(db)
}

/// Starts a background thread that monitors WAL size and triggers snapshots when threshold is exceeded.
/// Returns a shutdown flag that can be used to stop the monitor thread.
pub fn start_snapshot_monitor(
    wal_manager: Arc<std::sync::Mutex<WriteAheadLogManager>>,
    db: Arc<std::sync::RwLock<InternalDatabaseSchema>>,
    check_interval_secs: u64,
) -> Arc<AtomicBool> {
    let should_stop = Arc::new(AtomicBool::new(false));
    let should_stop_clone = should_stop.clone();

    thread::spawn(move || {
        info!(
            "Snapshot monitor thread started with check interval: {}s",
            check_interval_secs
        );

        loop {
            if should_stop_clone.load(Ordering::Relaxed) {
                info!("Shutdown signal received, stopping snapshot monitor");
                break;
            }

            thread::sleep(Duration::from_secs(check_interval_secs));
            debug!("Snapshot monitor check triggered");

            let wal_size = {
                debug!("Attempting to acquire WAL lock to read size");
                match wal_manager.lock() {
                    Ok(wal) => {
                        let size = wal.get_wal_size();
                        debug!("WAL lock acquired, size: {} bytes", size);
                        size
                    }
                    Err(e) => {
                        error!("Failed to acquire WAL lock for size check: {}", e);
                        continue;
                    }
                }
            };

            let threshold = {
                debug!("Attempting to acquire WAL lock to read threshold");
                match wal_manager.lock() {
                    Ok(wal) => {
                        let thresh = wal.config.wal_threshold as u64;
                        debug!("WAL lock acquired, threshold: {} bytes", thresh);
                        thresh
                    }
                    Err(e) => {
                        error!("Failed to read WAL threshold: {}", e);
                        continue;
                    }
                }
            };

            info!(
                "Snapshot monitor: WAL size: {} bytes, Threshold: {} bytes",
                wal_size, threshold
            );

            if wal_size > threshold {
                info!(
                    "WAL size ({} bytes) exceeds threshold ({} bytes), triggering snapshot",
                    wal_size, threshold
                );

                // Step 1: Briefly acquire read lock, clone DB, and release lock
                debug!("Attempting to acquire database read lock");
                let db_clone = match db.read() {
                    Ok(db_guard) => {
                        info!("Database read lock acquired for snapshot copy");
                        let cloned = db_guard.clone();
                        info!("Database snapshot cloned to memory");
                        cloned
                    }
                    Err(e) => {
                        error!("Failed to acquire database read lock: {}", e);
                        continue;
                    }
                };
                // Lock is automatically released here
                info!("Database read lock released");

                // Step 2: Serialize to a temporary file outside the lock
                // This can take a long time without blocking DB operations
                let snap_path = {
                    debug!("Acquiring WAL lock to read snapshot path");
                    match wal_manager.lock() {
                        Ok(wal) => {
                            let path = wal.config.snapshot_path.clone();
                            debug!("Snapshot path retrieved: {}", path);
                            path
                        }
                        Err(e) => {
                            error!("Failed to read snapshot path: {}", e);
                            continue;
                        }
                    }
                };

                let temp_path = format!("{}.tmp", snap_path);
                info!("Serializing database to temporary file: {}", temp_path);

                match write_snapshot(&db_clone, &temp_path) {
                    Ok(_) => {
                        info!("Snapshot serialization completed");

                        // Step 3: Briefly re-acquire locks to swap files and clear WAL
                        debug!("Attempting to acquire WAL lock for finalization");
                        match wal_manager.lock() {
                            Ok(mut wal_guard) => {
                                debug!("WAL lock acquired for file swap");
                                match std::fs::rename(&temp_path, &snap_path) {
                                    Ok(_) => {
                                        info!(
                                            "Snapshot file moved from {} to {}",
                                            temp_path, snap_path
                                        );
                                        match wal_guard.clear_wal() {
                                            Ok(_) => {
                                                info!(
                                                    "Snapshot completed: file moved and WAL cleared"
                                                );
                                            }
                                            Err(e) => {
                                                error!("Failed to clear WAL after snapshot: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to move snapshot file from {} to {}: {}",
                                            temp_path, snap_path, e
                                        );
                                        let _ = std::fs::remove_file(&temp_path);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to acquire WAL lock for finalization: {}", e);
                                let _ = std::fs::remove_file(&temp_path);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize database: {}", e);
                        let _ = std::fs::remove_file(&temp_path);
                    }
                }
            }
        }

        info!("Snapshot monitor thread stopped");
    });

    should_stop
}
