use crate::backend::config::InternalStateManager;
use crate::backend::core::database::InternalDatabaseSchema;
use crate::backend::errors::DataBaseErrors;
use crate::backend::storage::wal::WALManager;
use rmp_serde::from_slice;
use rmp_serde::to_vec;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use tracing::{error, info};

pub fn write_snapshot(db: &InternalDatabaseSchema, path: &str) -> Result<(), DataBaseErrors> {
    let bytes = to_vec(db).map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
    fs::write(path, bytes).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
    Ok(())
}

pub fn read_snapshot_with_context(
    path: &str,
    internal_state_manager: Arc<RwLock<InternalStateManager>>,
    wal_manager: Arc<Mutex<WALManager>>,
) -> Result<InternalDatabaseSchema, DataBaseErrors> {
    let mut db: InternalDatabaseSchema =
        from_slice(&std::fs::read(path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?)
            .map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;

    // Inject the proper contexts into the deserialized database AND all its tables
    db.inject_contexts(internal_state_manager, wal_manager);

    Ok(db)
}

/// Starts a background thread that monitors WAL size and triggers snapshots when threshold is exceeded.
/// Returns a shutdown flag that can be used to stop the monitor thread.
pub fn start_snapshot_monitor(
    wal_manager: Arc<std::sync::Mutex<WALManager>>,
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

        while !should_stop_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(check_interval_secs));

            let wal_size = {
                match wal_manager.lock() {
                    Ok(wal) => wal.get_wal_size(),
                    Err(e) => {
                        error!("Failed to acquire WAL lock: {}", e);
                        continue;
                    }
                }
            };

            let threshold = {
                match wal_manager.lock() {
                    Ok(wal) => wal.config.wal_threshold as u64,
                    Err(e) => {
                        error!("Failed to read WAL threshold: {}", e);
                        continue;
                    }
                }
            };
            info!(
                "Current WAL size: {} bytes, Threshold: {} bytes",
                wal_size, threshold
            );

            if wal_size > threshold {
                info!(
                    "WAL size ({} bytes) exceeds threshold ({} bytes), triggering snapshot",
                    wal_size, threshold
                );

                // Step 1: Briefly acquire read lock, clone DB, and release lock
                let db_clone = match db.read() {
                    Ok(db_guard) => {
                        info!("Database read lock acquired for snapshot copy");
                        db_guard.clone()
                    }
                    Err(e) => {
                        error!("Failed to acquire database read lock: {}", e);
                        continue;
                    }
                };
                // Lock is automatically released here

                // Step 2: Serialize to a temporary file outside the lock
                // This can take a long time without blocking DB operations
                let snap_path = {
                    match wal_manager.lock() {
                        Ok(wal) => wal.config.snapshot_path.clone(),
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
                        match wal_manager.lock() {
                            Ok(mut wal_guard) => match std::fs::rename(&temp_path, &snap_path) {
                                Ok(_) => {
                                    info!("Snapshot file moved to final location");
                                    match wal_guard.clear_wal() {
                                        Ok(_) => {
                                            info!("Snapshot completed successfully, WAL cleared");
                                        }
                                        Err(e) => {
                                            error!("Failed to clear WAL: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to move snapshot file: {}", e);
                                    let _ = std::fs::remove_file(&temp_path);
                                }
                            },
                            Err(e) => {
                                error!("Failed to acquire WAL lock for finalization: {}", e);
                                let _ = std::fs::remove_file(&temp_path);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize database snapshot: {}", e);
                        let _ = std::fs::remove_file(&temp_path);
                    }
                }
            }
        }

        info!("Snapshot monitor thread stopped");
    });

    should_stop
}
