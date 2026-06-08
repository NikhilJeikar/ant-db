use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::error;

use crate::backend::config::InternalStateManager;
use crate::backend::core::table::{Table, TableID};
use crate::backend::core::transaction::{Transaction, TransactionSnapshot};
use crate::backend::errors::DataBaseErrors;

fn serialize_tables<S>(
    tables: &BTreeMap<String, Arc<RwLock<Table>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let mut map = serializer.serialize_map(Some(tables.len()))?;
    for (name, table) in tables.iter() {
        match table.read() {
            Ok(guard) => {
                map.serialize_entry(name, &*guard)?;
            }
            Err(e) => {
                error!(
                    "Failed to acquire read lock on table {} during serialization: {}",
                    name, e
                );
            }
        }
    }
    map.end()
}

fn deserialize_tables<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Arc<RwLock<Table>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let intermediate = BTreeMap::<String, Table>::deserialize(deserializer)?;
    Ok(intermediate
        .into_iter()
        .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
        .collect())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    #[serde(
        serialize_with = "serialize_tables",
        deserialize_with = "deserialize_tables"
    )]
    pub tables: BTreeMap<String, Arc<RwLock<Table>>>,
    pub next_table_id: AtomicU64,
    #[serde(skip)]
    pub internal_state_manager: Arc<RwLock<InternalStateManager>>,
    #[serde(skip)]
    pub transaction_snapshot: Arc<RwLock<TransactionSnapshot>>,
    pub next_transaction_id: AtomicU64,
}

impl Database {
    pub fn new(name: String, internal_state_manager: Arc<RwLock<InternalStateManager>>) -> Self {
        Database {
            name,
            tables: BTreeMap::new(),
            next_table_id: AtomicU64::new(0),
            internal_state_manager,
            transaction_snapshot: Arc::new(RwLock::new(TransactionSnapshot::default())),
            next_transaction_id: AtomicU64::new(1),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bind_tables(&mut self) {
        for table in self.tables.values() {
            if let Ok(mut table) = table.write() {
                table.bind_page_store(&self.name, self.internal_state_manager.clone());
            }
        }
    }

    /// Restore database state from a snapshot file, if one exists.
    pub fn load_from_snapshot(
        path: &str,
        expected_name: &str,
        internal_state_manager: Arc<RwLock<InternalStateManager>>,
    ) -> Result<Option<Self>, DataBaseErrors> {
        if !Path::new(path).exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let mut db: Database = bincode::deserialize(&bytes)
            .map_err(|e| DataBaseErrors::DeserializationError(e.to_string()))?;
        if db.name != expected_name {
            return Err(DataBaseErrors::QueryError(format!(
                "Snapshot database name '{}' does not match configured database '{}'",
                db.name, expected_name
            )));
        }
        db.internal_state_manager = internal_state_manager;
        let last_committed = db
            .next_transaction_id
            .load(Ordering::SeqCst)
            .saturating_sub(1);
        db.transaction_snapshot = Arc::new(RwLock::new(TransactionSnapshot {
            smallest_active_transaction_id: last_committed,
            last_possible_transaction_id: last_committed,
            active_transaction: HashSet::new(),
        }));
        db.bind_tables();
        Ok(Some(db))
    }

    /// Persist the full database metadata and in-memory state to disk.
    pub fn save_snapshot(&self, path: &str) -> Result<(), DataBaseErrors> {
        for table in self.tables.values() {
            if let Ok(table) = table.read() {
                table.flush_all_pages()?;
            }
        }
        let bytes = bincode::serialize(self)
            .map_err(|e| DataBaseErrors::SerializationError(e.to_string()))?;
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
            }
        }
        let tmp_path = Path::new(path).with_extension("tmp");
        fs::write(&tmp_path, bytes).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        fs::rename(&tmp_path, path).map_err(|e| DataBaseErrors::IOError(e.to_string()))?;
        Ok(())
    }

    pub fn create_table(&mut self, table_name: String) -> Result<TableID, DataBaseErrors> {
        if self.tables.get(&table_name).is_none() {
            let table_id = self.next_table_id.fetch_add(1, Ordering::SeqCst);
            self.tables.insert(
                table_name.clone(),
                Arc::new(RwLock::new(Table::new(
                    table_id,
                    self.name.clone(),
                    table_name.clone(),
                    self.internal_state_manager.clone(),
                ))),
            );
            return Ok(table_id);
        }
        Err(DataBaseErrors::TableAlreadyExists(table_name.clone()))
    }

    pub fn drop_table(&mut self, table_name: String) {
        self.tables.remove(&table_name);
    }

    pub fn search(
        &self,
        request: &crate::backend::core::search::SearchRequest,
        transaction: &Transaction,
    ) -> Result<Vec<crate::backend::core::search::SearchResult>, DataBaseErrors> {
        crate::backend::core::query::execute_search(self, request, transaction)
    }

    pub fn foreign_key_value_exists(
        &self,
        refered_table_id: TableID,
        refered_column_id: crate::backend::core::column::ColumnID,
        value: &crate::backend::core::row::DataBaseDataEntry,
        transaction: &Transaction,
    ) -> Result<bool, DataBaseErrors> {
        if value.is_null() {
            return Ok(true);
        }

        for table in self.tables.values() {
            let table_guard = table.read().map_err(|_| {
                DataBaseErrors::QueryError("Failed to acquire table read lock".into())
            })?;
            if table_guard.table_id() != refered_table_id {
                continue;
            }
            return table_guard.referenced_value_exists(refered_column_id, value, transaction);
        }

        Ok(false)
    }

    pub fn get_table(&self, table_name: String) -> Option<Arc<RwLock<Table>>> {
        if let Some(table) = self.tables.get(&table_name) {
            return Some(table.clone());
        }

        let requested_name = table_name.to_ascii_lowercase();
        self.tables
            .iter()
            .find(|(key, _)| key.to_ascii_lowercase() == requested_name)
            .map(|(_, table)| table.clone())
    }

    pub fn create_transaction(&mut self) -> Transaction {
        let transaction_id = self.next_transaction_id.fetch_add(1, Ordering::SeqCst);
        self.transaction_snapshot
            .write()
            .unwrap()
            .active_transaction
            .insert(transaction_id);
        Transaction {
            transaction_id,
            snapshot: self.transaction_snapshot.clone(),
        }
    }

    pub fn commit_transaction(&mut self, transaction_id: u64) {
        self.transaction_snapshot
            .write()
            .unwrap()
            .active_transaction
            .remove(&transaction_id);
        self.transaction_snapshot
            .write()
            .unwrap()
            .last_possible_transaction_id = transaction_id;
        if self
            .transaction_snapshot
            .read()
            .unwrap()
            .active_transaction
            .is_empty()
        {
            self.transaction_snapshot
                .write()
                .unwrap()
                .smallest_active_transaction_id = transaction_id;
        } else {
            let smallest_active = self
                .transaction_snapshot
                .read()
                .unwrap()
                .active_transaction
                .iter()
                .min()
                .copied()
                .unwrap_or(transaction_id);
            self.transaction_snapshot
                .write()
                .unwrap()
                .smallest_active_transaction_id = smallest_active;
        }
    }

    pub fn rollback_transaction(&mut self, transaction: &Transaction) {
        self.transaction_snapshot
            .write()
            .unwrap()
            .active_transaction
            .remove(&transaction.transaction_id);

        for table in self.tables.values() {
            table.write().unwrap().rollback_transaction(transaction);
        }

        let smallest_active = {
            let snapshot = self.transaction_snapshot.read().unwrap();
            if snapshot.active_transaction.is_empty() {
                snapshot.last_possible_transaction_id
            } else {
                snapshot.active_transaction.iter().min().copied().unwrap_or_else(|| {
                    snapshot.last_possible_transaction_id
                })
            }
        };
        self.transaction_snapshot
            .write()
            .unwrap()
            .smallest_active_transaction_id = smallest_active;
    }

    pub fn remove_stray_tables(&self) {
        let oldest_active = self
            .transaction_snapshot
            .read()
            .unwrap()
            .smallest_active_transaction_id;
        for (_, table) in self.tables.iter() {
            table.write().unwrap().prune(oldest_active);
        }
    }

    pub fn auto_vacuum(&self) {
        self.remove_stray_tables();
        for table in self.tables.values() {
            if let Ok(table) = table.read() {
                if let Err(err) = table.flush_all_pages() {
                    error!(
                        "Failed to flush table '{}' during auto vacuum: {err}",
                        table.name()
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, RwLock};

    use ahash::AHashMap;

    use crate::backend::config::{Config, InternalStateManager};
    use crate::backend::core::column::DataBaseDataType;
    use crate::backend::core::row::DataBaseDataEntry;

    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    use super::Database;
    use crate::backend::core::page_store::PageStore;

    #[test]
    fn dirty_pages_are_written_on_flush() {
        let dir = std::env::temp_dir().join(format!("ant-db-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        table
            .write()
            .unwrap()
            .create_column(
                "id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let mut data = AHashMap::new();
        data.insert(0, DataBaseDataEntry::IntegerU64(42));
        table
            .read()
            .unwrap()
            .insert_row(data, &txn, |_, _, _| Ok(true))
            .unwrap();
        db.commit_transaction(txn.transaction_id);
        table
            .read()
            .unwrap()
            .flush_all_pages()
            .expect("flush should write dirty pages to disk");

        let expected = dir.join("testdb-users");
        assert!(
            expected.exists(),
            "expected table page file at {}",
            expected.display()
        );
        assert!(expected.metadata().unwrap().len() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_roundtrip_restores_tables() {
        let dir = std::env::temp_dir().join(format!("ant-db-snapshot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let snapshot_path = dir.join("snapshot.db");
        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();
        config.snapshot_path = snapshot_path.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config.clone())));
        let mut db = Database::new("testdb".to_string(), ism.clone());
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        table
            .write()
            .unwrap()
            .create_column(
                "id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let mut data = AHashMap::new();
        data.insert(0, DataBaseDataEntry::IntegerU64(42));
        table
            .read()
            .unwrap()
            .insert_row(data, &txn, |_, _, _| Ok(true))
            .unwrap();
        db.commit_transaction(txn.transaction_id);

        db.save_snapshot(&snapshot_path.to_string_lossy()).unwrap();

        let page_file = PathBuf::from(&config.table_data_path).join("testdb-users");
        assert!(page_file.exists(), "table page file should exist after snapshot save");
        let page_store = PageStore::new(page_file);
        let page_ids = page_store.list_page_ids().unwrap();
        assert!(!page_ids.is_empty(), "table page file should contain flushed pages");
        let flushed_page = page_store
            .read_page(page_ids[0])
            .unwrap()
            .expect("first flushed page should be readable");
        assert!(
            flushed_page.has_rows(),
            "flushed page should contain row data"
        );

        let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();
        assert!(
            snapshot_bytes.len() < 4096,
            "metadata-only snapshot should stay small, got {} bytes",
            snapshot_bytes.len()
        );

        let mut loaded = Database::load_from_snapshot(
            &snapshot_path.to_string_lossy(),
            "testdb",
            ism,
        )
        .unwrap()
        .expect("snapshot should load");

        assert!(loaded.get_table("users".to_string()).is_some());
        assert_eq!(loaded.tables.len(), 1);
        assert_eq!(
            loaded.next_transaction_id.load(Ordering::SeqCst),
            2,
            "snapshot must preserve next transaction id"
        );

        let loaded_table = loaded.get_table("users".to_string()).unwrap();
        assert_eq!(
            loaded_table.read().unwrap().row_location_count(),
            1,
            "row locations should be rebuilt from page file"
        );

        let verify_txn = loaded.create_transaction();
        let results = loaded_table
            .read()
            .unwrap()
            .search(
                &crate::backend::core::search::SearchRequest::single_table(
                    "users".to_string(),
                    Some(vec!["id".to_string()]),
                    None,
                    vec![],
                    None,
                    None,
                ),
                &verify_txn,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].values.get("id"),
            Some(&DataBaseDataEntry::IntegerU64(42))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_removes_unique_index_entry() {
        use crate::backend::core::column::Constraint;

        let dir = std::env::temp_dir().join(format!("ant-db-delete-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        let mut constraints = BTreeSet::new();
        constraints.insert(Constraint::Unique);
        let email_col = table
            .write()
            .unwrap()
            .create_column(
                "email".to_string(),
                DataBaseDataType::String,
                constraints,
                &txn,
            )
            .unwrap();

        let mut data = AHashMap::new();
        data.insert(email_col, DataBaseDataEntry::String("a@b.c".to_string()));
        let row_id = table
            .read()
            .unwrap()
            .insert_row(data, &txn, |_, _, _| Ok(true))
            .unwrap();
        db.commit_transaction(txn.transaction_id);

        let delete_txn = db.create_transaction();
        table
            .read()
            .unwrap()
            .delete_row(row_id, &delete_txn)
            .unwrap();
        db.commit_transaction(delete_txn.transaction_id);

        let insert_txn = db.create_transaction();
        let mut data = AHashMap::new();
        data.insert(email_col, DataBaseDataEntry::String("a@b.c".to_string()));
        table
            .read()
            .unwrap()
            .insert_row(data, &insert_txn, |_, _, _| Ok(true))
            .expect("reinsert after delete should not hit stale unique index entry");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rolled_back_delete_preserves_unique_index_entry() {
        use crate::backend::core::column::Constraint;

        let dir = std::env::temp_dir().join(format!("ant-db-rollback-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        let mut constraints = BTreeSet::new();
        constraints.insert(Constraint::Unique);
        let email_col = table
            .write()
            .unwrap()
            .create_column(
                "email".to_string(),
                DataBaseDataType::String,
                constraints,
                &txn,
            )
            .unwrap();

        let mut data = AHashMap::new();
        data.insert(email_col, DataBaseDataEntry::String("a@b.c".to_string()));
        let row_id = table
            .read()
            .unwrap()
            .insert_row(data, &txn, |_, _, _| Ok(true))
            .unwrap();
        db.commit_transaction(txn.transaction_id);

        let delete_txn = db.create_transaction();
        table
            .read()
            .unwrap()
            .delete_row(row_id, &delete_txn)
            .unwrap();
        db.rollback_transaction(&delete_txn);

        let insert_txn = db.create_transaction();
        let mut data = AHashMap::new();
        data.insert(email_col, DataBaseDataEntry::String("a@b.c".to_string()));
        let err = table
            .read()
            .unwrap()
            .insert_row(data, &insert_txn, |_, _, _| Ok(true))
            .unwrap_err();
        assert!(
            matches!(err, crate::backend::errors::DataBaseErrors::UniqueConstraint(_)),
            "rolled back delete should leave the unique index intact, got {err:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_column_returns_error_for_missing_column() {
        let ism = Arc::new(RwLock::new(InternalStateManager::new(Config::default())));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        let err = table
            .write()
            .unwrap()
            .drop_column(99, &txn)
            .unwrap_err();
        assert!(
            matches!(err, crate::backend::errors::DataBaseErrors::ColumnNotFound(99)),
            "expected ColumnNotFound, got {err:?}"
        );
    }

    #[test]
    fn foreign_key_enforces_referential_integrity() {
        use crate::backend::core::column::Constraint;

        let dir = std::env::temp_dir().join(format!("ant-db-fk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        let users_table_id = db.create_table("users".to_string()).unwrap();
        let orders_table_id = db.create_table("orders".to_string()).unwrap();
        assert_ne!(users_table_id, orders_table_id);

        let txn = db.create_transaction();
        let users = db.get_table("users".to_string()).unwrap();
        let orders = db.get_table("orders".to_string()).unwrap();

        let user_id_col = users
            .write()
            .unwrap()
            .create_column(
                "id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let mut order_constraints = BTreeSet::new();
        order_constraints.insert(Constraint::ForeignKey {
            refered_table_id: users_table_id,
            refered_column_id: user_id_col,
        });
        let order_user_col = orders
            .write()
            .unwrap()
            .create_column(
                "user_id".to_string(),
                DataBaseDataType::IntegerU64,
                order_constraints,
                &txn,
            )
            .unwrap();

        let mut user_row = AHashMap::new();
        user_row.insert(user_id_col, DataBaseDataEntry::IntegerU64(1));
        users
            .read()
            .unwrap()
            .insert_row(user_row, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .unwrap();

        let mut valid_order = AHashMap::new();
        valid_order.insert(order_user_col, DataBaseDataEntry::IntegerU64(1));
        orders
            .read()
            .unwrap()
            .insert_row(valid_order, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .unwrap();

        let mut invalid_order = AHashMap::new();
        invalid_order.insert(order_user_col, DataBaseDataEntry::IntegerU64(99));
        let err = orders
            .read()
            .unwrap()
            .insert_row(invalid_order, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .unwrap_err();
        assert!(
            matches!(err, crate::backend::errors::DataBaseErrors::ForeignKeyViolation(_)),
            "expected ForeignKeyViolation, got {err:?}"
        );

        let mut null_order = AHashMap::new();
        null_order.insert(order_user_col, DataBaseDataEntry::Null);
        orders
            .read()
            .unwrap()
            .insert_row(null_order, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .expect("nullable foreign keys should accept NULL");

        db.commit_transaction(txn.transaction_id);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn foreign_key_works_without_referenced_index() {
        use crate::backend::core::column::Constraint;

        let dir = std::env::temp_dir().join(format!("ant-db-fk-scan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        let users_table_id = db.create_table("users".to_string()).unwrap();
        db.create_table("orders".to_string()).unwrap();

        let txn = db.create_transaction();
        let users = db.get_table("users".to_string()).unwrap();
        let orders = db.get_table("orders".to_string()).unwrap();

        // No PRIMARY KEY / UNIQUE: FK lookup must fall back to a full scan.
        let user_id_col = users
            .write()
            .unwrap()
            .create_column(
                "id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let mut order_constraints = BTreeSet::new();
        order_constraints.insert(Constraint::ForeignKey {
            refered_table_id: users_table_id,
            refered_column_id: user_id_col,
        });
        let order_user_col = orders
            .write()
            .unwrap()
            .create_column(
                "user_id".to_string(),
                DataBaseDataType::IntegerU64,
                order_constraints,
                &txn,
            )
            .unwrap();

        let mut user_row = AHashMap::new();
        user_row.insert(user_id_col, DataBaseDataEntry::IntegerU64(7));
        users
            .read()
            .unwrap()
            .insert_row(user_row, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .unwrap();

        let mut order_row = AHashMap::new();
        order_row.insert(order_user_col, DataBaseDataEntry::IntegerU64(7));
        orders
            .read()
            .unwrap()
            .insert_row(order_row, &txn, |table_id, column_id, value| {
                db.foreign_key_value_exists(table_id, column_id, value, &txn)
            })
            .expect("FK should resolve via table scan when reference column is not indexed");

        db.commit_transaction(txn.transaction_id);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn check_constraint_rejects_non_null_values() {
        use crate::backend::core::column::Constraint;

        let dir = std::env::temp_dir().join(format!("ant-db-check-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("scores".to_string()).unwrap();

        let txn = db.create_transaction();
        let scores = db.get_table("scores".to_string()).unwrap();
        let mut constraints = BTreeSet::new();
        constraints.insert(Constraint::Check);
        let score_col = scores
            .write()
            .unwrap()
            .create_column(
                "value".to_string(),
                DataBaseDataType::IntegerU64,
                constraints,
                &txn,
            )
            .unwrap();

        let mut null_row = AHashMap::new();
        null_row.insert(score_col, DataBaseDataEntry::Null);
        scores
            .read()
            .unwrap()
            .insert_row(null_row, &txn, |_, _, _| Ok(true))
            .expect("NULL should bypass unsupported CHECK enforcement");

        let mut row = AHashMap::new();
        row.insert(score_col, DataBaseDataEntry::IntegerU64(1));
        let err = scores
            .read()
            .unwrap()
            .insert_row(row, &txn, |_, _, _| Ok(true))
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::backend::errors::DataBaseErrors::CheckConstraintUnsupported(_)
            ),
            "expected CheckConstraintUnsupported, got {err:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn inner_join_returns_matching_rows() {
        use crate::backend::core::search::SearchRequest;

        let dir = std::env::temp_dir().join(format!("ant-db-join-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();
        db.create_table("orders".to_string()).unwrap();

        let txn = db.create_transaction();
        let users = db.get_table("users".to_string()).unwrap();
        let orders = db.get_table("orders".to_string()).unwrap();

        let user_id_col = users
            .write()
            .unwrap()
            .create_column(
                "id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let user_name_col = users
            .write()
            .unwrap()
            .create_column(
                "name".to_string(),
                DataBaseDataType::String,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let order_user_col = orders
            .write()
            .unwrap()
            .create_column(
                "user_id".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();
        let order_amount_col = orders
            .write()
            .unwrap()
            .create_column(
                "amount".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();

        let mut alice = AHashMap::new();
        alice.insert(user_id_col, DataBaseDataEntry::IntegerU64(1));
        alice.insert(user_name_col, DataBaseDataEntry::String("alice".to_string()));
        users.read().unwrap().insert_row(alice, &txn, |_, _, _| Ok(true)).unwrap();

        let mut bob = AHashMap::new();
        bob.insert(user_id_col, DataBaseDataEntry::IntegerU64(2));
        bob.insert(user_name_col, DataBaseDataEntry::String("bob".to_string()));
        users.read().unwrap().insert_row(bob, &txn, |_, _, _| Ok(true)).unwrap();

        let mut order = AHashMap::new();
        order.insert(order_user_col, DataBaseDataEntry::IntegerU64(1));
        order.insert(order_amount_col, DataBaseDataEntry::IntegerU64(99));
        orders
            .read()
            .unwrap()
            .insert_row(order, &txn, |_, _, _| Ok(true))
            .unwrap();

        db.commit_transaction(txn.transaction_id);

        let query_txn = db.create_transaction();
        let request = SearchRequest::from_sql(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name",
        )
        .unwrap();
        let results = db.search(&request, &query_txn).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].values.get("name"),
            Some(&DataBaseDataEntry::String("alice".to_string()))
        );
        assert_eq!(
            results[0].values.get("amount"),
            Some(&DataBaseDataEntry::IntegerU64(99))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn insert_rejects_wrong_column_type() {
        let dir = std::env::temp_dir().join(format!("ant-db-type-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = Config::default();
        config.database = "testdb".to_string();
        config.table_data_path = dir.to_string_lossy().to_string();

        let ism = Arc::new(RwLock::new(InternalStateManager::new(config)));
        let mut db = Database::new("testdb".to_string(), ism);
        db.create_table("users".to_string()).unwrap();

        let txn = db.create_transaction();
        let table = db.get_table("users".to_string()).unwrap();
        let age_col = table
            .write()
            .unwrap()
            .create_column(
                "age".to_string(),
                DataBaseDataType::IntegerU64,
                BTreeSet::new(),
                &txn,
            )
            .unwrap();

        let mut data = AHashMap::new();
        data.insert(age_col, DataBaseDataEntry::String("not-a-number".to_string()));
        let err = table
            .read()
            .unwrap()
            .insert_row(data, &txn, |_, _, _| Ok(true))
            .unwrap_err();
        assert!(
            matches!(err, crate::backend::errors::DataBaseErrors::DataTypeMismatch(_, _, _)),
            "expected DataTypeMismatch, got {err:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
