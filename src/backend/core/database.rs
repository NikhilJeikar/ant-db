use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
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
    let export: BTreeMap<String, Table> = tables
        .iter()
        .filter_map(|(k, v)| match v.read() {
            Ok(guard) => Some((k.clone(), guard.clone())),
            Err(e) => {
                error!(
                    "Failed to acquire read lock on table {} during serialization: {}",
                    k, e
                );
                None
            }
        })
        .collect();
    export.serialize(serializer)
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
                table.bind_page_store(&self.name);
            }
        }
    }

    fn persist_all_dirty_pages(&self) {
        for table in self.tables.values() {
            if let Ok(table) = table.read() {
                if let Err(err) = table.persist_dirty_pages() {
                    error!(
                        "Failed to persist dirty pages for table '{}': {err}",
                        table.name()
                    );
                }
            }
        }
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
            self.transaction_snapshot
                .write()
                .unwrap()
                .smallest_active_transaction_id = self
                .transaction_snapshot
                .read()
                .unwrap()
                .active_transaction
                .iter()
                .min()
                .unwrap()
                .clone();
        }
        self.persist_all_dirty_pages();
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
                .smallest_active_transaction_id = self
                .transaction_snapshot
                .read()
                .unwrap()
                .last_possible_transaction_id;
        } else {
            self.transaction_snapshot
                .write()
                .unwrap()
                .smallest_active_transaction_id = self
                .transaction_snapshot
                .read()
                .unwrap()
                .active_transaction
                .iter()
                .min()
                .unwrap()
                .clone();
        }
    }

    pub fn remove_stray_tables(&mut self) {
        for (_, table) in self.tables.iter() {
            table.write().unwrap().prune(
                self.transaction_snapshot
                    .read()
                    .unwrap()
                    .smallest_active_transaction_id,
            );
        }
    }

    pub fn auto_vacuum(&mut self) {
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

    use super::Database;

    #[test]
    fn dirty_pages_are_written_on_commit() {
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
        table.read().unwrap().insert_row(data, &txn).unwrap();
        db.commit_transaction(txn.transaction_id);

        let expected = dir.join("testdb-users");
        assert!(
            expected.exists(),
            "expected table page file at {}",
            expected.display()
        );
        assert!(expected.metadata().unwrap().len() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
