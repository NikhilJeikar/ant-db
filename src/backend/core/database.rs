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
    pub fn new(internal_state_manager: Arc<RwLock<InternalStateManager>>) -> Self {
        Database {
            tables: BTreeMap::new(),
            next_table_id: AtomicU64::new(0),
            internal_state_manager,
            transaction_snapshot: Arc::new(RwLock::new(TransactionSnapshot::default())),
            next_transaction_id: AtomicU64::new(1),
        }
    }

    pub fn create_table(&mut self, table_name: String) -> Result<TableID, DataBaseErrors> {
        if self.tables.get(&table_name).is_none() {
            let table_id = self.next_table_id.fetch_add(1, Ordering::SeqCst);
            self.tables.insert(
                table_name.clone(),
                Arc::new(RwLock::new(Table::new(
                    table_id,
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
}
