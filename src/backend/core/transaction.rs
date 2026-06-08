use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

pub type TransactionID = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionSnapshot {
    pub smallest_active_transaction_id: TransactionID,
    pub last_possible_transaction_id: TransactionID,
    pub active_transaction: HashSet<TransactionID>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub transaction_id: TransactionID,
    pub snapshot: Arc<RwLock<TransactionSnapshot>>,
}

impl Default for TransactionSnapshot {
    fn default() -> Self {
        TransactionSnapshot { smallest_active_transaction_id: 0, last_possible_transaction_id: 0, active_transaction: HashSet::new() }
    }
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionHeader {
    pub created_by: TransactionID,
    pub deleted_by: Option<TransactionID>,
}


impl TransactionHeader {
    pub fn is_visible(&self, txn: &Transaction) -> bool {
        let snapshot = txn.snapshot.read().unwrap();

        if self.created_by == txn.transaction_id {
            return self.deleted_by != Some(txn.transaction_id);
        }

        if self.created_by > snapshot.last_possible_transaction_id {
            return false; // created after snapshot
        }

        if snapshot.active_transaction.contains(&self.created_by) {
            return false; // still in progress
        }

        match self.deleted_by {
            None => true,
            Some(deleted_by) => {
                if deleted_by == txn.transaction_id {
                    return false;
                }

                if deleted_by < snapshot.smallest_active_transaction_id {
                    return false;
                }

                if deleted_by <= snapshot.last_possible_transaction_id
                    && !snapshot.active_transaction.contains(&deleted_by)
                {
                    return false;
                }

                true
            }
        }
    }
}