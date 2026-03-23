use std::collections::HashSet;

use serde::{Deserialize, Serialize};

pub type TransactionID = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionSnapshot {
    smallest_active_transaction_id: TransactionID,
    last_possible_transaction_id: TransactionID,
    active_transaction: HashSet<TransactionID>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transaction {
    pub transaction_id: TransactionID,
    pub snapshot: TransactionSnapshot,
}

impl TransactionSnapshot {
    pub fn is_visible(&self, created_by: TransactionID, deleted_by: Option<TransactionID>) -> bool {
        if created_by > self.last_possible_transaction_id {
            return false; // created after snapshot
        }

        if self.active_transaction.contains(&created_by) {
            return false; // still in progress
        }

        match deleted_by {
            None => return true, // not deleted

            Some(deleted_by) => {
                // deleted by a transaction definitely committed before snapshot
                if deleted_by < self.smallest_active_transaction_id {
                    return false;
                }

                // deleted by a transaction that committed before snapshot
                if deleted_by <= self.last_possible_transaction_id
                    && !self.active_transaction.contains(&deleted_by)
                {
                    return false;
                }

                // deleting transaction is still in progress → visible
                true
            }
        }
    }
}
