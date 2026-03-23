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


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionHeader {
    pub created_by: TransactionID,
    pub deleted_by: Option<TransactionID>,
}


impl TransactionHeader {
    pub fn is_visible(&self, txn: &Transaction) -> bool {
        if self.created_by > txn.snapshot.last_possible_transaction_id {
            return false; // created after snapshot
        }

        if txn.snapshot.active_transaction.contains(&self.created_by) {
            return false; // still in progress
        }

        match self.deleted_by {
            None => return true, // not deleted

            Some(deleted_by) => {
                // deleted by a transaction definitely committed before snapshot
                if deleted_by < txn.snapshot.smallest_active_transaction_id {
                    return false;
                }

                // deleted by a transaction that committed before snapshot
                if deleted_by <= txn.snapshot.last_possible_transaction_id
                    && !txn.snapshot.active_transaction.contains(&deleted_by)
                {
                    return false;
                }

                // deleting transaction is still in progress → visible
                true
            }
        }
    }
}