use std::collections::HashMap;
use nohash_hasher::BuildNoHashHasher;

pub type IntMap<V> = HashMap<u64, V, BuildNoHashHasher<u64>>;
pub type DecodedData = crate::backend::core::row::DataBaseDataEntry;