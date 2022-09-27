use crate::table_lake::Entry;
use crate::TableIndex;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::Receiver;


/// Baseline measure of data, the way it is present in database
pub(crate) fn baseline(receiver: Receiver<(String, TableIndex)>) -> (usize, Vec<Entry>) {
    let mut ii = Vec::new();
    for data in receiver {
        ii.push(data);
    }

    (ii.len(), ii)
}

/// Performs deduplication using a HashMap
pub(crate) fn dedup_hash(
    receiver: Receiver<(String, TableIndex)>,
) -> (usize, HashMap<String, Vec<TableIndex>>) {
    let mut ii: HashMap<String, Vec<TableIndex>> = HashMap::new();
    for (index, data) in receiver {
        ii.entry(index).or_insert_with(Vec::new).push(data);
    }

    let entry_count = ii.len();
    (entry_count, ii)
}

/// Performs deduplication using a btreemap
pub(crate) fn dedup_btree(
    receiver: Receiver<(String, TableIndex)>,
) -> (usize, BTreeMap<String, Vec<TableIndex>>) {
    let mut ii: BTreeMap<String, Vec<TableIndex>> = BTreeMap::new();
    for (index, data) in receiver {
        ii.entry(index).or_insert_with(Vec::new).push(data);
    }

    let entry_count = ii.len();
    (entry_count, ii)
}
