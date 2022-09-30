use crate::table_lake::Entry;
use crate::TableIndex;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::Receiver;
use int_compression_4_wise::ListUInt32;


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

type Compressed4Wise = HashMap<String, ListUInt32>;

pub(crate) fn ns_4_wise(
    receiver: Receiver<(String, TableIndex)>,
) -> (usize, Compressed4Wise) {
    let mut ii: Compressed4Wise = HashMap::new();
    let mut i = 0;

    for (index, TableIndex { table_id, row_id, column_id }) in receiver {
        let index = ii.entry(index).or_insert_with(Default::default);
        index.push(table_id);
        index.push(row_id);
        let column_id = if column_id <= std::u32::MAX as u64 {
            column_id as u32
        } else {
            eprintln!("error, number is to high {}", column_id);
            column_id.min(std::u32::MAX as u64) as u32
        };
        
        index.push(column_id);
        i+= 1;
    }

    let entry_count = i;
    (entry_count, ii)
}
