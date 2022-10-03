use crate::table_lake::Entry;
use crate::TableLocation;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::Receiver;

/// Baseline measure of data, the way it is present in database
pub(crate) fn baseline(receiver: Receiver<(String, TableLocation)>) -> (usize, Vec<Entry>) {
    let mut ii = Vec::new();
    for data in receiver {
        ii.push(data);
    }

    (ii.len(), ii)
}

/// Performs deduplication using a HashMap
pub(crate) fn dedup_hash(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, HashMap<String, Vec<TableLocation>>) {
    let mut ii: HashMap<String, Vec<TableLocation>> = HashMap::new();

    let mut group_id = String::new();
    let mut buffer = Vec::new();

    for (index, data) in receiver {
        if index != group_id {
            ii.insert(index.clone(), buffer.clone());
            group_id = index;
            buffer.clear();
        }

        buffer.push(data);
    }

    let entry_count = ii.len();
    (entry_count, ii)
}

/// Performs deduplication using a btreemap
pub(crate) fn dedup_btree(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, BTreeMap<String, Vec<TableLocation>>) {
    let mut ii: BTreeMap<String, Vec<TableLocation>> = BTreeMap::new();

    let mut group_id = String::new();
    let mut buffer = Vec::new();

    for (index, data) in receiver {
        if index != group_id {
            ii.insert(index.clone(), buffer.clone());
            group_id = index;
            buffer.clear();
        }

        buffer.push(data);
    }

    let entry_count = ii.len();
    (entry_count, ii)
}

// we're storing the overshooting length,
// as the implementation does not consider that elements may not come in blocks of precisely 4.
pub type Compressed4Wise = HashMap<String, (Vec<u8>, u8)>;

pub(crate) fn ns_4_wise(receiver: Receiver<(String, TableLocation)>) -> (usize, Compressed4Wise) {
    use int_compression_4_wise::compress;
    let mut ii: Compressed4Wise = HashMap::new();
    let mut entry_count = 0;

    // we're using the index to group received indices
    let mut group_id = String::new();

    // we're using an intermediate buffer
    // to collect the integers we'd like to compress
    let mut current_buffer = Vec::<u32>::with_capacity(256);

    for (index, location) in receiver {
        if index != group_id {
            // 0, 1, 2, 3,
            let overshoot: u8 = (current_buffer.len() % 4) as u8;
            let overshoot = (4 - overshoot) % 4;

            let compressed_data = compress(current_buffer.iter().cloned());

            // set new index as group-indentifier
            group_id = index;

            ii.insert(group_id.clone(), (compressed_data, overshoot));

            // clear the buffer for new usage.
            current_buffer.clear();
        }

        current_buffer.extend(location.integers());

        entry_count += 1;
    }

    (entry_count, ii)
}
