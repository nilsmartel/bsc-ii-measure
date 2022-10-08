use crate::table_lake::*;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use std::collections::{BTreeMap, HashMap};

/// Performs deduplication using a HashMap
pub fn dedup_hash(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, HashMap<String, Vec<TableLocation>>) {
    let mut ii: HashMap<String, Vec<TableLocation>> = HashMap::new();

    let mut group_id = String::new();
    let mut buffer = Vec::new();

    let mut build_time = Duration::new(0, 0);
    let mut entry_count = 0;

    for (index, data) in receiver {
        let starttime = Instant::now();
        if index != group_id {
            ii.insert(index.clone(), buffer.clone());
            group_id = index;
            buffer.clear();
        }

        buffer.push(data);
        build_time += starttime.elapsed();
        entry_count += 1;
    }

    (entry_count, build_time, ii)
}

/// Performs deduplication using a btreemap
pub fn dedup_btree(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, BTreeMap<String, Vec<TableLocation>>) {
    let mut ii: BTreeMap<String, Vec<TableLocation>> = BTreeMap::new();

    let mut entry_count = 0;

    let mut group_id = String::new();
    let mut buffer = Vec::new();

    let mut build_time = Duration::new(0, 0);

    for (index, data) in receiver {
        let starttime = Instant::now();
        if index != group_id {
            ii.insert(index.clone(), buffer.clone());
            group_id = index;
            buffer.clear();
        }

        buffer.push(data);

        build_time += starttime.elapsed();
        entry_count += 1;
    }

    (entry_count, build_time, ii)
}
