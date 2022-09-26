use crate::inverted_index::InvertedIndex;
use crate::table_lake::Entry;
use crate::util::RandomKeys;
use crate::{log::Logger, TableIndex};
use get_size::GetSize;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::Receiver;
use std::time::Instant;

fn retrieval<T>(ii: T, mut log: Logger)
where
    T: InvertedIndex + RandomKeys,
{
    println!("Step 2. Measure retrieval time.");

    // TODO ensure that this is not getting optimized out!
    let keys = ii.random_keys();
    let max = keys.len() as f32;

    for (index, key) in keys.into_iter().enumerate() {
        let starttime = Instant::now();
        let _table_indexes = ii.get(&key);
        drop(_table_indexes);

        let duration = starttime.elapsed();

        log.retrieval_info(duration);

        if index & 0xfff == 0 {
            let percentage = (index as f32 / max) * max;
            println!("{:02}%", percentage);
        }
    }
}

pub fn measure_logging<F, II>(algorithm: F, receiver: Receiver<(String, TableIndex)>, log: Logger)
where
    F: Fn(Receiver<(String, TableIndex)>) -> (usize, II),
    II: InvertedIndex + RandomKeys + GetSize,
{
    println!("Step 1. Measure insertion time.");

    let starttime = Instant::now();

    let (entry_count, ii) = algorithm(receiver);

    let duration = starttime.elapsed();
    log.memory_info(entry_count, ii.get_size(), duration);
    retrieval(ii, log);
}

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
