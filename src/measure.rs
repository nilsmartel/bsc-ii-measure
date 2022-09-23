use crate::inverted_index::InvertedIndex;
use crate::util::RandomKeys;
use crate::{log::Logger, TableIndex};
use get_size::GetSize;
use std::sync::mpsc::Receiver;

/// Macro used to measure the time it takes
/// to perform some expression
macro_rules! timed {
    ($e:expr) => {{
        let time_now = std::time::Instant::now();
        let result = $e;
        let duration = time_now.elapsed();
        (duration, result)
    }};
}

/// Baseline measure of data, the way it is present in database
pub(crate) fn baseline(receiver: Receiver<(String, TableIndex)>, log: Logger) {
    let mut ii = Vec::new();

    println!("Step 1. Measure insertion time.");

    for data in receiver {
        let (t, _) = timed!(ii.push(data));

        log.memory_info(ii.len(), ii.get_size(), t);
    }

    println!("Step 2. Measure retrieval time.");
    retrieval(ii, log);
}

fn retrieval<'a>(ii: impl InvertedIndex<'a> + RandomKeys, mut log: Logger) {
    // TODO ensure that this is not getting optimized out!
    let keys = ii.random_keys();
    for key in keys {
        let (duration, table_indexes) = timed!(ii.get(&key));

        log.retrieval_info(duration);
    }

    log.join();
}

/// Performs deduplication using a HashMap
pub(crate) fn duplicates_hash(receiver: Receiver<(String, TableIndex)>, log: Logger) {
    use std::collections::HashMap as Map;

    let mut ii: Map<String, Vec<TableIndex>> = Map::new();
    let mut i = 1;
    for (index, data) in receiver {
        let (t, _) = timed!({
            ii.entry(index).or_insert_with(Vec::new).push(data);
        });

        log.memory_info(i, ii.get_size(), t);

        i += 1;
    }

    retrieval(ii, log);
}

/// Performs deduplication using a btreemap
pub(crate) fn duplicates_tree(receiver: Receiver<(String, TableIndex)>, log: Logger) {
    use std::collections::BTreeMap as Map;

    let mut ii: Map<String, Vec<TableIndex>> = Map::new();

    let mut i = 1;
    for (index, data) in receiver {
        let (t, _) = timed!({
            ii.entry(index).or_insert_with(Vec::new).push(data);
        });

        log.memory_info(i, ii.get_size(), t);

        i += 1;
    }

    retrieval(ii, log);
}
