
use anyhow::Result;

use super::Log;
use crate::TableIndex;
use get_size::GetSize;
use std::sync::mpsc::Receiver;

/// Macro used to measure the time it takes 
/// to perform some expression
macro_rules! timed {
    ($e:expr) => {
        {
            let time_now = std::time::Instant::now();
            let result = $e;
            let duration = time_now.elapsed();
            (duration, result)
        }
    };
}

/// Baseline measure of data, the way it is present in database
pub(crate) fn baseline(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()> {
    let mut ii = Vec::new();
    for data in receiver {
        let (t, _) = timed!(ii.push(data));

        log.send((ii.len(), ii.get_size(), t))?;
    }

    Ok(())
}

/// Performs deduplication using a HashMap
pub(crate) fn duplicates_hash(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()> {
    use std::collections::HashMap as Map;

    let mut ii = Map::new();
    let mut i = 1;
    for (index, data) in receiver {
        let (t, _) = timed!(ii.insert(index, data));

        log.send((i, ii.get_size(), t))?;

        i += 1;
    }

    Ok(())
}

/// Performs deduplication using a btreemap
pub(crate) fn duplicates_tree(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()> {
    use std::collections::BTreeMap as Map;

    let mut ii = Map::new();
    let mut i = 1;
    for (index, data) in receiver {
        let (t, _) = timed!(ii.insert(index, data));

        log.send((i, ii.get_size(), t))?;

        i += 1;
    }

    Ok(())
}
