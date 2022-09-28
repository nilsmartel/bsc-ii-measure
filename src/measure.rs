use crate::inverted_index::InvertedIndex;
use crate::util::RandomKeys;
use crate::{log::Logger, TableIndex};
use get_size::GetSize;
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

    let mut ds = Vec::with_capacity(keys.len());

    for (index, key) in keys.into_iter().enumerate() {
        let starttime = Instant::now();
        let _table_indexes = ii.get(&key);
        drop(_table_indexes);

        let duration = starttime.elapsed();
        ds.push(duration);

        if index & 0x1ff == 0 {
            let percentage = (index as f32 / max) * 100.0;
            println!("{:02}%", percentage);
        }
    }

    log.retrieval_info(ds);
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
    log.memory_info(vec![(entry_count, ii.get_size(), duration)]);
    retrieval(ii, log);
}