use crate::inverted_index::InvertedIndex;
use crate::util::RandomKeys;
use crate::{log::Logger, TableLocation};
use get_size::GetSize;
use std::sync::mpsc::Receiver;
use std::time::Instant;

fn retrieval<T, O>(ii: T, mut log: Logger)
where
    T: InvertedIndex<O> + RandomKeys,
    O: Sized,
{
    println!("Step 2. Measure retrieval time.");

    // TODO ensure that this is not getting optimized out!
    let keys = ii.random_keys();
    let max = keys.len() as f32;

    let mut ds = Vec::with_capacity(keys.len());

    for (index, key) in keys.into_iter().enumerate() {
        let starttime = Instant::now();
        let _table_indices = ii.get(&key);
        drop(_table_indices);

        let duration = starttime.elapsed();
        ds.push(duration);

        if index & 0x1ff == 0 {
            let percentage = (index as f32 / max) * 100.0;
            println!("{:02}%", percentage);
        }
    }

    log.retrieval_info(ds);
}

pub fn measure_logging<F, II, O>(
    algorithm: F,
    receiver: Receiver<(String, TableLocation)>,
    log: Logger,
) where
    F: Fn(Receiver<(String, TableLocation)>) -> (usize, II),
    II: InvertedIndex<O> + RandomKeys + GetSize,
    O: Sized,
{
    println!("Step 1. Measure insertion time.");

    let starttime = Instant::now();

    let (entry_count, ii) = algorithm(receiver);

    let duration = starttime.elapsed();
    log.memory_info(vec![(entry_count, ii.get_size(), duration)]);
    retrieval(ii, log);
}
