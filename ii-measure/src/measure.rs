use crate::inverted_index::InvertedIndex;
use crate::util::RandomKeys;
use crate::{log::Logger, TableLocation};
use get_size::GetSize;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn retrieval<T, O>(ii: T, mut log: Logger)
where
    T: InvertedIndex<O> + RandomKeys,
    O: Sized,
{
    eprintln!("Step 2. Measure retrieval time.");

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
            eprintln!("{:02}%", percentage);
        }
    }

    let len = ds.len() as f64;
    let avg = ds.into_iter().map(|d| d.as_nanos() as u64).sum::<u64>() as f64 / len;
    let avg = Duration::from_nanos(avg as u64);
    log.retrieval_info(avg);

    log.print();
}

pub fn measure_logging<F, II, O>(
    algorithm: F,
    receiver: Receiver<(String, TableLocation)>,
    mut log: Logger,
) where
    F: Fn(Receiver<(String, TableLocation)>) -> (usize, II),
    II: InvertedIndex<O> + RandomKeys + GetSize,
    O: Sized,
{
    eprintln!("Step 1. Measure insertion time.");

    let starttime = Instant::now();

    let (entry_count, ii) = algorithm(receiver);

    let duration = starttime.elapsed();

    log.memory_info((entry_count, ii.get_size(), duration));

    retrieval(ii, log);
}
