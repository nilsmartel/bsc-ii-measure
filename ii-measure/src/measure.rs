use crate::inverted_index::InvertedIndex;
use crate::util::RandomKeys;
use crate::{log::Logger, TableLocation};
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn retrieval<T, O>(ii: &T, log: &mut Logger)
where
    T: InvertedIndex<O> + RandomKeys,
    O: Sized,
{
    eprintln!("Step 2. Measure retrieval time.");

    let keys = ii.random_keys();
    {
        let zeros = keys.iter().filter(|k| k == &"0").count();
        eprintln!("the key '0' was present {zeros} times");
    }

    let total_attempts = keys.len() as u32;
    let total_attempts_f = total_attempts as f32;

    let err = std::io::stderr();
    let mut stdout = err.lock();

    let starttime = Instant::now();
    // TODO ensure that this is not getting optimized out!
    for (index, key) in keys.into_iter().enumerate() {
        let _table_indices = ii.get(&key);

        if index & 0x1ff == 0 {
            let percentage = (index as f32 / total_attempts_f) * 100.0;
            writeln!(&mut stdout, "{:02}%", percentage).expect("log progress");
        }
    }

    drop(stdout);
    drop(err);

    let average_retrieval_time = starttime.elapsed() / total_attempts;

    log.retrieval_info(average_retrieval_time);
}

pub fn measure_logging<F, II, O>(
    algorithm: F,
    receiver: Receiver<(String, TableLocation)>,
    mut log: Logger,
) where
    F: Fn(Receiver<(String, TableLocation)>) -> (usize, Duration, II),
    II: InvertedIndex<O> + RandomKeys,
    O: Sized,
{
    eprintln!("Step 1. Measure insertion time.");

    let starttime = Instant::now();

    let (entry_count, build_time, ii) = algorithm(receiver);

    let insertion_time = starttime.elapsed();

    retrieval(&ii, &mut log);
    log.memory_info((entry_count, get_size(ii), build_time, insertion_time));
    log.print();
}

fn get_size<T>(t: T) -> usize {
    use jemalloc_ctl::{epoch, stats};
    let e = epoch::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();

    let with = allocated.read().unwrap();

    drop(t);

    e.advance().unwrap();
    let without = allocated.read().unwrap();

    with - without
}
