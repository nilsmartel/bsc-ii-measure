use crate::table_lake::*;
use is_sorted::IsSorted;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

/// Baseline measure of data, the way it is present in database
pub fn baseline(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, Vec<Entry>) {
    let mut ii = Vec::new();
    let mut build_time = Duration::new(0, 0);

    for data in receiver {
        let starttime = Instant::now();
        ii.push(data);

        build_time += starttime.elapsed();
    }

    eprintln!("entries: {}", ii.len());
    if IsSorted::is_sorted(&mut ii.iter()) {
        eprint!("sorting");
        ii.sort_unstable();
        eprint!(" complete");
    }

    (ii.len(), build_time, ii)
}

pub fn baseline_exact(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, Vec<Entry>) {
    let (count, time, mut ii) = baseline(receiver);
    ii.shrink_to_fit();
    (count, time, ii)
}
