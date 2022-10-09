use crate::table_lake::*;
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

        if ii.len() & 0x3ff == 0 {
            eprint!(".")
        }
    }

    (ii.len(), build_time, ii)
}
