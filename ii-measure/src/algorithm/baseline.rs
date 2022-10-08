use crate::table_lake::*;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

/// Baseline measure of data, the way it is present in database
pub fn baseline(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, Vec<Entry>) {
    let mut ii = Vec::new();
    let mut build_time = Duration::new(0, 0);

    let mut count = 0;
    for data in receiver {
        if data.0 == "0" {
            count += 1;
        }
        let starttime = Instant::now();
        ii.push(data);

        build_time += starttime.elapsed();
    }

    let percentage = (count as f32 / ii.len() as f32) * 100.0;
    eprintln!("there were {count} '0' in the set. {percentage}% of the entire set.");

    (ii.len(), build_time, ii)
}
