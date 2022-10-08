use std::sync::mpsc::Receiver;
use crate::table_lake::*;
use std::time::{ Duration, Instant };

/// Baseline measure of data, the way it is present in database
pub fn baseline(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, Vec<Entry>) {
    let mut ii = Vec::new();
    let mut build_time = Duration::new(0, 0);
    for data in receiver {
        let starttime = Instant::now();
        ii.push(data);

        build_time += starttime.elapsed();
    }

    // sadly postgres strings are not ordered the  same as rust strings.
    // we need to order them manually.

    eprintln!("ordering strings now");
    ii.sort_by_key(|a| a.0.to_string());
    eprintln!("ordering complete");

    (ii.len(), build_time, ii)
}
