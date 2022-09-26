use std::fs::File;
use std::io::Write;
use std::time::Duration;

pub type MemData = (usize, usize, Duration);

/// Handles logging and formatting of information to file
pub struct Logger {
    mem_file: String,
    retr_file: String,
}

impl Logger {
    /// Starts a new logging server as a separate thread and opens the desired files.
    pub fn new(output_file: impl Into<String>) -> Self {
        // File containing the csv formatted information about how the memory rises
        // with respect to the amount of cell values inserted into the table
        let outputfile = output_file.into();
        let mem_file = format!("{outputfile}-mem.csv");
        let retr_file = format!("{outputfile}-retr.csv");

        println!("writing to {mem_file}");
        println!("writing to {retr_file}");

        Logger {
            mem_file,
            retr_file,
        }
    }

    pub fn memory_info(&self, data: Vec<MemData>) {
        let mut mem_stats = File::create(&self.mem_file).expect("create mem.csv file");
        writeln!(&mut mem_stats, "cells;bytes;insert_duration_nanosec")
            .expect("to write mem stat header");

        for (cells, bytes, duration) in data {
            let duration = duration.as_nanos();
            writeln!(&mut mem_stats, "{cells};{bytes};{duration}").expect("to write mem stat row");
        }
    }

    pub fn retrieval_info(&mut self, durations: Vec<Duration>) {
        let mut retr_stats = File::create(&self.retr_file).expect("create retr.csv file");
        writeln!(&mut retr_stats, "retrieval_duration_nanosec")
            .expect("write to retrieval stat file");

        for duration in durations {
            let duration = duration.as_nanos();
            writeln!(&mut retr_stats, "{duration}").expect("write row to retrieval stat file");
        }
    }
}
