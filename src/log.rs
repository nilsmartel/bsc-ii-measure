use std::fs::File;
use std::io::Write;
use std::sync::mpsc::*;
use std::thread::{spawn, JoinHandle};
use std::time::Duration;

use crate::cli::CompressionAlgorithm;

pub type MemLog = (usize, usize, Duration);

/// Handles logging and formatting of information to file
pub struct Logger {
    sender: Sender<Msg>,
}

impl Logger {
    /// Starts a new logging server as a separate thread and opens the desired files.
    pub fn new(output_file: impl Into<String>, algo: CompressionAlgorithm) -> Self {
        let (sender, receiver) = channel::<Msg>();

        let algo = algo.str();
        // File containing the csv formatted information about how the memory rises
        // with respect to the amount of cell values inserted into the table
        let outputfile = output_file.into() + &algo;
        let mem_stats = outputfile.clone() + "-mem.csv";
        let retr_stats = outputfile + "-retr.csv";

        spawn(move || {
            let mut mem_stats = File::create(mem_stats).expect("create mem stat file");
            writeln!(&mut mem_stats, "cells;bytes;insert_duration_microsec")
                .expect("to write mem stat header");

            let mut retr_stats = File::create(retr_stats).expect("create retrieval stat file");
            writeln!(&mut retr_stats, "retrieval_duration_microsec")
                .expect("write to retrieval stat file");

            for msg in receiver {
                match msg {
                    Msg::Mem((cells, bytes, duration)) => {
                        let duration = duration.as_micros();
                        writeln!(&mut mem_stats, "{cells};{bytes};{duration}")
                            .expect("to write mem stat row");
                    }
                    Msg::Retr(duration) => {
                        let duration = duration.as_micros();
                        writeln!(&mut retr_stats, "{duration}")
                            .expect("write row to retrieval stat file");
                    }
                }
            }
        });

        Logger { sender }
    }

    #[inline]
    pub fn memory_info(&self, cells: usize, bytes: usize, duration: std::time::Duration) {
        self.sender
            .send(Msg::Mem((cells, bytes, duration)))
            .expect("write to logging channel");
    }

    #[inline]
    pub fn retrieval_info(&mut self, duration: std::time::Duration) {
        self.sender
            .send(Msg::Retr(duration))
            .expect("write to retrieval logging channel");
    }
}

enum Msg {
    Mem(MemLog),
    Retr(Duration),
}
