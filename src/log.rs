use std::fs::File;
use std::io::Write;
use std::sync::mpsc::*;
use std::thread::spawn;
use std::time::Duration;

pub type MemLog = (usize, usize, Duration);

/// Handles logging and formatting of information to file
pub struct Logger {
    sender: Sender<Msg>,
}

impl Logger {
    /// Starts a new logging server as a separate thread and opens the desired files.
    pub fn new(output_file: impl Into<String>) -> Self {
        let (sender, receiver) = channel::<Msg>();

        // File containing the csv formatted information about how the memory rises
        // with respect to the amount of cell values inserted into the table
        let outputfile = output_file.into();
        let mem_stats = outputfile.clone() + "-mem.csv";
        let retr_stats = outputfile + "-retr.csv";

        println!("writing to {mem_stats}");
        println!("writing to {retr_stats}");

        spawn(move || {
            let mut mem_stats = File::create(mem_stats).expect("create mem stat file");
            writeln!(&mut mem_stats, "cells;bytes;insert_duration_nanosec")
                .expect("to write mem stat header");

            let mut retr_stats = File::create(retr_stats).expect("create retrieval stat file");
            writeln!(&mut retr_stats, "retrieval_duration_nanosec")
                .expect("write to retrieval stat file");

            for msg in receiver {
                match msg {
                    Msg::Mem((cells, bytes, duration)) => {
                        let duration = duration.as_nanos();
                        writeln!(&mut mem_stats, "{cells};{bytes};{duration}")
                            .expect("to write mem stat row");
                    }
                    Msg::Retr(duration) => {
                        let duration = duration.as_nanos();
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
