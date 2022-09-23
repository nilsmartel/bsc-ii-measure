mod util;
use util::*;

mod measure; 
mod table_lake;
use structopt::StructOpt;
use table_lake::*;
use log::Logger;

use std::{sync::mpsc::{ channel, Sender }, thread::spawn, time::Duration};

mod log;
mod cli;
mod db;

type Log = Sender<(usize, usize, Duration)>;

fn main() {
    let config = cli::Config::from_args();
    let table = &config.table;

    let (receiver, p) = collect_indices(table, config.limit);

    // init information logger
    let log = spawn_logger(&config.output);

    // Select Compression Algorithm and perfom 
    use cli::CompressionAlgorithm::*;
    match config.compression {
        Baseline => measure::baseline(receiver, log),
        DuplicatesHash => measure::duplicates_hash(receiver, log),
        DuplicatesTree => measure::duplicates_tree(receiver, log),
    }.expect("perform compression");

    p.join().expect("join thread");
}

fn spawn_logger(output: &str) -> Log {
    let mut log = Logger::new(output)
        .expect("init logging");
    let (snd, rc) = channel();
    spawn(move|| {
        for (cells, bytes, duration) in rc {
            log.memory(cells, bytes, duration)
        }
    });
    
    snd
}
