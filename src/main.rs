use get_size::GetSize;
use anyhow::Result;

mod util;
use util::*;

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

macro_rules! timed {
    ($e:expr) => {
        {
            let time_now = std::time::Instant::now();
            let result = $e;
            let duration = time_now.elapsed();
            (duration, result)
        }
    };
}

mod measure {
    use anyhow::Result;

    use super::Log;
    use crate::TableIndex;
    use get_size::GetSize;
    use std::sync::mpsc::Receiver;

    /// Baseline measure of data, the way it is present in database
    pub(crate) fn baseline(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()>{
        let mut ii = Vec::new();
        for data in receiver {
            let (t,_) = timed!(ii.push(data));

            log.send((ii.len(), ii.get_size(), t))?;
        }

        Ok(())
    }

    /// Performs deduplication using a HashMap
    pub(crate) fn duplicates_hash(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()> {
        use std::collections::HashMap as Map;

        let mut ii = Map::new();
        let mut i = 1;
        for (index, data) in receiver {
            let (t,_) = timed!(
                ii.insert(index, data)
            );

            log.send((i, ii.get_size(), t))?;

            i += 1;
        }

        Ok(())
    }

    /// Performs deduplication using a btreemap
    pub(crate) fn duplicates_tree(receiver: Receiver<(String, TableIndex)>, log: Log) -> Result<()> {
        use std::collections::BTreeMap as Map;

        let mut ii = Map::new();
        let mut i = 1;
        for (index, data) in receiver {
            let (t,_) = timed!(
                ii.insert(index, data)
            );

            log.send((i, ii.get_size(), t))?;

            i += 1;
        }

        Ok(())
    }
}
