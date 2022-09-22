use get_size::GetSize;

mod util;
use util::*;

mod table_lake;
use structopt::StructOpt;
use table_lake::*;
use log::Logger;

mod log;
mod cli;
mod db;

fn main() {
    let config = cli::Config::from_args();
    let table = &config.table;

    let (receiver, p) = collect_indices(table, config.limit);

    // init information logger
    let mut log = Logger::new(&config.output)
        .expect("init logging");

    let mut ii = Vec::new();
    
    for data in receiver {
        ii.push(data);
        log.memory(ii.len(), ii.get_size()).expect("log information");
    }

    p.join().expect("join thread");
}