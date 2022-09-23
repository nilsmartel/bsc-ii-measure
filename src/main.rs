mod util;
use util::*;

mod inverted_index;
mod measure;
mod table_lake;
use log::Logger;
use structopt::StructOpt;
use table_lake::*;

mod cli;
mod db;
mod log;

fn main() {
    let config = cli::Config::from_args();
    let table = &config.table;
    let limit = config.limit;
    let compression = config.compression;
    let output = config.output.unwrap_or_else(|| best_filename(table, limit, config.compression));

    println!("benchmarking {table} {limit} {}", compression.str());

    let (receiver, p) = collect_indices(table, limit);

    // init information logger
    let log = Logger::new(&output);

    // Select Compression Algorithm and perfom
    use cli::CompressionAlgorithm::*;
    match config.compression {
        Baseline => measure::baseline(receiver, log),
        DuplicatesHash => measure::duplicates_hash(receiver, log),
        DuplicatesTree => measure::duplicates_tree(receiver, log),
    }
    p.join().expect("join thread");
}
