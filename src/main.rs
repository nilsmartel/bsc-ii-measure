use get_size::GetSize;
use std::sync::mpsc::{ channel, Receiver };
use std::thread::{ spawn, JoinHandle };

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

    let (receiver, p) = collect_indices(table);

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

fn collect_indices(table: &str) -> (Receiver<(String, TableIndex)>, JoinHandle<()>) {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), table).limit(15);
    let p = spawn(move || database.read(sender));
    (receiver, p)
}
