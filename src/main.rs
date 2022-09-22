use std::sync::mpsc::channel;
use std::thread::spawn;

mod table_lake;
use table_lake::*;

mod db;

// type InvertedIndex = Map<String, Vec<TableIndex>>;

fn main() {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), "gittables_main_tokenized").limit(15);

    let p = spawn(move || database.read(sender));

    for (cell, _) in receiver {
        println!("{cell}");
    }

    p.join().expect("join thread");
}
