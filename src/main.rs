use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap as Map;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::channel;
use std::thread::spawn;

mod table_lake;
use table_lake::*;

mod db;
mod util;

// type InvertedIndex = Map<String, Vec<TableIndex>>;

fn main() {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), "gitttables_main_tokenized").limit(15);

    let p = spawn(move || database.read(sender));

    // this will never be reached at the moment
    for (cell, _) in receiver {
        println!("{cell}");
    }

    // let inverted_index = into_inverted_index(receiver.into_iter());

    // print_cell_value_overlap_distribution(&inverted_index);

    p.join().expect("to join thread");
}
