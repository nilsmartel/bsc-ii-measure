use anyhow::Result;
use std::sync::mpsc::channel;
use std::thread::spawn;

mod table_lake;
use table_lake::*;

mod db;

// type InvertedIndex = Map<String, Vec<TableIndex>>;

fn main() {
    doodlemain();
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), "gittables_main_tokenized").limit(15);

    let p = spawn(move || database.read(sender));

    // this will never be reached at the moment
    for (cell, _) in receiver {
        println!("{cell}");
    }

    // let inverted_index = into_inverted_index(receiver.into_iter());

    // print_cell_value_overlap_distribution(&inverted_index);

    p.join().expect("to join thread");
}

fn doodlemain() -> ! {
    let mut client = db::client();

    let table = "gittables_main_tokenized";
    let rows = client
        .query(
            "
        SELECT * FROM gittables_main_tokenized LIMIT 10
        ",
            &[],
        )
        .expect("perform query");

    for row in rows {
        dbg!(row);
    }

    std::process::exit(0);
}
