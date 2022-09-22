use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap as Map;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::channel;
use std::thread::spawn;

mod table_lake;
use table_lake::*;

mod util;

// type InvertedIndex = Map<String, Vec<TableIndex>>;

fn main() {
    let mut lake = util::collect_tables_from_stdin();

    let (sender, receiver) = channel();

    let p = spawn(move || lake.read(sender));

    for (cell, _ ) in receiver {
        println!("{cell}");
    }

    // let inverted_index = into_inverted_index(receiver.into_iter());

    // print_cell_value_overlap_distribution(&inverted_index);

    p.join().expect("to join thread");
}

fn print_cell_value_overlap_distribution(ii: &Map<u64, u32>) {
    let mut d = Vec::new();

    for (_, occurences) in ii {
        let distr_index = *occurences as usize;

        while d.len() <= distr_index {
            d.push(0);
        }

        d[distr_index] += 1;
    }

    println!("n;cvo");

    for (n, cvo) in d.into_iter().enumerate() {
        if cvo == 0 {
            continue;
        }

        println!("{n};{cvo}");
    }
}

fn into_inverted_index(iter: impl Iterator<Item = Entry>) -> Map<u64, u32> {
    let mut ii = Map::new();
    for (cell_value, _position) in iter {
        let cell_value_hash = {
            let mut h = DefaultHasher::new();
            cell_value.hash(&mut h);
            h.finish()
        };

        *ii.entry(cell_value_hash).or_insert(0) += 1;
    }

    ii
}
