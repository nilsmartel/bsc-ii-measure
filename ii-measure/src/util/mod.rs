use std::thread::spawn;

use std::sync::mpsc::channel;

use std::sync::mpsc::Receiver;

mod random_keys;
pub use random_keys::RandomKeys;

use crate::cli::CompressionAlgorithm;
use crate::db;
use crate::table_lake::*;
use bintable::BinTable;

pub fn indices_from_bintable(
    bintable: &str,
    limit: Option<usize>,
) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = channel();

    let mut bintable = BinTable::open(bintable).expect("open bintable");

    if let Some(limit) = limit {
        // TODO BE AWARE THAT WE ARE USING A
        // HIGHLY BIASED
        // CONVINIENCE SAMPLE
        // HERE IMPLICITLY
        //
        // Since we sort by the table cells,
        // the resulting rows have
        // MUCH LOWER ENTROPY
        // that usually can be expected from
        // lakes of tables of size `limit`
        spawn(move || bintable.take(limit).read(sender));
    } else {
        spawn(move || bintable.read(sender));
    }

    receiver
}

pub fn indices(table: &str, limit: usize) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), table, limit);

    spawn(move || database.read(sender));
    receiver
}

pub fn best_filename(table: &str, limit: usize, algo: CompressionAlgorithm) -> String {
    let limit = to_sci_str(limit);
    let algo = algo.str();

    let table = if let Some((_path, file)) = table.rsplit_once('/') {
        file
    } else {
        table
    };

    format!("{limit}-{table}-{algo}")
}

pub fn to_sci_str(n: usize) -> String {
    if n < 1_000 {
        return format!("{n}");
    }

    if n < 1_000_000 {
        let n = n / 1000;
        return format!("{n}k");
    }

    if n < 1_000_000_000 {
        let n = n / 1_000_000;
        return format!("{n}M");
    }

    let n = n / 1_000_000_000;

    format!("{n}G")
}
