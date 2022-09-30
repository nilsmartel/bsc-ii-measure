use std::thread::spawn;

use std::sync::mpsc::channel;

use std::sync::mpsc::Receiver;

use crate::cli::CompressionAlgorithm;
use crate::*;

mod random_keys;
pub use random_keys::RandomKeys;

pub(crate) fn collect_indices(table: &str, limit: usize) -> Receiver<(String, TableIndex)> {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), table, limit);

    spawn(move || database.read(sender));
    receiver
}

pub fn best_filename(table: &str, limit: usize, algo: CompressionAlgorithm) -> String {
    let limit = to_sci_str(limit);
    let algo = algo.str();

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
