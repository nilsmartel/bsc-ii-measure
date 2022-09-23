use std::thread::spawn;

use std::sync::mpsc::channel;

use std::thread::JoinHandle;

use std::sync::mpsc::Receiver;

use crate::*;

mod random_keys;
pub use random_keys::RandomKeys;

pub(crate) fn collect_indices(
    table: &str,
    limit: usize,
) -> (Receiver<(String, TableIndex)>, JoinHandle<()>) {
    let (sender, receiver) = channel();

    let mut database = DatabaseCollection::new(db::client(), table, limit);

    let p = spawn(move || database.read(sender));
    (receiver, p)
}
