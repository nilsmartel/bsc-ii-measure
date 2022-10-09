use std::thread::spawn;

use std::sync::mpsc::{Receiver,  sync_channel};

mod random_keys;
use rand::random;
pub use random_keys::RandomKeys;

use crate::db;
use crate::table_lake::*;
use bintable::BinTable;

const CHANNEL_BOUND: usize = 32;

pub fn indices_from_bintable(
    bintable: &str,
    factor: Option<f32>,
) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let mut bintable = BinTable::open(bintable).expect("open bintable");

    if let Some(factor) = factor {
        spawn(move || bintable.filter(|_| random::<f32>() > factor).read(sender));
    } else {
        spawn(move || bintable.read(sender));
    }

    receiver
}

pub fn indices(table: &str, factor: Option<f32>) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let mut database = DatabaseCollection::new(db::client(), table, factor);

    spawn(move || database.read(sender));
    receiver
}
