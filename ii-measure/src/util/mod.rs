use std::thread::spawn;

use std::sync::mpsc::{sync_channel, Receiver};

pub mod random_keys;
use rand::random;
pub use random_keys::RandomKeys;

use crate::db::sqlx_pool;
use crate::table_lake::*;
use bintable2::BinTable;

const CHANNEL_BOUND: usize = 32;

pub fn indices_from_bintable(
    bintable: &str,
    factor: Option<f32>,
) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let mut bintable = BinTable::open(bintable).expect("open bintable");

    if let Some(factor) = factor {
        spawn(move || bintable.filter(|_| random::<f32>() < factor).read(sender));
    } else {
        spawn(move || bintable.read(sender));
    }

    receiver
}

pub fn indices_sqlx(table: &str, factor: Option<f32>) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let pool = sqlx_pool();
    let mut database = SqlxCollection::new(pool, table, factor);

    spawn(move || database.read(sender));
    receiver
}
