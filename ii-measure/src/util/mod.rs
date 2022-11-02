use std::thread::spawn;

use std::sync::mpsc::{sync_channel, Receiver};

pub mod random_keys;
pub use random_keys::RandomKeys;

use crate::db::sqlx_pool;
use crate::table_lake::*;
use bintable2::{BinTable, BinTableSampler};

const CHANNEL_BOUND: usize = 32;

pub fn indices_from_bintable(
    bintable: &str,
    factor: Option<f32>,
) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    if let Some(factor) = factor {
        let bintable = bintable.to_string();
        spawn(move || {
            let mut bintable =
                BinTableSampler::open(&bintable, factor).expect("open bintable sampler");
            bintable.read(sender)
        });
    } else {
        let mut bintable = BinTable::open(bintable).expect("open bintable");
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
