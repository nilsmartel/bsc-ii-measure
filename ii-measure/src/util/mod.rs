use std::thread::spawn;

use std::sync::mpsc::{sync_channel, Receiver};

mod random_keys;
use rand::random;
pub use random_keys::RandomKeys;
use sqlx::postgres::PgPoolOptions;

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
        spawn(move || bintable.filter(|_| random::<f32>() < factor).read(sender));
    } else {
        spawn(move || bintable.read(sender));
    }

    receiver
}

pub fn indices_postgresql(table: &str, factor: Option<f32>) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let mut database = DatabaseCollection::new(db::postgresql_client(), table, factor);

    spawn(move || database.read(sender));
    receiver
}

pub fn indices_sqlx(table: &str, factor: Option<f32>) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::client_config());

    let pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pool);

    let pool = pool.expect("set up sqlx postgres pool");

    let mut database = SqlxCollection::new(pool, table, factor);

    spawn(move || database.read(sender));
    receiver
}
