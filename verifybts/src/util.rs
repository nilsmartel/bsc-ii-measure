use std::thread::spawn;

use std::sync::mpsc::{sync_channel, Receiver};

use crate::db::sqlx_pool;
use crate::table_lake::SqlxCollection;
use crate::table_lake::TableLakeReader;
use crate::table_lake::TableLocation;

use bintable::BinTable;

const CHANNEL_BOUND: usize = 32;

pub fn indices_from_bintable(bintable: &str) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let mut bintable = BinTable::open(bintable).expect("open bintable");

    spawn(move || bintable.read(sender));

    receiver
}

pub fn indices_sqlx(table: &str) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(CHANNEL_BOUND);

    let pool = sqlx_pool();
    let mut database = SqlxCollection::new(pool, table, None);

    spawn(move || database.read(sender));
    receiver
}
