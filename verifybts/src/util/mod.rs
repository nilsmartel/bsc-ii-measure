use std::thread::spawn;

use std::sync::mpsc::sync_channel;

use std::sync::mpsc::Receiver;

use rand::random;

use crate::db;
use crate::table_lake::*;
use bintable::BinTable;

pub fn indices_from_bintable(
    bintable: &str,
    factor: Option<f32>,
) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(32);

    let mut bintable = BinTable::open(bintable).expect("open bintable");

    if let Some(factor) = factor {
        spawn(move || bintable.filter(|_| random::<f32>() > factor).read(sender));
    } else {
        spawn(move || bintable.read(sender));
    }

    receiver
}

pub fn indices(table: &str, factor: Option<f32>) -> Receiver<(String, TableLocation)> {
    let (sender, receiver) = sync_channel(32);

    let mut database = DatabaseCollection::new(db::client(), table, factor);

    spawn(move || database.read(sender));
    receiver
}
