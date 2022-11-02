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

fn get_factor(path: &str) -> f32 {
    if let Some((_, f)) = path.rsplit_once('-') {
        f.parse().expect("valid float at the end of bintable name")
    } else {
        1.0
    }
}
/// Searches for the best version of a given corpus to use for streaming
/// and adjusts the factor.
/// For example: doubling the factor, if a bintable is found, that contains only 50% of the entire
/// collection
pub fn find_best_input(path: &str, corpus: &str, factor: f32) -> std::io::Result<(String, f32)> {
    use std::fs::read_dir;

    if path.ends_with('/') {
        panic!("path musn't end with /");
    }

    let mut bestfactor = 1.0;
    let mut bestfile = format!("{}/{}", path, corpus);

    for entry in read_dir(path)? {
        if entry.is_err() {
            continue;
        }
        let entry = entry?;

        let filename = entry.file_name().into_string().unwrap();
        if !filename.starts_with(&corpus) {
            continue;
        }

        let f = get_factor(&filename);

        if f < factor {
            continue;
        }

        if f < bestfactor {
            bestfactor = f;
            bestfile = format!("{}/{}", path, filename);
        }
    }

    Ok((bestfile, factor / bestfactor))
}
