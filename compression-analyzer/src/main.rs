use bintable2::{BinTable, TableRow};
use std::sync::mpsc::*;
use std::thread::spawn;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    corpus: String,
}

fn main() {
    let Config { corpus } = Config::from_args();

    let corpus = BinTable::open(&corpus).unwrap();

    for (id, ints) in group(corpus) {}
}

fn group(mut corpus: BinTable) -> Receiver<(String, Vec<u32>)> {
    let (s, r) = sync_channel(256);

    spawn(move || {
        let first = corpus.by_ref().next().unwrap();
        let mut group = first.integers().to_vec();
        let mut current = first.tokenized;

        for row in corpus {
            if current != row.tokenized {
                s.send((current, group)).expect("send to channel");
                current = row.tokenized.clone();
                group = Vec::new();
            }

            group.extend(row.integers());
        }
    });

    r
}
