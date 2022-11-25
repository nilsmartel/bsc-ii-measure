use bintable2::BinTable;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file
    #[structopt()]
    corpus: String,

    /// Cell to be searched and counted
    #[structopt()]
    cell: String,
}

fn main() {
    let Config { corpus, cell } = Config::from_args();

    let mut corpus = BinTable::open(&corpus).expect("open corpus");

    for row in corpus.by_ref() {
        if row.tokenized < cell {
            continue;
        }
    }

    let mut count = 0;

    for row in corpus.by_ref() {
        if row.tokenized == cell {
            count += 1;
            continue;
        }

        if row.tokenized > cell {
            break;
        }
    }

    println!("Occurences of {cell}: {count}");
}
