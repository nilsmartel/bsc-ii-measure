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

    let corpus = BinTable::open(&corpus).expect("open corpus");

    let mut count = 0;
    for row in corpus {
        if row.tokenized == cell {
            count += 1;
        }
        if row.tokenized > cell {
            break;
        }
    }

    println!("Occurences of {cell}: {count}");
}
