use bintable2::*;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file to be checked
    #[structopt()]
    table: String,
}

fn main() {
    let Config { table } = Config::from_args();

    let mut bintable = BinTable::open(&table).expect("open bintable file");

    let mut last = bintable.by_ref().next().unwrap().tokenized;
    let mut i = 0;

    for TableRow {
        tokenized,
        tableid,
        colid,
        rowid,
    } in bintable
    {
        if last <= tokenized {
            last = tokenized;
            i += 1;
            continue;
        }

        println!("[{i}] keys {last} {tokenized} are not in order");
        std::process::exit(1);
    }
}
