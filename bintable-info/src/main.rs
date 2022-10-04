use bintable::*;
use get_size::GetSize;
use std::io::Write;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file
    #[structopt()]
    table: String,

    #[structopt(long, short)]
    limit: usize,
}

fn main() {
    let Config { table, limit } = Config::from_args();

    let table = BinTable::open(&table).expect("open bintable file");

    let e = std::io::stderr();
    let mut out = e.lock();

    let skip = 101;
    let mut count = skip;
    let mut size = 0u64;
    for row in table.take(limit) {
        if count & 0xfff == 0 {
            writeln!(&mut out, "[{}] {:?}", count, row).unwrap();
        }

        count += 1;
        size += row.tokenized.get_size() as u64 + 16;

    }

    println!("count: {count}");
    println!("size: {size}");
}
