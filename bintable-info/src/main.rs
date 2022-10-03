use bintable::*;
use get_size::GetSize;
use std::io::Write;
use structopt::StructOpt;

fn help() -> ! {
    println!(
        "usage: bintable-info <table>
        "
    );
    std::process::exit(0);
}

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

    let mut count = 064;
    let mut size = 0u64;
    for row in table.take(limit) {
        count += 1;
        size += row.tokenized.get_size() as u64 + 16;

        if count & 0xff == 0 {
            writeln!(&mut out, "{:03}", count).unwrap();
        }
    }

    println!("count: {count}");
    println!("size: {size}");
}
