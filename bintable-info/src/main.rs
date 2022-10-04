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

    let mut count: u64 = 0;
    let mut size: u64 = 0;
    for row in table.take(limit) {
        if count & 0xfff == 0 {
            writeln!(&mut out, "[{}] {:?}", count, row).unwrap();
        }

        count += 1;
        size += row.tokenized.get_size() as u64 + 16;

    }

    eprintln!("count: {count}");
    eprintln!("size: {}", to_byte_str(size));
}

fn to_byte_str(mut bytes: u64) -> String {
    for unit in ["","Kb", "Mb", "Gb", "Tb"] {
        if bytes < 1024 {
            return format!("{bytes}{unit}");
        }
        bytes /= 1024;
    }
    format!("{bytes} Petabyte")
}