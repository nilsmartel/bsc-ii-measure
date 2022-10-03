use bintable::*;
use get_size::GetSize;
use std::io::Write;

fn help() -> ! {
    println!(
        "usage: bintable-info <table>
        "
    );
    std::process::exit(0);
}

fn main() {
    let table = match std::env::args().nth(1) {
        Some(t) => t,
        None => help(),
    };

    let table = BinTable::open(&table).expect("open bintable file");

    let e = std::io::stderr();
    let mut out = e.lock();

    let mut count = 064;
    let mut size = 0u64;
    for row in table {
        count += 1;
        size += row.tokenized.get_size() as u64 + 16;

        if count & 0xff == 0 {
            writeln!(&mut out, "{: 3}", count).unwrap();
        }
    }

    println!("count: {count}");
    println!("size: {size}");
}
