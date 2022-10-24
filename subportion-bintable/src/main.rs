use bintable2::*;
use rand::Rng;
use std::fs::File;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::spawn;

fn print_help() -> ! {
    eprintln!("subportion-bintable <input> <outputname> <factor>");
    std::process::exit(0);
}

fn get_args() -> (String, String, f32) {
    let mut a = std::env::args().skip(1);
    let input = a.next().unwrap_or_else(|| print_help());
    let output = a.next().unwrap_or_else(|| print_help());
    let factor = a
        .next()
        .unwrap_or_else(|| print_help())
        .parse::<f32>()
        .expect("factor to be a number");

    if factor < 0. || factor >= 1.0 {
        panic!("factor must be smaller than 1 and greater than 0");
    }

    (input, output, factor)
}

fn main() {
    let (input, output, factor) = get_args();

    if input == output {
        panic!("input must not be output");
    }

    let rows = get_rows(input);

    // write back data

    let mut out = File::open(output).expect("open output file");
    let mut acc = ParseAcc::default();

    let mut rnd = rand::thread_rng();
    for row in rows {
        if rnd.gen::<f32>() > factor {
            continue;
        }

        row.write_bin(&mut out, &mut acc).expect("write to output");
    }
}

fn get_rows(path: String) -> Receiver<TableRow> {
    let (s, r) = sync_channel(1024);
    spawn(move || {
        let table = BinTable::open(&path).expect("open bintable");

        for row in table {
            s.send(row).expect("send to channel");
        }
    });

    r
}
