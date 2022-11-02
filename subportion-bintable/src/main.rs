#![feature(file_create_new)]

use bintable2::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::spawn;

fn print_help() -> ! {
    eprintln!("subportion-bintable <input> <factor>");
    std::process::exit(0);
}

struct BintableName {
    path: String,
    name: String,
    _factor: f32,
}

fn parse_bintable_name(path: String) -> BintableName {
    let (path, name) = if let Some((path, name)) = path.rsplit_once('/') {
        (path, name)
    } else {
        (".", &path as &str)
    };

    let path = path.to_string();
    let name = name.to_string();

    let (name, _factor) = if let Some((name, factor)) = name.rsplit_once("-") {
        (
            name.to_string(),
            factor
                .parse::<f32>()
                .expect("factor to be a floating point number"),
        )
    } else {
        (name, 1.0f32)
    };

    BintableName {
        path,
        name,
        _factor,
    }
}

fn args() -> (BintableName, f32) {
    let mut a = std::env::args().skip(1);
    let input = a.next().unwrap_or_else(|| print_help());
    let factor = a
        .next()
        .unwrap_or_else(|| print_help())
        .parse::<f32>()
        .expect("factor to be a number");

    if factor <= 0. || factor >= 1.0 {
        panic!("factor must be smaller than 1 and greater than 0");
    }

    (parse_bintable_name(input), factor)
}

fn main() {
    let (input, factor) = args();

    let output = format!("{}/{}-{}", input.path, input.name, factor);

    let inputfile = format!("{}/{}", input.path, input.name);

    let rows = get_rows(&inputfile, factor);

    // write back data

    let out = File::create_new(output).expect("open output file");
    let mut out = BufWriter::new(out);
    let mut acc = ParseAcc::default();

    for row in rows {
        row.write_bin(&mut out, &mut acc).expect("write to output");
    }

    out.flush().expect("flush output");
}

fn get_rows(path: &str, factor: f32) -> Receiver<TableRow> {
    let path = path.to_string();
    let (s, r) = sync_channel(1024);
    spawn(move || {
        let table = BinTableSampler::open(&path, factor).expect("open bintable");

        for row in table {
            s.send(row).expect("send to channel");
        }
    });

    r
}
