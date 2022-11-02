#![feature(file_create_new)]

use bintable2::*;
use rand::Rng;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::spawn;

fn print_help() -> ! {
    eprintln!("subportion-bintable <input> <factor>");
    std::process::exit(0);
}

struct BintableName {
    path: String,
    name: String,
    factor: f32,
}

fn parse_bintable_name(path: String) -> BintableName {
    let (path, name) = if let Some((path, name)) = path.rsplit_once('/') {
        (path, name)
    } else {
        (".", &path as &str)
    };

    let path = path.to_string();
    let name = name.to_string();

    let (name, factor) = if let Some((name, factor)) = name.rsplit_once("-") {
        (
            name.to_string(),
            factor
                .parse::<f32>()
                .expect("factor to be a floating point number"),
        )
    } else {
        (name, 1.0f32)
    };

    BintableName { path, name, factor }
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

fn find_best_input(path: &str, corpus: &str, factor: f32) -> io::Result<(String, f32)> {
    use std::fs::read_dir;

    if path.ends_with('/') {
        panic!("path musn't end with /");
    }

    let mut bestfactor = 1.0;
    let mut bestfile = format!("{}/{}", path, corpus);

    for entry in read_dir(path)? {
        if entry.is_err() {
            continue;
        }
        let entry = entry?;

        let filename = entry.file_name().into_string().unwrap();
        if !filename.starts_with(&corpus) {
            continue;
        }

        let f = parse_bintable_name(filename.clone()).factor;

        if f < factor {
            continue;
        }

        if f < bestfactor {
            bestfactor = f;
            bestfile = format!("{}/{}", path, filename);
        }
    }

    Ok((bestfile, factor / bestfactor))
}

fn main() {
    let (input, factor) = args();
    let (inputfile, factor_derived) = find_best_input(&input.path, &input.name, factor).unwrap();

    let output = format!("{}/{}-{}", input.path, input.name, factor);

    if inputfile == output {
        panic!("input must not be output, filename {output} is the same as input.");
    }

    let rows = get_rows(&inputfile);

    // write back data

    let out = File::create_new(output).expect("open output file");
    let mut out = BufWriter::new(out);
    let mut acc = ParseAcc::default();

    let mut rnd = rand::thread_rng();
    for row in rows {
        if rnd.gen::<f32>() > factor_derived {
            continue;
        }

        row.write_bin(&mut out, &mut acc).expect("write to output");
    }

    out.flush().expect("flush output");
}

fn get_rows(path: &str) -> Receiver<TableRow> {
    let path = path.to_string();
    let (s, r) = sync_channel(1024);
    spawn(move || {
        let table = BinTable::open(&path).expect("open bintable");

        for row in table {
            s.send(row).expect("send to channel");
        }
    });

    r
}
