use bintable2::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::spawn;

fn print_help() -> ! {
    eprintln!("sort-bintable <input> <outputname>");
    std::process::exit(0);
}

fn get_args() -> (String, String) {
    let mut a = std::env::args().skip(1);
    let input = a.next().unwrap_or_else(|| print_help());
    let output = a.next().unwrap_or_else(|| print_help());

    (input, output)
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Group {
    id: String,
    data: Vec<u32>,
}

impl Iterator for Group {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        let tokenized = self.id.clone();

        let e = "data to be in groups of 3";
        let rowid = self.data.pop().expect(e);
        let colid = self.data.pop().expect(e);
        let tableid = self.data.pop().expect(e);

        Some(TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        })
    }
}

fn main() {
    let (input, output) = get_args();

    if input == output {
        panic!("input must not be output");
    }

    eprintln!("streaming");
    let rows = get_rows(input);

    let mut groups = group(rows);

    eprintln!("sorting");
    groups.sort_unstable();

    // write back data

    let out = File::create(output).expect("open output file");
    let mut out = BufWriter::with_capacity(1024 * 1024 /*1Mb*/, out);
    let mut acc = ParseAcc::default();

    eprintln!("writing");
    for g in groups {
        for row in g {
            row.write_bin(&mut out, &mut acc).expect("write to output");

            drop(row)
        }
    }

    out.flush().expect("flush buffered writer");
}

fn group(rows: Receiver<TableRow>) -> Vec<Group> {
    let mut v = Vec::with_capacity(1024);

    let mut current = {
        let first = rows.recv().expect("first row");
        Group {
            id: first.tokenized,
            data: vec![first.tableid, first.colid, first.rowid],
        }
    };

    for TableRow {
        tokenized,
        tableid,
        colid,
        rowid,
    } in rows
    {
        if current.id != tokenized {
            v.push(current);
            current = Group {
                id: tokenized,
                data: vec![tableid, colid, rowid],
            };
        } else {
            current.data.extend([tableid, colid, rowid]);
        }
    }
    v.push(current);

    v
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
