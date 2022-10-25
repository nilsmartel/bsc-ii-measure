use std::fs::File;

use bintable2::*;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file
    #[structopt()]
    table: String,

    #[structopt(short, long)]
    print_rows: bool,

    #[structopt(long)]
    histogram: Option<String>,
}

fn basefile(s: &str) -> &str {
    if !s.contains('/') {
        return s;
    }

    s.rsplit_once('/').unwrap().1
}

fn main() {
    let Config {
        table,
        histogram,
        print_rows,
    } = Config::from_args();

    let bintable = BinTable::open(&table).expect("open bintable file");

    let mut values: u64 = 0;
    let mut distinct_values: u64 = 0;

    let mut total_length_tokenized: u64 = 0;
    let mut total_length_tableid: u64 = 0;
    let mut total_length_colid: u64 = 0;
    let mut total_length_rowid: u64 = 0;

    let mut hist = Stats::new();

    if print_rows {
        eprintln!("tokenized: [tableid, colid, rowid]")
    }

    {
        let mut last_value = String::from("lick the himalayan saltlamp");
        for TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        } in bintable
        {
            if print_rows {
                eprintln!("'{tokenized}': [{tableid}, {colid}, {rowid}]")
            }

            total_length_tokenized += tokenized.as_bytes().len() as u64;
            total_length_tableid += tableid as u64;
            total_length_colid += colid as u64;
            total_length_rowid += rowid as u64;

            hist.table(tableid as u64);
            hist.col(colid as u64);
            hist.row(rowid as u64);

            if tokenized != last_value {
                last_value = tokenized;
                distinct_values += 1;
            }

            values += 1;
        }
    }

    let mean_cardinality = values as f64 / distinct_values as f64;

    let values = values as f64;
    let cell_len = total_length_tokenized as f64 / values;
    let tableid = total_length_tableid as f64 / values;
    let colid = total_length_colid as f64 / values;
    let rowid = total_length_rowid as f64 / values;

    println!("table;values;distinct_values;mean_cardinality;avg_cell_len;avg_tableid;avg_colid;avg_rowid");

    let tablename = basefile(&table);
    println!("{tablename};{values};{distinct_values};{mean_cardinality};{cell_len};{tableid};{colid};{rowid}");

    if histogram.is_none() {
        return;
    }

    let histogram = histogram.unwrap();

    let mut f = File::create(histogram.clone()).expect("create file for histogram");
    use std::io::Write;

    writeln!(&mut f, "rows;cols;tableids").expect("write header of histogram");

    for i in 0..BINS {
        writeln!(
            &mut f,
            "{};{};{}",
            hist.row_bins[i], hist.col_bins[i], hist.tableid_bins[i],
        )
        .expect("to write row of histogram");
    }

    drop(f);
    let mut f = File::create(histogram + "-bitwidth.csv").expect("create bitwidth histogram file");
    writeln!(&mut f, "rows;cols;tableids").expect("write header of histogram");
    for i in 0..64 {
        writeln!(
            &mut f,
            "{};{};{}",
            hist.row_width[i], hist.col_width[i], hist.tableid_width[i],
        )
        .expect("to write row of histogram");
    }
}

const BINS: usize = 256;
const BIN_SPAN: u64 = 16;

#[derive(Debug)]
struct Stats {
    row_bins: [u64; BINS],
    col_bins: [u64; BINS],
    tableid_bins: [u64; BINS],

    row_width: [u64; 64],
    col_width: [u64; 64],
    tableid_width: [u64; 64],
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            row_bins: [0; BINS],
            col_bins: [0; BINS],
            tableid_bins: [0; BINS],

            row_width: [0; 64],
            col_width: [0; 64],
            tableid_width: [0; 64],
        }
    }

    pub fn row(&mut self, id: u64) {
        Stats::linear_bin(id, &mut self.row_bins);
        Stats::bit_bin(id, &mut self.row_width);
    }

    pub fn col(&mut self, id: u64) {
        Stats::linear_bin(id, &mut self.col_bins);
        Stats::bit_bin(id, &mut self.col_width);
    }

    pub fn table(&mut self, id: u64) {
        Stats::linear_bin(id, &mut self.tableid_bins);
        Stats::bit_bin(id, &mut self.tableid_width);
    }

    fn linear_bin(value: u64, bins: &mut [u64]) {
        let mut value = value / BIN_SPAN;

        if value >= bins.len() as u64 {
            value = bins.len() as u64 - 1;
        }

        bins[value as usize] += 1;
    }

    fn bit_bin(value: u64, bins: &mut [u64]) {
        let bitwidth = bitwidth(value);

        bins[bitwidth as usize] += 1;
    }
}

fn bitwidth(mut v: u64) -> u8 {
    let mut w = 1;

    while v > 1 {
        v >>= 1;
        w += 1;
    }

    w
}

#[cfg(test)]
mod tests {
    #[test]
    fn width_of_integers() {
        use super::bitwidth;

        assert_eq!(bitwidth(0), 1);
        assert_eq!(bitwidth(1), 1);
        assert_eq!(bitwidth(0b10), 2);
        assert_eq!(bitwidth(0b11), 2);
        assert_eq!(bitwidth(0b100), 3);
        assert_eq!(bitwidth(0b111), 3);
        assert_eq!(bitwidth(0b1000), 4);
        assert_eq!(bitwidth(0b1000000), 7);
        assert_eq!(bitwidth(0b1111111), 7);
        assert_eq!(bitwidth(0b10000000), 8);
        assert_eq!(bitwidth(0b11111111), 8);
        assert_eq!(bitwidth(0b100000000), 9);
        assert_eq!(bitwidth(0b111111111), 9);
        assert_eq!(bitwidth(0b1000000000), 10);
        assert_eq!(bitwidth(0b1111111111), 10);
    }
}
