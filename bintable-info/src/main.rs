use std::fs::File;

use bintable::*;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file
    #[structopt()]
    table: String,

    #[structopt(short, long)]
    header: bool,

    #[structopt(long)]
    histogram: Option<String>,
}

fn main() {
    let Config {
        table,
        header,
        histogram,
    } = Config::from_args();

    if header {
        println!("table;values;distinct_values;mean_cardinality;avg_cell_len;avg_tableid;avg_colid;avg_rowid");
    }

    let bintable = BinTable::open(&table).expect("open bintable file");

    let mut values: u64 = 0;
    let mut distinct_values: u64 = 0;

    let mut total_length_tokenized: u64 = 0;
    let mut total_length_tableid: u64 = 0;
    let mut total_length_colid: u64 = 0;
    let mut total_length_rowid: u64 = 0;

    let mut hist = Stats::new();

    {
        let mut last_value = String::new();
        for row in bintable {
            total_length_tokenized += row.tokenized.as_bytes().len() as u64;
            total_length_tableid += row.tableid as u64;
            total_length_colid += row.colid as u64;
            total_length_rowid += row.rowid as u64;

            hist.table(row.tableid as u64);
            hist.col(row.colid as u64);
            hist.row(row.rowid as u64);

            if row.tokenized != last_value {
                last_value = row.tokenized;
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

    println!("{table};{values};{distinct_values};{mean_cardinality};{cell_len};{tableid};{colid};{rowid}");

    if histogram.is_none() {
        return;
    }

    let histogram = histogram.unwrap();

    let mut f = File::create(histogram).expect("create file for histogram");
    use std::io::Write;

    writeln!(&mut f, "rows;cols;tableids").expect("write header of histogram");

    for i in 0..BINS {
        writeln!(
            &mut f,
            "{};{};{}",
            hist.row_bins[i], hist.row_bins[i], hist.row_bins[i],
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
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            row_bins: [0; BINS],
            col_bins: [0; BINS],
            tableid_bins: [0; BINS],
        }
    }

    pub fn row(&mut self, id: u64) {
        Stats::log_stat(id, &mut self.row_bins);
    }

    pub fn col(&mut self, id: u64) {
        Stats::log_stat(id, &mut self.col_bins);
    }

    pub fn table(&mut self, id: u64) {
        Stats::log_stat(id, &mut self.tableid_bins);
    }

    fn log_stat(value: u64, bins: &mut [u64]) {
        let mut value = value / BIN_SPAN;

        if value >= bins.len() as u64 {
            value = bins.len() as u64 - 1;
        }

        bins[value as usize] += 1;
    }
}
