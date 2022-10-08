use bintable::*;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Bintable file
    #[structopt()]
    table: String,

    #[structopt(short, long)]
    header: bool,
}

fn main() {
    let Config { table, header } = Config::from_args();

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

    {
        let mut last_value = String::new();
        for row in bintable {
            eprint!(".");
            total_length_tokenized += row.tokenized.as_bytes().len() as u64;
            total_length_tableid += row.tableid as u64;
            total_length_colid += row.colid as u64;
            total_length_rowid += row.rowid as u64;

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
}
