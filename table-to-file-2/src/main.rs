mod db;

use std::io::Write;

use bintable2::TableRow;
use sqlx::{postgres::PgPoolOptions, FromRow, Postgres};
use sqlx::{postgres::PgRow, Row};
use tokio_stream::StreamExt;

use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    #[structopt()]
    corpus: String,

    #[structopt()]
    outfile: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let config = Config::from_args();
    let corpus = &config.corpus;

    if config.outfile.is_empty() {
        panic!("outfile mustnt be empty");
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db::client_str())
        .await?;

    let output = std::fs::File::create(&config.outfile).expect("to create outfile");
    let mut output = std::io::BufWriter::new(output);

    let query = format!(
        "SELECT tokenized, tableid, colid, rowid
            FROM {corpus}"
    );
    println!("{query}");

    let query = sqlx::query_as::<_, TableRow>(&query);

    let mut stream = query.fetch(&pool);

    let mut count = 0;
    let mut acc = bintable2::ParseAcc::default();
    while let Some(row) = stream.try_next().await? {
        row.write_bin(&mut output, &mut acc)
            .expect("write to outfile");

        count += 1;
        if count & 0xfff == 0 {
            println!("{count} rows");
        }
    }

    output.flush().expect("flush file");

    Ok(())
}
