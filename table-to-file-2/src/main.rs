mod db;

use sqlx::postgres::PgPoolOptions;
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

    let limit = 1_000_000;
    let mut offset = 0;

    let mut output = std::fs::File::create(&config.outfile).expect("to create outfile");

    loop {
        eprintln!("reading rows after size {offset}");

        let query = format!(
            "SELECT tokenized, tableid, colid, rowid
                FROM {corpus}
                ORDER BY tokenized
                LIMIT {limit}
                OFFSET {offset}"
        );
        eprintln!("{query}");

        let query = sqlx::query_as::<_, (String, i32, i32, i64)>(&query);

        let mut stream = query.fetch(&pool);

        let mut count = 0;
        while let Some(row) = stream.try_next().await? {
            let (tokenized, tableid, colid, rowid) = row;

            let row = bintable::TableRow {
                tokenized,
                tableid: tableid as u32,
                colid: colid as u32,
                rowid: rowid as u64,
            };

            row.write_bin(&mut output).expect("write to outfile");

            count += 1;
        }

        if count == 0 {
            break;
        }

        offset += limit;
    }

    Ok(())
}
