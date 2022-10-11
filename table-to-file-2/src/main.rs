mod db;

use bintable::TableRow;
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

    let mut output = std::fs::File::create(&config.outfile).expect("to create outfile");

    let query = format!(
        "SELECT tokenized, tableid, colid, rowid
            FROM {corpus}"
    );
    eprintln!("{query}");

    let query = sqlx::query_as::<_, RowWrap>(&query);

    let mut stream = query.fetch(&pool);

    let mut count = 0;
    while let Some(RowWrap(row)) = stream.try_next().await? {
        row.write_bin(&mut output).expect("write to outfile");

        count += 1;
        if count & 0xfff == 0 {
            eprintln!("{count} rows");
        }
    }

    Ok(())
}

struct RowWrap(TableRow);

impl<'r> FromRow<'r, PgRow> for RowWrap {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let tokenized: Option<String> = row.try_get(0)?;
        let tokenized = tokenized.unwrap_or_default();

        let tableid = get_number(row, 1) as u32;
        let colid = get_number(row, 2) as u32;
        let rowid = get_number(row, 3) as u64;

        Ok(Self(TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        }))
    }
}

fn get_number(row: &PgRow, index: usize) -> i64 {
    if let Ok(v) = row.try_get::<i64, usize>(index) {
        return v;
    }
    if let Ok(v) = row.try_get::<i32, usize>(index) {
        return v as i64;
    }
    if let Ok(v) = row.try_get::<i8, usize>(index) {
        return v as i64;
    }

    row.try_get::<i16, usize>(index).unwrap() as i64
}
