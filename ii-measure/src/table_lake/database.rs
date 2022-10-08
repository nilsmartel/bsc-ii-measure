use crate::{table_lake::TableLocation, Entry, TableLakeReader};
use bintable::TableRow;
use fallible_iterator::FallibleIterator;
use rand::*;
use std::sync::mpsc::Sender;

pub struct DatabaseCollection {
    client: postgres::Client,
    table: String,
    factor: Option<f32>,
}

impl DatabaseCollection {
    pub fn new(client: postgres::Client, table: impl Into<String>, factor: Option<f32>) -> Self {
        DatabaseCollection {
            client,
            table: table.into(),
            factor,
        }
    }
}

fn entry(row: &postgres::Row) -> Entry {
    let row: TableRow = row.into();
    let TableRow {
        tokenized,
        tableid,
        colid,
        rowid,
    } = row;

    (
        tokenized,
        TableLocation {
            tableid,
            colid,
            rowid,
        },
    )
}

impl TableLakeReader for DatabaseCollection {
    fn read(&mut self, ch: Sender<Entry>) {
        let limit = 10_000;
        let mut offset = 0;
        loop {
            let query = format!(
                "
                SELECT tokenized, tableid, colid, rowid
                FROM {}
                ORDER BY tokenized
                LIMIT {limit}
                OFFSET {offset}
            ",
                self.table
            );

            eprintln!("execute query");
            let rows = self.client.query(&query, &[]).expect("query database");

            eprintln!("retrieved {} rows", rows.len());

            if rows.is_empty() {
                return;
            }

            if let Some(f) = self.factor {
                let mut rng = rand::thread_rng();
                for row in rows {
                    if rng.gen::<f32>() < f {
                        continue;
                    }
                    let e = entry(&row);
                    ch.send(e).expect("send index to channel");
                }
            } else {
                for row in rows {
                    let e = entry(&row);
                    ch.send(e).expect("send index to channel");
                }
            }

            offset += limit;
        }
    }
}
