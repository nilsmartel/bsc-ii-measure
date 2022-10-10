use crate::{table_lake::TableLocation, Entry, TableLakeReader};
use bintable::TableRow;
use fallible_iterator::FallibleIterator;
use rand::*;
use std::sync::mpsc::SyncSender;

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
    fn read(&mut self, ch: SyncSender<Entry>) {
        let query = format!(
            "
                SELECT tokenized, tableid, colid, rowid
                FROM {}
                ORDER BY tokenized
            ",
            self.table
        );

        let params: [bool; 0] = [];
        let mut rows = self
            .client
            .query_raw(&query, &params)
            .expect("query database");

        let mut rng = rand::thread_rng();
        while let Some(row) = rows.next().expect("read next row") {
            if let Some(f) = self.factor {
                if rng.gen::<f32>() > f {
                    continue;
                }
            }
            let e = entry(&row);
            ch.send(e).expect("send index to channel");
        }
    }
}
