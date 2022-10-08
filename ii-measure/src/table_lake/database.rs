use crate::{table_lake::TableLocation, Entry, TableLakeReader};
use bintable::TableRow;
use fallible_iterator::FallibleIterator;
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

// impl TableLakeReader for DatabaseCollection {
//     fn read(&mut self, ch: Sender<Entry>) {
//         let query = format!(
//             "
//                 SELECT tokenized, tableid, colid, rowid FROM {}
//                 ORDER BY tokenized
//             ",
//             self.table,
//         );

//         eprintln!("execute query");
//         let rows = self.client.query(&query, &[]).expect("query database");

//         eprintln!("retrieved {} rows", rows.len());

//         if let Some(f) = self.factor {
//             let mut rng = rand::thread_rng();
//             for row in rows {
//                 if rng.gen::<f32>() < f {
//                     continue;
//                 }
//                 let e = entry(&row);
//                 ch.send(e).expect("send index to channel");
//             }
//         } else {
//             for row in rows {
//                 let e = entry(&row);
//                 ch.send(e).expect("send index to channel");
//             }
//         }
//     }
// }

// slow implementation, that works for big loads.
// but we have 1/2T RAM. Lets put it to use.

impl TableLakeReader for DatabaseCollection {
    fn read(&mut self, ch: Sender<Entry>) {
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
            .query_raw(&query, params)
            .expect("query database");

        let mut i = 0;
        while let Some(row) = rows.next().expect("read next row") {
            let e = entry(&row);
            ch.send(e).expect("send index to channel");
            i += 1;

            if i & 0x3ff == 0 {
                eprint!(".");
            }
        }
    }
}
