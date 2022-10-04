use crate::{table_lake::TableLocation, Entry, TableLakeReader};
use bintable::TableRow;
use std::sync::mpsc::Sender;

pub struct DatabaseCollection {
    client: postgres::Client,
    table: String,
    limit: usize,
}

impl DatabaseCollection {
    pub fn new(client: postgres::Client, table: impl Into<String>, limit: usize) -> Self {
        DatabaseCollection {
            client,
            table: table.into(),
            limit,
        }
    }
}

impl TableLakeReader for DatabaseCollection {
    fn read(&mut self, ch: Sender<Entry>) {
        let query = if self.limit != 0 {
            format!(
                "
                SELECT tokenized, tableid, colid, rowid FROM {}
                ORDER BY tokenized
                LIMIT {}
            ",
                self.table, self.limit
            )
        } else {
            format!(
                "
                SELECT tokenized, tableid, colid, rowid FROM {}
                ORDER BY tokenized
            ",
                self.table
            )
        };

        println!("execute query");
        let rows = self.client.query(&query, &[]).expect("query database");

        println!("retrieved {} rows", rows.len());

        for row in rows {
            let row: TableRow = (&row).into();
            let TableRow {
                tokenized,
                tableid,
                colid,
                rowid,
            } = row;
            ch.send((
                tokenized,
                TableLocation {
                    tableid,
                    colid,
                    rowid,
                },
            ))
            .expect("send index to channel");
        }
    }
}

// slow implementation, that works for big loads.
// but we have 1/2T RAM. Lets put it to use.

// impl TableLakeReader for DatabaseCollection {
//     fn read(&mut self, ch: Sender<Entry>) {
//         let query = format!(
//             "
//                 SELECT * FROM {}
//                 LIMIT {}
//             ",
//             self.table, self.limit
//         );

//         let params: [bool; 0] = [];
//         let mut rows = self
//             .client
//             .query_raw(&query, params)
//             .expect("query database");

//         while let Some(row) = rows.next().expect("read next row") {
//             // both saved as `integer`
//             let table_id: i32 = row.get("tableid");
//             let column_id: i32 = row.get("colid");

//             // saved as bigint
//             let row_id: i64 = row.get("rowid");

//             let tokenized = row.get("tokenized");
//             let index = TableIndex::new(table_id as u32, column_id as u32, row_id as u64);

//             ch.send((tokenized, index)).expect("send index to channel");
//         }
//     }
// }
