use crate::{Entry, TableIndex, TableLakeReader};
use fallible_iterator::FallibleIterator;
use std::sync::mpsc::Sender;

pub struct DatabaseCollection {
    client: postgres::Client,
    table: String,
    limit: Option<usize>,
}

impl DatabaseCollection {
    pub fn new(client: postgres::Client, table: impl Into<String>) -> Self {
        DatabaseCollection {
            client,
            table: table.into(),
            limit: None,
        }
    }

    pub fn limit(self, limit: usize) -> Self {
        let limit = Some(limit);
        Self { limit, ..self }
    }
}

impl TableLakeReader for DatabaseCollection {
    fn read(&mut self, ch: Sender<Entry>) {
        let (query, params) = if let Some(limit) = self.limit {
            let query = "
                SELECT * FROM $1
                LIMIT $2
                ";
            let params = vec![self.table.clone(), format!("{limit}")];
            (query, params)
        } else {
            let query = "SELECT * FROM $1";
            let params = vec![self.table.clone()];
            (query, params)
        };

        let mut rows = self
            .client
            .query_raw(query, params)
            .expect("query database");

        while let Some(row) = rows.next().expect("read next row") {
            // both saved as `integer`
            let table_id: i32 = row.get("tableid");
            let column_id: i32 = row.get("colid");

            // saved as bigint
            let row_id: i64 = row.get("rowid");

            let tokenized = row.get("tokenized");
            let index = TableIndex::new(table_id as u32, column_id as u32, row_id as u64);

            ch.send((tokenized, index)).expect("send index to channel");
        }

        unimplemented!()
    }
}
