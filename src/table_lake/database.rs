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
            println!("{:#?}", row);
        }

        unimplemented!()
    }
}
