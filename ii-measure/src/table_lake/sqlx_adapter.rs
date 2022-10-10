use crate::{table_lake::TableLocation, Entry, TableLakeReader};
use rand::*;
use sqlx::{Pool, Postgres};
use std::sync::mpsc::SyncSender;
use tokio_stream::StreamExt;

pub struct SqlxCollection {
    pool: Pool<Postgres>,
    table: String,
    factor: Option<f32>,
}

impl SqlxCollection {
    pub fn new(pool: Pool<Postgres>, table: impl Into<String>, factor: Option<f32>) -> Self {
        SqlxCollection {
            pool,
            table: table.into(),
            factor,
        }
    }
}

impl TableLakeReader for SqlxCollection {
    fn read(&mut self, ch: SyncSender<Entry>) {
        eprintln!("read called");

        let query = format!(
            "
            SELECT tokenized, tableid, colid, rowid
            FROM {}
            ORDER BY tokenized
        ",
            self.table
        );

        let query = sqlx::query_as::<_, (String, i32, i32, i32)>(&query);
        let mut rows = query.fetch(&self.pool);
        let mut rng = thread_rng();

        let coroutine = async {
            while let Some(row) = rows.try_next().await.expect("read row from sqlx") {
                eprint!(".");

                if let Some(f) = self.factor {
                    let random_number = rng.gen::<f32>();
                    if random_number < f {
                        continue;
                    }
                }

                let (tokenized, tableid, colid, rowid) = row;
                ch.send((
                    tokenized,
                    TableLocation {
                        tableid: tableid as u32,
                        colid: colid as u32,
                        rowid: rowid as u64,
                    },
                ))
                .expect("send to channel");
            }
        };

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(coroutine);
    }
}
