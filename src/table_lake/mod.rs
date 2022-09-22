mod database;

pub use database::*;

use std::sync::mpsc::Sender;

pub struct TableIndex {
    pub table_id: u32,
    pub row_id: u32,
    pub column_id: u64,
}

impl TableIndex {
    pub fn new(table_id: u32, row_id: u32, column_id: u64) -> Self {
        Self {
            table_id,
            row_id,
            column_id,
        }
    }
}

pub type Entry = (String, TableIndex);

pub trait TableLakeReader
where
    Self: Send,
{
    fn read(&mut self, ch: Sender<Entry>);
}
