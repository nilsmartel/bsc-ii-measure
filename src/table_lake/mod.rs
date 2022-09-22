mod csv_collection;
mod database;

pub use csv_collection::CSVCollection;
pub use database::*;

use std::sync::mpsc::Sender;

pub struct TableIndex {
    pub table_id: usize,
    pub row_id: usize,
    pub column_id: usize,
}

pub type Entry = (String, TableIndex);

pub trait TableLakeReader
where
    Self: Send,
{
    fn read(&mut self, ch: Sender<Entry>);
}
