mod csv_collection;

pub use csv_collection::CSVCollection;

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
