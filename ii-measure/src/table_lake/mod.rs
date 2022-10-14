mod sqlx_adapter;
pub use bintable2::TableRow;

pub use sqlx_adapter::*;

use std::sync::mpsc::SyncSender;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TableLocation {
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u32,
}

impl TableLocation {
    pub fn integers(self) -> [u32; 3] {
        let TableLocation {
            tableid,
            colid,
            rowid,
        } = self;

        [tableid, colid, rowid]
    }
}

pub type Entry = (String, TableLocation);

/// Trait used to digest multiple tables
/// from various sources.
pub trait TableLakeReader
where
    Self: Send,
{
    fn read(&mut self, ch: SyncSender<Entry>);
}

impl<I: Iterator<Item = TableRow> + Send> TableLakeReader for I {
    fn read(&mut self, ch: SyncSender<Entry>) {
        for row in self {
            let TableRow {
                tokenized,
                tableid,
                colid,
                rowid,
            } = row;
            let location = TableLocation {
                tableid,
                colid,
                rowid,
            };
            ch.send((tokenized, location))
                .expect("streadm tablelocation");
        }
    }
}
