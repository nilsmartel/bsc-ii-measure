use crate::tablerow::ReadError;

use super::tablerow::TableRow;
use std::fs::File;
use std::io::{prelude::*, BufReader};

pub struct BinTable {
    reader: BufReader<File>,
    buffer: Vec<u8>,
}

impl BinTable {
    pub fn open(path: &str) -> std::io::Result<BinTable> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(BinTable {
            reader,
            buffer: Vec::with_capacity(1024),
        })
    }
}

impl Iterator for BinTable {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.clear();

        loop {
            _ = self
                .reader
                .by_ref()
                .take(4)
                .read_to_end(&mut self.buffer)
                .ok()?;

            match TableRow::from_bin(&self.buffer) {
                Ok(_) => unreachable!("this should be impossible"),
                Err(ReadError::InitialNumber) => continue,
                Err(ReadError::Needed(n)) => {
                    self.reader
                        .by_ref()
                        .take(n as u64)
                        .read_to_end(&mut self.buffer)
                        .expect("read complete tablerow");
                    break;
                }
            }
        }

        match TableRow::from_bin(&self.buffer) {
            Err(e) => panic!("reading complete row {:#?}", e),
            Ok((row, rest)) => {
                if !rest.is_empty() {
                    panic!("{} bytes left in buffer for {row:?}", rest.len());
                }

                return Some(row);
            }
        }
    }
}
