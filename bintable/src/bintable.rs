use crate::tablerow::ReadError;

use super::tablerow::TableRow;
use std::fs::File;
use std::io::Read;

pub struct BinTable {
    reader: File,
    buffer: Vec<u8>,
    offset: usize,
}

impl BinTable {
    pub fn open(path: &str) -> std::io::Result<BinTable> {
        let reader = File::open(path)?;

        Ok(BinTable {
            reader,
            buffer: Vec::with_capacity(1024 * 8),
            offset: 0,
        })
    }
}

impl Iterator for BinTable {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.len() == self.offset {
            self.buffer.clear();
            self.offset = 0;
            let space = self.buffer.capacity();

            let n = self
                .reader
                .by_ref()
                .take(space as u64)
                .read_to_end(&mut self.buffer)
                .expect("to read file");

            if n == 0 {
                return None;
            }
        }

        let fresh_data = &self.buffer[self.offset..];

        match TableRow::from_bin(fresh_data) {
            Ok((row, rest)) => {
                self.offset = self.buffer.len() - rest.len();
                return Some(row);
            }
            Err(ReadError::InitialNumber) => {
                // we only have very tiny end of buffer and need to seek more.
                let tmp = fresh_data.to_vec();
                self.offset = tmp.len();
                self.buffer.clear();
                self.buffer.extend(tmp);

                // now fill the buffer
            }
            Err(ReadError::Needed(n)) => {
                eprint!("buffer to short ");
                while (self.buffer.capacity() - self.offset) < n {
                    self.buffer.reserve(1024);
                    eprint!("... ");
                }
                eprintln!();
            }
        }

        // read at least n bytes

        let space = self.buffer.capacity() - self.buffer.len();
        self.reader
            .by_ref()
            .take(space as u64)
            .read_to_end(&mut self.buffer)
            .expect("to read file");

        self.next()
    }
}
