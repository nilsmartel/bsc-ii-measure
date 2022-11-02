use crate::util::*;
use rand::rngs::ThreadRng;
use rand::Rng;

use super::tablerow::TableRow;
use crate::tablerow::{ParseAcc, ReadError};
use std::fs::File;
use std::io::Read;

pub struct BinTableSampler {
    bintable: BinTable,
    factor: f32,
    rng: ThreadRng,
}
impl BinTableSampler {
    pub fn open(path: &str, factor: f32) -> std::io::Result<BinTableSampler> {
        // first search, if a better bintable exists beside the original one.
        let (path, factor) = if let Some((path, corpus)) = path.rsplit_once('/') {
            find_best_input(path, corpus, factor)
        } else {
            find_best_input(".", path, factor)
        }?;

        let bintable = BinTable::open(&path)?;
        let rng = rand::thread_rng();

        Ok(BinTableSampler {
            bintable,
            factor,
            rng,
        })
    }
}

impl Iterator for BinTableSampler {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.bintable.next()?;

            if self.rng.gen::<f32>() < self.factor {
                return Some(item);
            }
        }
    }
}

pub struct BinTable {
    reader: File,
    buffer: Vec<u8>,
    offset: usize,
    acc: ParseAcc,
}

impl BinTable {
    pub fn open(path: &str) -> std::io::Result<BinTable> {
        let reader = File::open(path)?;

        Ok(BinTable {
            reader,
            buffer: Vec::with_capacity(1024 * 8),
            offset: 0,
            acc: ParseAcc::default(),
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

        match TableRow::from_bin(fresh_data, &mut self.acc) {
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
                while (self.buffer.capacity() - self.offset) < n {
                    self.buffer.reserve(1024);
                }
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
