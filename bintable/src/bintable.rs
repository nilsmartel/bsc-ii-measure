use super::tablerow::TableRow;
use std::fs::File;
use std::io::prelude::*;

pub struct BinTable {
    file: File,

    /// buffers some bytes into memory for parsing
    buffer: Vec<u8>,
    /// a buffer can hold information for 0 to n (n \in Nat) rows to parse.
    /// After parsing one row, we'd like to save a pointer to where in the parsing process we are inside the buffer.
    parsing_pointer: usize,
}

impl BinTable {
    pub fn open(path: &str) -> std::io::Result<BinTable> {
        let file = File::open(path)?;

        Ok(BinTable {
            file,
            buffer: vec![0u8; 1024],
            parsing_pointer: 0,
        })
    }
}

impl Iterator for BinTable {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        let tmpbuffer: &[u8] = &self.buffer[self.parsing_pointer..];
        if tmpbuffer.len() > 4 {
            if let Ok((row, rest)) = TableRow::from_bin(tmpbuffer) {
                // the buffer contained enough bytes to parse an entire row.
                let bytes_consumed = tmpbuffer.len() - rest.len();

                // advance the parsing pointer
                self.parsing_pointer += bytes_consumed;

                return Some(row);
            }
        }

        // seek more bytes into the buffer

        let bytes_remaining = tmpbuffer.to_vec();
        self.parsing_pointer = bytes_remaining.len();

        self.buffer.clear();
        self.buffer.extend(bytes_remaining);

        // fill up buffer with zeros.
        while self.buffer.len() < self.buffer.capacity() {
            self.buffer.push(0);
        }

        let bytes_read = self.file.read(&mut self.buffer).expect("to read from file");

        if bytes_read == 0 {
            if self.parsing_pointer == 0 {
                return None;
            } else {
                self.buffer.extend((0..1024).map(|_| 0));
                eprintln!("bufferlen: {}", self.buffer.len());
            }
        }

        self.next()
    }
}
