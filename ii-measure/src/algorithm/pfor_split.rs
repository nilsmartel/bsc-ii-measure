use fastpfor::Codec;

use crate::inverted_index::InvertedIndex;
use crate::table_lake::*;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ArenaIndex {
    start: usize,
    length: u32,
    uncompressed_length: u32,
}

pub struct IIFastPforSplit {
    ii: HashMap<String, ArenaIndex>,
    compressed_data: Vec<u32>,
    codec: Codec,
}

// EXAKT pfor algorithm
pub fn pforsplit(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, IIFastPforSplit) {
    let codec = Codec::simdfastpfor128();

    let mut ii = HashMap::new();
    let mut build_time = Duration::new(0, 0);

    let mut compressed_data = vec![0u32; 1024 * 1024];
    // Offset into the compressed data itself.
    let mut offset = compressed_data.as_ptr().align_offset(16);

    let mut count = 0;

    {
        let (mut curr_key, loc) = receiver.recv().expect("first item");
        let mut curr_group = Vec::from_iter(loc.integers());

        for (key, location) in receiver {
            count += 1;
            let starttime = Instant::now();

            if key != curr_key {
                // make sure that compressed data has at least 4 times the amount of data available, as the uncompressed data needs.
                while curr_group.len() * 4 > compressed_data[offset..].len() {
                    compressed_data.extend((0..1024).map(|_| 0));
                }

                let compressed_data = &mut compressed_data[offset..];

                // compress integers of current group
                let written = codec
                    .compress(&curr_group, compressed_data)
                    .expect("no buffer overflow");

                // calculate position of compressed data inside buffer
                let start = offset;
                let length = written as u32;
                let uncompressed_length = curr_group.len() as u32;

                // offset compressed data
                offset += written;

                let index = ArenaIndex {
                    start,
                    length,
                    uncompressed_length,
                };
                ii.insert(curr_key, index);

                curr_key = key;
                curr_group.clear();
            }

            curr_group.extend(location.integers());

            build_time += starttime.elapsed();
        }
    }

    {
        let starttime = Instant::now();

        eprintln!("{offset} actual length");
        eprintln!("{} allocated", compressed_data.capacity());

        compressed_data.resize(offset, 0);
        compressed_data.shrink_to(offset);

        build_time += starttime.elapsed();
    }

    let ii = IIFastPforSplit {
        ii,
        compressed_data,
        codec,
    };
    (count, build_time, ii)
}

impl InvertedIndex<Vec<TableLocation>> for IIFastPforSplit {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let index = *self.ii.get(key).expect("to find index");

        let compressed_data =
            &self.compressed_data[index.start..(index.start + index.length as usize)];
        let mut destination = vec![0; index.uncompressed_length as usize];

        self.codec
            .decompress(compressed_data, &mut destination)
            .expect("decompress data");

        let mut tables = Vec::with_capacity(destination.len());
        for i in (0..destination.len()).step_by(3) {
            let l = TableLocation {
                tableid: destination[i],
                colid: destination[i + 1],
                rowid: destination[i + 2],
            };

            tables.push(l);
        }

        tables
    }
}

impl crate::util::RandomKeys for IIFastPforSplit {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        self.ii.random_keys_potentially_ordered()
    }
}
