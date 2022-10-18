use crate::inverted_index::InvertedIndex;
use crate::table_lake::*;
use crate::util::random_keys::{RandomKeys, DESIRED_KEY_COUNT};
use int_compression_4_wise::{compress, decompress};
use rand::random;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

pub struct SmazNsInvertedIndex {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl InvertedIndex<Vec<TableLocation>> for SmazNsInvertedIndex {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let key = fast_smaz::compress(key);
        let data = self.data.get(&key).expect("to find key");
        let data = decompress(data).collect();

        let mut locations = Vec::with_capacity(data.len() / 3);
        for i in (2..(data.len())).step_by(3) {
            let location = TableLocation {
                tableid: data[i - 2],
                colid: data[i - 1],
                rowid: data[i],
            };
            locations.push(location);
        }

        locations
    }
}

impl RandomKeys for SmazNsInvertedIndex {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let mut v = Vec::with_capacity(DESIRED_KEY_COUNT);
        let chance = DESIRED_KEY_COUNT as f64 / self.data.len() as f64;
        v.extend(
            self.data
                .keys()
                .filter(|_| random::<f64>() <= chance)
                .map(fast_smaz::decompress)
                .map(Result::unwrap)
                .map(String::from_utf8)
                .map(Result::unwrap),
        );
        v
    }
}

pub(crate) fn smaz_ns(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, SmazNsInvertedIndex) {
    let mut data = HashMap::new();
    let mut entry_count = 0;

    // we're using the index to group received indices
    let mut group_id = String::new();

    // we're using an intermediate buffer
    // to collect the integers we'd like to compress
    let mut current_buffer = Vec::with_capacity(256);

    let mut build_time = Duration::new(0, 0);

    for (index, location) in receiver {
        let starttime = Instant::now();

        if index != group_id {
            let locations = &current_buffer;
            let locations = compress(locations.iter().cloned().flat_map(TableLocation::integers));

            let compressed_index = fast_smaz::compress(&index);
            data.insert(compressed_index, locations);

            group_id = index;
            current_buffer.clear();
        }

        current_buffer.push(location);

        build_time += starttime.elapsed();
        entry_count += 1;
    }

    (entry_count, build_time, SmazNsInvertedIndex { data })
}
