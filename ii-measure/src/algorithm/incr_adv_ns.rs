use std::{
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use crate::{
    inverted_index::InvertedIndex, table_lake::TableLocation, util::random_keys::DESIRED_KEY_COUNT,
};
use dict_incremental_coding_improved::Dict;
use rand::random;

/// List of u32s compressed using Group Varint Encoding (ns)
#[derive(Clone)]
struct CompressedLocations {
    data: Box<[u8]>,
}

impl CompressedLocations {
    fn new(vs: Vec<TableLocation>) -> Self {
        let data = vs.into_iter().flat_map(TableLocation::integers);
        let mut data = group_varint_encoding::compress(data);
        data.shrink_to_fit();

        let data = data.into_boxed_slice();

        Self { data }
    }

    fn get_data(&self) -> Vec<u32> {
        let mut data = group_varint_encoding::decompress(&self.data).collect();

        // if the last 3 pieces of data are all 0, remove them.
        // this can hhappen, because I decided to use 0 to extend our data to groups of 3.

        let i = data.len();
        if data[i - 1] == 0 && data[i - 2] == 0 && data[i - 3] == 0 {
            data.pop();
            data.pop();
            data.pop();
        }

        data
    }

    pub fn locations(&self) -> Vec<TableLocation> {
        let data = self.get_data();

        let mut i = 2;

        let mut locations = Vec::with_capacity(data.len() / 3);
        while i < data.len() {
            let t = TableLocation::from_integers(&[data[i - 2], data[i - 1], data[i]]);
            locations.push(t);
            i += 3;
        }

        locations
    }
}

pub struct InvertedIndexIncrementalCodingNS {
    dict: Dict<CompressedLocations, 16>,
}

impl InvertedIndexIncrementalCodingNS {
    pub fn new(
        receiver: Receiver<(String, TableLocation)>,
    ) -> (usize, Duration, InvertedIndexIncrementalCodingNS) {
        let mut dict = Dict::new();

        let mut build_time = Duration::new(0, 0);

        let mut count = 0;

        let (mut current_key, location_group) = receiver.recv().expect("first item from receiver");
        let mut location_group = vec![location_group];
        for (key, location) in receiver {
            count += 1;
            let starttime = Instant::now();

            if key != current_key {
                dict.push(
                    current_key.into_bytes(),
                    CompressedLocations::new(location_group.to_vec()),
                );
                location_group.clear();
                current_key = key;
            }

            location_group.push(location);

            build_time += starttime.elapsed();
        }

        let ii = InvertedIndexIncrementalCodingNS { dict };

        (count, build_time, ii)
    }
}

impl InvertedIndex<Vec<TableLocation>> for InvertedIndexIncrementalCodingNS {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let index = self
            .dict
            .index_of(key.as_bytes())
            .expect("to find key in dictionary");

        self.dict.values()[index].locations()
    }
}

impl crate::util::RandomKeys for InvertedIndexIncrementalCodingNS {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let max = self.dict.len() as f64;
        (0..DESIRED_KEY_COUNT)
            .map(|_| {
                let position = (random::<f64>() * max).floor() as usize;
                let bytes = self.dict.key_at_index(position);

                String::from_utf8(bytes).expect("bytes to be valid utf-8")
            })
            .collect()
    }
}
