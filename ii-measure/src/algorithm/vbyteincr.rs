use std::{
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use crate::{
    inverted_index::InvertedIndex, table_lake::TableLocation, util::random_keys::DESIRED_KEY_COUNT,
};
use dict_incremental_coding_improved::Dict;
use rand::random;

use super::vbyte::VBList;

pub struct VByteEncoded {
    dict: Dict<VBList, 16>,
}

impl VByteEncoded {
    pub fn new(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, VByteEncoded) {
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
                    VBList::from_table_locations(location_group.iter().cloned()),
                );
                location_group.clear();
                current_key = key;
            }

            location_group.push(location);

            build_time += starttime.elapsed();
        }

        let ii = VByteEncoded { dict };

        (count, build_time, ii)
    }
}

impl InvertedIndex<Vec<TableLocation>> for VByteEncoded {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let index = self
            .dict
            .index_of(key.as_bytes())
            .expect("to find key in dictionary");

        self.dict.values()[index].locations()
    }
}

impl crate::util::RandomKeys for VByteEncoded {
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
