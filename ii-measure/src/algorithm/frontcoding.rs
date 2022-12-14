use std::{
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use crate::{
    inverted_index::InvertedIndex, table_lake::TableLocation, util::random_keys::DESIRED_KEY_COUNT,
};
use dict_front_coding::Dict;
use rand::random;

pub struct IIFrontcoding {
    dict: Dict<Vec<TableLocation>, 8>,
}

pub fn frontcoding(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, IIFrontcoding) {
    let mut dict = Dict::new();

    let mut build_time = Duration::new(0, 0);

    let mut count = 0;

    let (mut current_key, location_group) = receiver.recv().expect("first item from receiver");
    let mut location_group = vec![location_group];
    for (key, location) in receiver {
        count += 1;
        let starttime = Instant::now();

        if key != current_key {
            dict.push(current_key.into_bytes(), location_group.to_vec());
            location_group.clear();
            current_key = key;
        }

        location_group.push(location);

        build_time += starttime.elapsed();
    }

    let ii = IIFrontcoding { dict };

    (count, build_time, ii)
}

impl InvertedIndex<Vec<TableLocation>> for IIFrontcoding {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let index = self
            .dict
            .index_of(key.as_bytes())
            .expect("to find key in dictionary");

        self.dict.values()[index].clone()
    }
}

impl crate::util::RandomKeys for IIFrontcoding {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let max = self.dict.len() as f64;
        (0..DESIRED_KEY_COUNT)
            .map(|_| {
                let position = (random::<f64>() * max).floor() as usize;
                let bytes = self.dict.key(position);

                String::from_utf8(bytes).expect("bytes to be valid utf-8")
            })
            .collect()
    }
}

pub struct IIFrontcodingBaseline {
    dict: Dict<TableLocation, 8>,
}

pub fn frontcoding_baseline(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, IIFrontcodingBaseline) {
    let mut dict = Dict::new();

    let mut build_time = Duration::new(0, 0);

    let mut count = 0;

    for (key, location) in receiver {
        count += 1;
        let starttime = Instant::now();

        dict.push(key.into_bytes(), location);

        build_time += starttime.elapsed();
    }

    let ii = IIFrontcodingBaseline { dict };

    (count, build_time, ii)
}

impl InvertedIndex<Vec<TableLocation>> for IIFrontcodingBaseline {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        // we do this twice. More than anything to emulate the binary search
        let _index = self
            .dict
            .index_of(key.as_bytes())
            .expect("to find key in dictionary");

        let index = self
            .dict
            .index_of(key.as_bytes())
            .expect("to find key in dictionary");

        let firstvalue = self.dict.values()[index];

        // Note: this implementation is faulty
        // and does not truly return actual values.
        vec![firstvalue]
    }
}

impl crate::util::RandomKeys for IIFrontcodingBaseline {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let max = self.dict.len() as f64;
        (0..DESIRED_KEY_COUNT)
            .map(|_| {
                let position = (random::<f64>() * max).floor() as usize;
                let bytes = self.dict.key(position);

                String::from_utf8(bytes).expect("bytes to be valid utf-8")
            })
            .collect()
    }
}
