use std::{
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use crate::{
    inverted_index::InvertedIndex, table_lake::TableLocation, util::random_keys::DESIRED_KEY_COUNT,
};
use dict_incremental_coding_improved::Dict;
use group_varint_encoding as gve;
use group_varint_offset_encoding as gvoe;
use rand::random;
use varint_compression;

/// List of u32s compressed using Group Varint Encoding (ns)
#[derive(Clone)]
struct CompressedLocations {
    // scheme:
    // tableids length in bytes (varint)
    // tableids gveo
    // (colids, rowids) (gve)
    data: Box<[u8]>,
}

impl CompressedLocations {
    fn new(vs: Vec<TableLocation>) -> Self {
        let mut tableids = Vec::with_capacity(vs.len());
        let mut ids = Vec::with_capacity(vs.len() * 2);

        for v in vs {
            tableids.push(v.tableid);
            ids.push(v.colid);
            ids.push(v.rowid);
        }

        let mut data = Vec::new();
        let tableids = gvoe::compress(tableids);
        data.extend(varint_compression::compress(tableids.len() as u64));
        data.extend(tableids);

        data.extend(gve::compress(ids));
    }

    fn get_data(&self) -> Vec<u32> {
        let (len, rest) = varint_compression::decompress(&self.data);
        let len = len as usize;
        let tableids = &rest[..len];
        let tableids = gvoe::decompress(&tableids);

        let ids = &rest[len..];
        let mut ids = gve::decompress(&ids).collect();

        let maxlen = (ids.len() / 2).min(tableids.len());

        let v = Vec::with_capacity(maxlen);

        for i in 0..maxlen {
            let tableid = tableids[i];
            let colid = ids[i * 2];
            let rowid = ids[i * 2 + 1];

            v.push(TableLocation {
                tableid,
                colid,
                rowid,
            });
        }

        v
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
