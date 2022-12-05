use crate::inverted_index::{binary_search_by_index, InvertedIndex};
use crate::table_lake::*;
use crate::util::random_keys::{RandomKeys, DESIRED_KEY_COUNT};
use rand::random;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

pub struct SmazInvertedIndex {
    data: HashMap<Vec<u8>, Vec<TableLocation>>,
}

impl InvertedIndex<Vec<TableLocation>> for SmazInvertedIndex {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let key = fast_smaz::compress(key);
        self.data.get(&key).expect("to find key").clone()
    }
}

impl RandomKeys for SmazInvertedIndex {
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

pub(crate) fn smaz(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, SmazInvertedIndex) {
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
            let locations = current_buffer.to_vec();
            let compressed_index = fast_smaz::compress(&index);
            data.insert(compressed_index, locations);

            group_id = index;
            current_buffer.clear();
        }

        current_buffer.push(location);

        build_time += starttime.elapsed();
        entry_count += 1;
    }

    (entry_count, build_time, SmazInvertedIndex { data })
}

pub struct SmazInvertedIndexRaw {
    data: Vec<(Vec<u8>, TableLocation)>,
}

/// Baseline measure of data, the way it is present in database
pub fn smaz_raw(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, SmazInvertedIndexRaw) {
    let mut data = Vec::new();
    let mut build_time = Duration::new(0, 0);

    for (key, location) in receiver {
        let starttime = Instant::now();
        let mut key = fast_smaz::compress(&key);
        key.shrink_to_fit();

        data.push((key, location));

        build_time += starttime.elapsed();
    }

    let starttime = Instant::now();
    eprintln!("sorting data");
    data.sort();
    build_time += starttime.elapsed();

    (data.len(), build_time, SmazInvertedIndexRaw { data })
}

impl InvertedIndex<Vec<TableLocation>> for SmazInvertedIndexRaw {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let key = fast_smaz::compress(key);
        let key = &key as &[u8];

        fn get_start_point(a: &[(Vec<u8>, TableLocation)], index: usize, elem: &[u8]) -> Ordering {
            let value = &a[index].0 as &[u8];
            if index == 0 {
                return value.cmp(elem);
            }

            match value.cmp(elem) {
                Ordering::Equal => {
                    if (&a[index - 1].0 as &[u8]) < elem {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                }
                o => o,
            }
        }

        fn get_end_point(a: &[(Vec<u8>, TableLocation)], index: usize, elem: &[u8]) -> Ordering {
            let value = &a[index].0 as &[u8];
            if index + 1 == a.len() {
                return value.cmp(elem);
            }

            match value.cmp(elem) {
                Ordering::Equal => {
                    if (&a[index + 1].0 as &[u8]) < elem {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                }
                o => o,
            }
        }

        let startindex =
            binary_search_by_index(&self.data, 0, self.data.len(), get_start_point, key)
                .unwrap_or(0);

        let endindex =
            binary_search_by_index(&self.data, 0, self.data.len(), get_end_point, key).unwrap_or(6);

        let size = endindex - startindex;

        let mut v = Vec::with_capacity(size);

        for (_, location) in self.data[startindex..endindex].iter() {
            v.push(*location);
        }

        v
    }
}

impl crate::util::RandomKeys for SmazInvertedIndexRaw {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        Vec::new()
        // let mut v = Vec::with_capacity(self.data.len() / 10);

        // let mut s: &[u8] = &[];
        // for elem in self.data.iter() {
        //     let si = &elem.0 as &[u8];
        //     if s == si {
        //         continue;
        //     }

        //     s = si;
        //     v.push(si);
        // }

        // (0..crate::util::random_keys::DESIRED_KEY_COUNT)
        //     .map(|_| {
        //         let index = random::<f64>() * v.len() as f64;
        //         let s = v[index as usize];
        //         let s = fast_smaz::decompress(s).expect("smaz decompress key");

        //         String::from_utf8(s).expect("smaz valid utf8 string")
        //     })
        //     .collect()
    }
}
