use crate::inverted_index::{binary_search_by_index, InvertedIndex};
use crate::table_lake::*;
use group_varint_encoding::{compress, decompress};
use std::cmp::Ordering;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};


#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ArenaIndex {
    start: usize,
    length: usize,
}

pub struct NSIndex {
    data: Vec<(String, ArenaIndex)>,
    arena: Vec<u8>
}

/// Baseline measure of data, the way it is present in database
pub fn ns_arena(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, NSIndex) {
    let mut data = Vec::new();
    let mut build_time = Duration::new(0, 0);

    let mut arena = Vec::new();

    for (key, location) in receiver {
        let starttime = Instant::now();
        let mut location = compress(location.integers());
        // remove last byte of redundancy
        location.pop().unwrap();

        let start = arena.len();
        let length = location.len();
        arena.extend(location);


        data.push((key, ArenaIndex {start, length}));

        build_time += starttime.elapsed();
    }

    eprintln!("entries: {}", data.len());
    // sorting is required, because postgres does not return it's entries in the same fashion rust expects it.
    eprint!("sorting");
    data.sort_unstable();
    eprint!(" complete");

    (data.len(), build_time, NSIndex { data, arena })
}

impl InvertedIndex<Vec<TableLocation>> for NSIndex {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        fn get_start_point<T>(a: &[(String, T)], index: usize, elem: &String) -> Ordering {
            if index == 0 {
                return a[0].0.cmp(elem);
            }

            match a[index].0.cmp(elem) {
                Ordering::Equal => {
                    if &a[index - 1].0 < elem {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                }
                o => o,
            }
        }

        fn get_end_point<T>(a: &[(String, T)], index: usize, elem: &String) -> Ordering {
            if a.len() == index + 1 {
                return a[index].0.cmp(elem);
            }

            match a[index].0.cmp(elem) {
                Ordering::Equal => {
                    if &a[index + 1].0 > elem {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                }
                o => o,
            }
        }

        // just for the type checker
        let key = key.to_string();

        let startindex =
            binary_search_by_index(&self.data, 0, self.data.len(), get_start_point, &key)
                .unwrap_or(0);

        let endindex = binary_search_by_index(&self.data, 0, self.data.len(), get_end_point, &key)
            .unwrap_or(6);

        let size = endindex - startindex;

        let mut v = Vec::with_capacity(size);

        // decode all
        let mut buffer = Vec::with_capacity(32);
        for (_, location) in &self.data[startindex..endindex] {
            let ArenaIndex { start, length } = *location;
            let end = start + length;
            buffer.clear();
            buffer.extend(&self.arena[start..end]);
            buffer.push(0);

            let location = decompress(&buffer).collect();
            let location = TableLocation::from_integers(&location);
            v.push(location);
        }

        v
    }
}

impl crate::util::RandomKeys for NSIndex {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        self.data.random_keys_potentially_ordered()
    }
}
