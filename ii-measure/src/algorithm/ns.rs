use crate::inverted_index::{binary_search_by_index, InvertedIndex};
use crate::table_lake::*;
use group_varint_encoding::{compress, decompress};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

// we're storing the overshooting length,
// as the implementation does not consider that elements may not come in blocks of precisely 4.
pub type Compressed4Wise = HashMap<String, (Vec<u8>, u8)>;

pub(crate) fn ns_4_wise(
    receiver: Receiver<(String, TableLocation)>,
) -> (usize, Duration, Compressed4Wise) {
    let mut ii: Compressed4Wise = HashMap::new();
    let mut entry_count = 0;

    // we're using the index to group received indices
    let mut group_id = String::new();

    // we're using an intermediate buffer
    // to collect the integers we'd like to compress
    let mut current_buffer = Vec::<u32>::with_capacity(256);

    let mut build_time = Duration::new(0, 0);

    for (index, location) in receiver {
        let starttime = Instant::now();

        if index != group_id {
            // 0, 1, 2, 3,
            let overshoot: u8 = (current_buffer.len() % 4) as u8;
            let overshoot = (4 - overshoot) % 4;

            let compressed_data = compress(current_buffer.iter().cloned());

            // set new index as group-indentifier
            group_id = index;

            ii.insert(group_id.clone(), (compressed_data, overshoot));

            // clear the buffer for new usage.
            current_buffer.clear();
        }

        current_buffer.extend(location.integers());

        build_time += starttime.elapsed();
        entry_count += 1;
    }

    (entry_count, build_time, ii)
}

pub struct InvIdxNsRaw {
    data: Vec<(String, Vec<u8>)>,
}

/// Baseline measure of data, the way it is present in database
pub fn ns_raw(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, InvIdxNsRaw) {
    let mut data = Vec::new();
    let mut build_time = Duration::new(0, 0);

    for (key, location) in receiver {
        let starttime = Instant::now();
        let mut location = compress(location.integers());
        // remove last byte of redundancy
        location.pop().unwrap();
        // shrink vector perfectly
        location.shrink_to_fit();

        data.push((key, location));

        build_time += starttime.elapsed();
    }

    eprintln!("entries: {}", data.len());
    eprint!("sorting");
    data.sort_unstable();
    eprint!(" complete");

    (data.len(), build_time, InvIdxNsRaw { data })
}

impl InvertedIndex<Vec<TableLocation>> for InvIdxNsRaw {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        fn get_start_point(a: &[(String, Vec<u8>)], index: usize, elem: &String) -> Ordering {
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

        fn get_end_point(a: &[(String, Vec<u8>)], index: usize, elem: &String) -> Ordering {
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

        if startindex > endindex || endindex - startindex > 1000 {
            eprintln!("=> '{key}'");
            eprintln!("[{}]: {}..{}", endindex - startindex, startindex, endindex);
        }

        let size = endindex - startindex;

        let mut v = Vec::with_capacity(size);

        // decode all
        for (_, location) in &self.data[startindex..endindex] {
            // append leading 0
            let mut location = location.to_vec();
            location.push(0);

            let location = decompress(&location).collect();
            let location = TableLocation::from_integers(&location);
            v.push(location);
        }

        v
    }
}

impl crate::util::RandomKeys for InvIdxNsRaw {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        self.data.random_keys_potentially_ordered()
    }
}
