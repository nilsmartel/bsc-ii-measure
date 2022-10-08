use crate::table_lake::*;
use int_compression_4_wise::compress;
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
