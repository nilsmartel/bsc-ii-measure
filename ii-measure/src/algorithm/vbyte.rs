use crate::inverted_index::InvertedIndex;
use crate::table_lake::*;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

/// List of vbyte encoded row and col ids,
/// gve encoded table ids.
#[derive(Clone)]
pub struct VBList {
    /// first `count` vbytes, then the rest using gve (ns)
    data: Box<[u8]>,
}

impl VBList {
    pub fn from_table_locations(vs: impl Iterator<Item = TableLocation>) -> Self {
        let mut cr = Vec::new();
        let mut t = Vec::new();
        for v in vs {
            cr.push(v.colid as u64);
            cr.push(v.rowid as u64);
            t.push(v.tableid);
        }

        let vdata = vbyte::compress_list(&cr);
        let vbytelen = vdata.len();

        let mut data = vbyte::compress(vbytelen as u64);
        data.extend(vdata);
        data.extend(group_varint_encoding::compress(t));
        data.shrink_to_fit();

        let data = data.into_boxed_slice();

        VBList { data }
    }

    pub fn locations(&self) -> Vec<TableLocation> {
        let (vbytelen, data) = vbyte::decompress(&self.data).unwrap();
        let vbytelen = vbytelen as usize;
        let crs = vbyte::decompress_list(&data[..vbytelen]).unwrap();
        let ts = group_varint_encoding::decompress(&data[vbytelen..]).collect();

        let len = crs.len() / 2;
        let mut locs: Vec<TableLocation> = Vec::with_capacity(len);

        for i in 0..len {
            let colid = crs[i * 2] as u32;
            let rowid = crs[i * 2 + 1] as u32;
            let tableid = ts[i];
            let loc = TableLocation {
                tableid,
                colid,
                rowid,
            };
            locs.push(loc);
        }

        locs
    }
}

// we're storing the overshooting length,
// as the implementation does not consider that elements may not come in blocks of precisely 4.
pub struct VByteEncoded {
    // first vbyte is `length` of vbyte strip.
    data: HashMap<String, VBList>,
}

impl VByteEncoded {
    pub fn new(receiver: Receiver<(String, TableLocation)>) -> (usize, Duration, Self) {
        let mut data = HashMap::new();
        let mut entry_count = 0;

        // we're using the index to group received indices
        // zzz is pretty late in the alphabet, so not the first thing that we receive. nice
        let mut group_id = String::from("zzzz");

        // we're using an intermediate buffer
        // to collect the integers we'd like to compress
        let mut current_buffer = Vec::<TableLocation>::with_capacity(256);

        let mut build_time = Duration::new(0, 0);

        for (index, location) in receiver {
            let starttime = Instant::now();

            if index != group_id {
                let entry = VBList::from_table_locations(current_buffer.iter().cloned());
                data.insert(group_id, entry);
                // set new index as group-indentifier
                group_id = index;

                // clear the buffer for new usage.
                current_buffer.clear();
            }

            current_buffer.push(location);

            build_time += starttime.elapsed();
            entry_count += 1;
        }

        (entry_count, build_time, Self { data })
    }
}

impl InvertedIndex<Vec<TableLocation>> for VByteEncoded {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        let data = self.data.get(key).expect("key to be present");
        data.locations()
    }
}

impl crate::util::RandomKeys for VByteEncoded {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        self.data.random_keys_potentially_ordered()
    }
}
