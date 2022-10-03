use crate::{algorithm::Compressed4Wise, table_lake::TableLocation};
use std::collections::*;

/// Interface that all implementations of the inverted index are desirde to
/// conform to.
pub trait InvertedIndex<O> {
    fn get(&self, key: &str) -> O;
}

impl InvertedIndex<Vec<TableLocation>> for Vec<(String, TableLocation)> {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        fn get_key(v: &(String, TableLocation)) -> &str {
            &v.0
        }

        match self.binary_search_by_key(&key, get_key) {
            Err(_) => Vec::new(),
            Ok(index) => {
                let elem = self[index].1;
                // TODO: come up with the right algorithm to return all elements
                vec![elem]
            }
        }
    }
}

impl InvertedIndex<Option<Vec<TableLocation>>> for HashMap<String, Vec<TableLocation>> {
    fn get(&self, key: &str) -> Option<Vec<TableLocation>> {
        self.get(key).cloned()
    }
}

impl InvertedIndex<Option<Vec<TableLocation>>> for BTreeMap<String, Vec<TableLocation>> {
    fn get(&self, key: &str) -> Option<Vec<TableLocation>> {
        self.get(key).cloned()
    }
}

impl InvertedIndex<Option<Vec<TableLocation>>> for Compressed4Wise {
    fn get(&self, key: &str) -> Option<Vec<TableLocation>> {
        use int_compression_4_wise::decompress;
        let v = {
            let (data, overshoot) = self.get(key)?;

            let mut values = decompress(data).collect();

            // cut of overshooting values
            for _ in 0..*overshoot {
                values.pop();
            }

            values
        };

        // second phase of decompression is recreating the TableIndices

        let mut ti = Vec::with_capacity(v.len() / 3);

        for i in (0..v.len()).step_by(3) {
            let tableid = v[i];
            let colid = v[i + 2];
            let rowid = v[i + 1] as u64;
            ti.push(TableLocation {
                tableid,
                colid,
                rowid,
            });
        }

        Some(ti)
    }
}
