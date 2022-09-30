use int_compression_4_wise::ListUInt32;

use crate::{algorithm::Compressed4Wise, table_lake::TableIndex};
use std::collections::*;

/// Interface that all implementations of the inverted index are desirde to
/// conform to.
pub trait InvertedIndex<O> {
    fn get(&self, key: &str) -> O;
}

impl InvertedIndex<Vec<TableIndex>> for Vec<(String, TableIndex)> {
    fn get(&self, key: &str) -> Vec<TableIndex> {
        fn get_key(v: &(String, TableIndex)) -> &str {
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

impl InvertedIndex<Option<Vec<TableIndex>>> for HashMap<String, Vec<TableIndex>> {
    fn get(&self, key: &str) -> Option<Vec<TableIndex>> {
        self.get(key).cloned()
    }
}

impl InvertedIndex<Option<Vec<TableIndex>>> for BTreeMap<String, Vec<TableIndex>> {
    fn get(&self, key: &str) -> Option<Vec<TableIndex>> {
        self.get(key).cloned()
    }
}

impl InvertedIndex<Option<Vec<TableIndex>>> for HashMap<String, ListUInt32> {
    fn get(&self, key: &str) -> Option<Vec<TableIndex>> {
        let v = self.get(key)?.collect();
        // second phase of decompression

        let mut ti = Vec::with_capacity(v.len() / 3);

        for i in (0..v.len()).step_by(3) {
            let table_id = v[i];
            let row_id = v[i + 1];
            let column_id = v[i + 2] as u64;
            ti.push(TableIndex {
                table_id,
                row_id,
                column_id,
            });
        }

        Some(ti)
    }
}

impl InvertedIndex<Option<Vec<TableIndex>>> for Compressed4Wise {
    fn get(&self, key: &str) -> Option<Vec<TableIndex>> {
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
            let table_id = v[i];
            let row_id = v[i + 1];
            let column_id = v[i + 2] as u64;
            ti.push(TableIndex {
                table_id,
                row_id,
                column_id,
            });
        }

        Some(ti)
    }
}
