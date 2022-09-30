use int_compression_4_wise::ListUInt32;

use crate::table_lake::TableIndex;
use std::collections::*;

/// Interface that all implementations of the inverted index are desirde to
/// conform to.
pub trait InvertedIndex<O> {
    fn get(&self, key: &str) -> O;
}

impl InvertedIndex<Vec<TableIndex>> for Vec<(String, TableIndex)> {
    fn get(&self, key: &str) -> Vec<TableIndex> {
        self.iter()
            .filter(|(s, _)| s == key)
            .map(|(_, k)| *k)
            .collect()
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
