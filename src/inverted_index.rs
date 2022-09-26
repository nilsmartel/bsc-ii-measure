use crate::table_lake::TableIndex;
use std::collections::*;

/// Interface that all implementations of the inverted index are desirde to
/// conform to.
pub trait InvertedIndex {
    fn get(&self, key: &str) -> Vec<TableIndex>;
}

impl InvertedIndex for Vec<(String, TableIndex)> {
    fn get(&self, key: &str) -> Vec<TableIndex> {
        self.iter()
            .filter(|(k, _)| k == key)
            .map(|(_, i)| *i)
            .collect::<Vec<_>>()
    }
}

impl InvertedIndex for HashMap<String, Vec<TableIndex>> {
    fn get(&self, key: &str) -> Vec<TableIndex> {
        match self.get(key) {
            None => Vec::new(),
            Some(v) => v.clone(),
        }
    }
}

impl InvertedIndex for BTreeMap<String, Vec<TableIndex>> {
    fn get(&self, key: &str) -> Vec<TableIndex> {
        match self.get(key) {
            None => Vec::new(),
            Some(v) => v.clone(),
        }
    }
}
