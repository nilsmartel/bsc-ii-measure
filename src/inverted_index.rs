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
            .map(|(_, k)|*k)
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