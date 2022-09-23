use crate::table_lake::TableIndex;
use std::collections::*;

/// Interface that all implementations of the inverted index are desirde to
/// conform to.
pub trait InvertedIndex<'a> {
    fn get(&'a self, key: &str) -> Indices<'a>;
}

type Indices<'a> = RefOrOwn<'a, Vec<TableIndex>>;

use RefOrOwn::*;
pub enum RefOrOwn<'a, T> {
    Ref(&'a T),
    Own(T),
}

impl<'a, T> Default for RefOrOwn<'a, T>
where
    T: Default,
{
    fn default() -> Self {
        Own(T::default())
    }
}

impl<'a, T> From<&'a T> for RefOrOwn<'a, T> {
    fn from(t: &'a T) -> Self {
        Ref(t)
    }
}

impl<'a, T> From<T> for RefOrOwn<'a, T> {
    fn from(t: T) -> Self {
        Own(t)
    }
}

impl<'a> InvertedIndex<'a> for Vec<(String, TableIndex)> {
    fn get(&self, key: &str) -> Indices<'a> {
        let v = self
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, i)| *i)
            .collect::<Vec<_>>();

        Own(v)
    }
}

impl<'a> InvertedIndex<'a> for HashMap<String, Vec<TableIndex>> {
    fn get(&'a self, key: &str) -> Indices<'a> {
        let res = self.get(key).map(RefOrOwn::Ref);

        res.unwrap_or_default()
    }
}

impl<'a> InvertedIndex<'a> for BTreeMap<String, Vec<TableIndex>> {
    fn get(&'a self, key: &str) -> Indices<'a> {
        let res = self.get(key).map(RefOrOwn::Ref);

        res.unwrap_or_default()
    }
}
