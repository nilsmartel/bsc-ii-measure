use crate::{algorithm::Compressed4Wise, table_lake::TableLocation};
use std::{cmp::Ordering, collections::*};

pub trait InvertedIndex<O> {
    fn get(&self, key: &str) -> O;
}

impl InvertedIndex<Vec<TableLocation>> for Vec<(String, TableLocation)> {
    fn get(&self, key: &str) -> Vec<TableLocation> {
        fn get_start_point(a: &[(String, TableLocation)], index: usize, elem: &String) -> Ordering {
            if index == 0 {
                return a[0].0.cmp(elem);
            }

            match a[index].0.cmp(elem) {
                Ordering::Equal => {
                    if &a[index - 1].0 < elem {
                        Ordering::Equal
                    } else {
                        // which one?
                        Ordering::Greater
                    }
                }
                o => o,
            }
        }

        fn get_end_point(a: &[(String, TableLocation)], index: usize, elem: &String) -> Ordering {
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
        let startindex = binary_search_by_index(self, 0, self.len(), get_start_point, &key)
            .expect("find element in collection");
        let endindex = binary_search_by_index(self, 0, self.len(), get_end_point, &key)
            .expect("find element in collection")
            + 1;

        self[startindex..endindex].iter().map(|a| a.1).collect()
    }
}

fn binary_search_by_index<T, T2>(
    a: &[T],
    start: usize,
    end: usize,
    f: impl Fn(&[T], usize, &T2) -> Ordering,
    elem: &T2,
) -> Option<usize> {
    if start == end || a.is_empty() {
        return None;
    }

    let mid = (end - start) / 2 + start;

    match f(a, mid, elem) {
        Ordering::Equal => Some(mid),
        // element in mid is smaller than pivot
        // desired element is on the right side of the middle point
        Ordering::Less => binary_search_by_index(a, mid, end, f, elem),
        // element in mid is greater than pivot
        // we search the left side for the element in question then.
        Ordering::Greater => binary_search_by_index(a, start, mid, f, elem),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binsearch_by_index() {
        let collection: Vec<i32> = (0..256).collect();

        fn f(a: &[i32], index: usize, elem: &i32) -> Ordering {
            a[index].cmp(elem)
        }

        for i in [7, 0, 4, 2, 6, 10, 255, 200] {
            let index = binary_search_by_index(&collection, 0, collection.len(), f, &i);
            assert_eq!(
                index,
                Some(i as usize),
                "{i} is inside the collection at position {i}"
            );
        }
    }

    #[test]
    fn sorted_vec_inverted_index_search() {
        let ii = "abbcccddddfffffgggggghhhhhiiiijjjkklmmnnnoooooppppppqqrrstuuuuuvw"
            .chars()
            .enumerate()
            .map(|(index, c)| {
                (
                    String::from(c),
                    TableLocation {
                        tableid: index as u32,
                        colid: 0,
                        rowid: 0,
                    },
                )
            })
            .collect::<Vec<_>>();

        let result = ii.get("c");

        let expected = (3..=5)
            .map(|tableid| TableLocation {
                tableid,
                rowid: 0,
                colid: 0,
            })
            .collect::<Vec<_>>();

        assert_eq!(result, expected);
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
