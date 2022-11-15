mod block;
mod util;
use block::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incrementalcoding1() {
        let values = [b"aal".to_vec(), b"aachen".to_vec()];

        let b = Block::new(&values);
        let data = b.data;
        assert_eq!(&data[1..=3], b"aal", "expected aal to be uncompressed");
        assert_eq!(data[4], 2, "expected 2 bytes to be saved");
        assert_eq!(data[5], 4, "expected 4 bytes to compress aachen");
        assert_eq!(&data[6..], b"chen", "aachen => chen");
    }

    #[test]
    fn incrementalcoding2() {
        let values = [b"aal".to_vec(), b"aachen".to_vec(), b"aachiluah".to_vec()];

        let b = Block::new(&values);
        let data = &b.data;
        assert_eq!(&data[1..=3], b"aal", "expected aal to be uncompressed");
        assert_eq!(data[4], 2, "expected 2 bytes to be saved");
        assert_eq!(data[5], 4, "expected 4 bytes to compress aachen");
        assert_eq!(&data[6..10], b"chen", "aachen => chen");

        assert_eq!(data[10], 4, "4 bytes to be ommited for aachen => aachiluah");
        assert_eq!(data[11], 5, "5 bytes to be used for iluah");

        assert_eq!(
            &data[12..],
            b"iluah",
            "expected incremental encoding to transfer"
        );

        let result = b.to_vec();

        assert_eq!(result, values, "expected decoded values to match result");
    }

    #[test]
    fn prefixes() {
        let testcases = [
            ("aachen", "aachiluah", 4),
            ("anderthen", "mond", 0),
            ("arnold", "arsch", 2),
            ("babc", "aabc", 0),
            ("aal", "aal", 3),
            ("aaligatoah", "aaligatoah", 10),
            ("aaligatoah", "aaligatoahxyz", 10),
            ("aaligatoahxyz", "aaligatoah", 10),
        ];

        for (a, b, n) in testcases {
            assert_eq!(
                crate::util::common_prefix_len(a.as_bytes(), b.as_bytes()),
                n,
                "expected {a} and {b} to have a common prefix of {n}"
            )
        }
    }

    #[test]
    fn dictionary_insertion() {
        // randomly generated words
        let mut words = "absorb
animal
application
arrow
assertive
affect
attack
anger
ash
abundant
acid
appeal
activity
air
aware
afford
appearance
administration
accompany
anniversary
association
acquaintance
AIDS
accent
acquit
address
aquarium
am
approval
adult
apparatus
album
absence
academy
arch
abandon
avant-garde
acute
archive
apathy
autonomy
arm
adventure
advocate
allocation
agriculture
aunt
assume
affair
analyst"
            .lines()
            .collect::<Vec<_>>();
        words.sort();

        let mut d = Dict::<usize, 8>::new();
        for w in &words {
            d.push(w.as_bytes().to_vec(), w.len());
        }

        d.finish();

        for w in words {
            let res = d.get(w.as_bytes());

            assert_eq!(res, Some(&w.len()), "expect key {w} to be in dictionary");
        }
    }
}

#[derive(Default)]
pub struct Dict<V, const BLOCKSIZE: usize = 16> {
    keys: Vec<Block>,
    values: Vec<V>,
    current_block: Vec<(Vec<u8>, V)>,
}

impl<V, const B: usize> Dict<V, B>
where
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            current_block: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn key_at_index(&self, index: usize) -> Vec<u8> {
        let blockid = index / B;
        let position = index % B;

        self.keys[blockid].to_vec().remove(position)
    }

    pub fn collect_keys(&self) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(self.len());

        for b in &self.keys {
            result.extend(b.to_vec());
        }

        result
    }

    pub fn value_at_index(&self, index: usize) -> &V {
        self.values.get(index).expect("index to be in range")
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let index = self.index_of(key)?;
        self.values.get(index)
    }

    // makes inserted values reliably available for retrieval
    pub fn finish(&mut self) {
        if self.current_block.is_empty() {
            return;
        }

        let values = self
            .current_block
            .iter()
            .map(|elem| &elem.0)
            .cloned()
            .collect::<Vec<_>>();

        self.values
            .extend(self.current_block.iter().map(|elem| elem.1.clone()));

        let block = Block::new(&values);
        self.keys.push(block);
        self.current_block.clear();
    }

    pub fn values(&self) -> &[V] {
        &self.values
    }

    pub fn index_of(&self, key: &[u8]) -> Option<usize> {
        fn binary_search<const B: usize>(data: &[Block], elem: &[u8]) -> Option<usize> {
            if data.is_empty() {
                return None;
            }

            let index = data.len() / 2;

            Some(match data[index].cmp(elem) {
                util::Ordering::FoundAt(i) => index * B + i,
                util::Ordering::Greater => binary_search::<B>(&data[..index], elem)?,
                util::Ordering::LessOrInHere => {
                    match data[index + 1].cmp(elem) {
                        // if the next block is also less, we don't need to search this block at all.
                        util::Ordering::LessOrInHere => {
                            index + 1 + binary_search::<B>(&data[(index + 1)..], elem)?
                        }

                        util::Ordering::FoundAt(i) => (index + 1) * B + i,

                        // we now know, that it's supposedly in block `index`.
                        util::Ordering::Greater => {
                            let values = data[index].to_vec();
                            for (i, value) in values.into_iter().enumerate() {
                                if value == elem {
                                    return Some(index * B + i);
                                }
                            }

                            // Value is not in this block :/
                            return None;
                        }
                    }
                }
            })
        }

        binary_search::<B>(&self.keys, key)
    }

    /// Push a new key into the dictionary. Input MUST BE SORTED.
    pub fn push(&mut self, key: Vec<u8>, value: V) {
        // NOTE actually it is vital to assert that our input data is sorted at this point.

        self.current_block.push((key, value));

        if self.current_block.len() == B {
            let keys = self
                .current_block
                .iter()
                .map(|elem| &elem.0)
                .cloned()
                .collect::<Vec<_>>();

            self.values
                .extend(self.current_block.iter().map(|elem| elem.1.clone()));

            let block = Block::new(&keys);
            self.keys.push(block);
            self.current_block.clear();
        }
    }
}
