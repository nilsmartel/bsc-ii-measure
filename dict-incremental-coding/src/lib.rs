use std::cmp::Ordering;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_insertion() {
        // randomly generated words
        let words = "absorb
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

        let mut d = Dict::<usize, 5>::new();
        for w in &words {
            d.push(w.as_bytes().to_vec(), w.len());
        }

        for w in words {
            let res = d.get(w.as_bytes());

            assert_eq!(res, Some(&w.len()), "expect key lenghts to match up");
        }
    }
}

#[derive(Default)]
pub struct Dict<V, const BLOCKSIZE: usize = 16> {
    keys: Vec<Block<BLOCKSIZE>>,
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

    pub fn value_at_index(&self, index: usize) -> &V {
        self.values.get(index).expect("index to be in range")
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let index = self.index_of(key)?;
        self.values.get(index)
    }

    // makes inserted values reliably available for retrieval
    pub fn flush_insert(&mut self) {
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

        let block = Block::<B>::new(&values);
        self.keys.push(block);
        self.current_block.clear();
    }

    pub fn values(&self) -> &[V] {
        &self.values
    }

    pub fn index_of(&self, key: &[u8]) -> Option<usize> {
        fn binary_search<const B: usize>(data: &[Block<B>], elem: &[u8]) -> Option<usize> {
            if data.is_empty() {
                return None;
            }

            let index = data.len() / 2;

            Some(match data[index].cmp(elem) {
                Ordering::Equal => index,
                Ordering::Less => index + 1 + binary_search(&data[(index + 1)..], elem)?,
                Ordering::Greater => binary_search(&data[..index], elem)?,
            })
        }

        let block_id = binary_search(&self.keys, key)?;

        for (i, v) in self.keys[block_id].to_vec().into_iter().enumerate() {
            if v == key {
                return Some(block_id * B + i);
            }
        }

        None
    }

    pub fn push(&mut self, key: Vec<u8>, value: V) {
        // actually it is vital to assert that our input data is sorted at this point.

        self.current_block.push((key, value));

        if self.current_block.len() == B {
            let values = self
                .current_block
                .iter()
                .map(|elem| &elem.0)
                .cloned()
                .collect::<Vec<_>>();

            self.values
                .extend(self.current_block.iter().map(|elem| elem.1.clone()));

            let block = Block::<B>::new(&values);
            self.keys.push(block);
            self.current_block.clear();
        }
    }
}

struct Block<const BLOCKSIZE: usize> {
    /*
        Scheme: prefix: <varint>data
        for b in Blocksize:
            <varint>data
    */
    data: Vec<u8>,
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let maxlen = a.len().min(b.len());
    for i in 0..maxlen {
        if a[i] != b[i] {
            return i;
        }
    }

    maxlen
}

impl<const B: usize> Block<B> {
    fn cmp(&self, other: &[u8]) -> Ordering {
        let values = self.to_vec();

        match (&values[0] as &[u8]).cmp(other) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => Ordering::Equal,
            Ordering::Less => match (&values[B - 1] as &[u8]).cmp(other) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Equal,
            },
        }
    }

    fn new(values: &[Vec<u8>]) -> Self {
        use varint_compression::compress;

        assert!(
            values.len() <= B,
            "expect size of values to be equal to block size"
        );

        let mut data = Vec::new();
        data.extend(compress(values[0].len() as u64));
        data.extend(&values[0]);

        let mut last = &values[0] as &[u8];
        for v in values.iter().skip(1) {
            let prefix = common_prefix_len(last, v);
            let v = &v[prefix..];
            data.extend(compress(prefix as u64));
            data.extend(compress(v.len() as u64));
            data.extend(v);
            last = v;
        }

        Block { data }
    }

    fn to_vec(&self) -> Vec<Vec<u8>> {
        use varint_compression::decompress;

        let mut v = Vec::with_capacity(B);

        let (n, input) = decompress(&self.data).unwrap();
        let n = n as usize;

        // push the first, uncompressed value to the vector
        v.push(input[..n].to_vec());

        let mut input = &input[n..];
        while !input.is_empty() {
            // decode the length of the prefix
            // and the length of the remaining substring
            let (prefixlen, rest) = decompress(input).unwrap();
            let (remainlen, rest) = decompress(rest).unwrap();

            let prefixlen = prefixlen as usize;
            let remainlen = remainlen as usize;

            // now we know exactly how many bytes are needed, allocate as much.
            let mut value = Vec::with_capacity(prefixlen + remainlen);

            // and push the correct amount of bytes from the previously decoded value
            // + the new information to this value
            value.extend(&v[v.len() - 1][..prefixlen]);
            value.extend(&rest[..remainlen]);

            // we have a new entry in the vector!
            v.push(value);

            input = &rest[remainlen..];
        }

        v
    }
}
