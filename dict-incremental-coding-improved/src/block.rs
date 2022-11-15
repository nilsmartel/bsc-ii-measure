use crate::util;
use crate::util::common_prefix_len;
use std::cmp::Ordering;

use varint_compression::decompress;

pub(crate) struct Block {
    /*
        Scheme: prefix: <varint>data
        for b in Blocksize:
            <varint>data
    */
    pub(crate) data: Box<[u8]>,
}

impl Block {
    pub(crate) fn cmp(&self, other: &[u8]) -> util::Ordering {
        let first = self.first();

        match first.cmp(other) {
            Ordering::Greater => util::Ordering::Greater,
            Ordering::Equal => util::Ordering::FoundAt(0),
            Ordering::Less => util::Ordering::LessOrInHere,
            // match (&values[values.len() - 1] as &[u8]).cmp(other) {
            // Ordering::Less => Ordering::Less,
            // Ordering::Equal => Ordering::Equal,
            // Ordering::Greater => Ordering::Equal,
            // },
        }
    }

    pub fn new(values: &[Vec<u8>]) -> Self {
        use varint_compression::compress;

        let mut data = Vec::new();
        data.extend(compress(values[0].len() as u64));
        data.extend(&values[0]);

        let mut last = &values[0] as &[u8];
        for v in values.iter().skip(1) {
            let prefixlen = common_prefix_len(last, v);
            last = v;
            let v = &v[prefixlen..];
            // first compress the length of the prefix
            data.extend(compress(prefixlen as u64));
            // then compress the length of the remaining bytes
            data.extend(compress(v.len() as u64));
            // and finally the remaining bytes
            data.extend(v);
        }

        data.shrink_to_fit();
        let data = data.into_boxed_slice();

        Block { data }
    }

    pub fn first(&self) -> &[u8] {
        let (n, input) = decompress(&self.data).unwrap();
        let n = n as usize;
        &input[..n]
    }

    pub fn to_vec(&self) -> Vec<Vec<u8>> {
        let mut v = Vec::new();

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
