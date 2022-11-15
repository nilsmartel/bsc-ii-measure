pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let maxlen = a.len().min(b.len());
    for i in 0..maxlen {
        if a[i] != b[i] {
            return i;
        }
    }

    maxlen
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Ordering {
    LessOrInHere,
    FoundAt(usize),
    Greater,
}
