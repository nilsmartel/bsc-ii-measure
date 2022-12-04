use std::str::FromStr;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum CompressionAlgorithm {
    Baseline,
    BaselineExact,

    DedupHash,
    DedupBTree,

    VByteArenaNSDedup,
    VByteNSDedup,
    VByteNSIncrDedup,

    NSDedup,
    NS,
    NSArena,

    SmazDedup,
    Smaz,

    FrontCodingBaseline,
    FrontCodingDedup,

    IncrCodingBaseline,
    IncrCodingDedup,

    IncrCodingDedupNS,

    IncrCodingAdvancedDedupNS,

    IncrCodingAdvancedDedupNSAdvanced,

    FastPforDedup,
    FastPforSplitDedup,

    SmazFastPforDedup,
    SmazNSDedup,
}

impl CompressionAlgorithm {
    fn lookup() -> Vec<(CompressionAlgorithm, &'static str)> {
        use CompressionAlgorithm::*;
        vec![
            (Baseline, "baseline"),
            (BaselineExact, "baselinex"),
            (DedupHash, "dedup_hash"),
            (DedupBTree, "dedup_btree"),
            (NSDedup, "ns+dedup"),
            (VByteNSDedup, "vbyte+ns+dedup"),
            (VByteArenaNSDedup, "vbyte+arena+ns+dedup"),
            (VByteNSIncrDedup, "vbyte+ns+incr+dedup"),
            (NS, "ns"),
            (NSArena, "ns_arena"),
            (Smaz, "smaz"),
            (FrontCodingBaseline, "frontcoding"),
            (FrontCodingDedup, "frontcoding+dedup"),
            (IncrCodingBaseline, "incr"),
            (IncrCodingDedup, "incr+dedup"),
            (IncrCodingDedupNS, "incr+dedup+ns"),
            (IncrCodingAdvancedDedupNS, "incr_adv+dedup+ns"),
            (IncrCodingAdvancedDedupNSAdvanced, "incr_adv+dedup+ns_adv"),
            (SmazDedup, "smaz+dedup"),
            (FastPforDedup, "pfor+dedup"),
            (FastPforSplitDedup, "pfor_split+dedup"),
            (SmazFastPforDedup, "smaz+pfor+dedup"),
            (SmazNSDedup, "smaz+ns+dedup"),
        ]
    }

    pub fn str(self) -> &'static str {
        CompressionAlgorithm::lookup()
            .into_iter()
            .find_map(|(elem, s)| (elem == self).then_some(s))
            .unwrap()
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        CompressionAlgorithm::lookup()
            .into_iter()
            .find_map(|(elem, name)| (name == s).then_some(elem))
            .ok_or_else(|| {
                let mut s = String::from("allowed: ");
                for name in Self::lookup().into_iter().map(|a| a.1) {
                    s += name;
                    s += " ";
                }
                s
            })
    }
}
