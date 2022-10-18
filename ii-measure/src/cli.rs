use std::str::FromStr;

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "inverted-index-measure",
    about = "Measure memory usage of inverted index implementations."
)]
pub struct Config {
    /// 0 > factor >= 1 (1 is default)
    /// how many percent of incoming rows should be used in inverted index.
    /// This is used to sample the sources.
    #[structopt(short, long)]
    pub factor: Option<f32>,

    #[structopt(short, long)]
    pub algorithm: CompressionAlgorithm,

    #[structopt()]
    pub table: String,

    #[structopt(short, long)]
    pub database: bool,

    /// Wether or not to include header in output csv
    #[structopt(short, long)]
    pub header: bool,

    /// if true, print only the csv header and exit
    #[structopt(long)]
    pub header_only: bool,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum CompressionAlgorithm {
    Baseline,
    DedupHash,
    DedupBTree,

    NS,
    NSRaw,

    Smaz,
    SmazRaw,

    FastPfor,
    FastPforRaw,

    SmazFastPfor,
    SmazNS,
}

impl CompressionAlgorithm {
    fn lookup() -> Vec<(CompressionAlgorithm, &'static str)> {
        use CompressionAlgorithm::*;
        vec![
            (Baseline, "baseline"),
            (DedupHash, "dedup_hash"),
            (DedupBTree, "dedup_btree"),
            (NS, "ns"),
            (NSRaw, "ns_raw"),
            (Smaz, "smaz"),
            (SmazRaw, "smaz_raw"),
            (FastPfor, "pfor"),
            (FastPforRaw, "pfor_raw"),
            (SmazFastPfor, "smaz+pfor"),
            (SmazNS, "smaz+ns"),
        ]
    }

    pub fn str(self) -> &'static str {
        CompressionAlgorithm::lookup()
            .into_iter()
            .find_map(|(elem, s)| (elem == self).then(|| s))
            .unwrap()
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CompressionAlgorithm::lookup()
            .into_iter()
            .find_map(|(elem, name)| (name == s).then(|| elem))
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
