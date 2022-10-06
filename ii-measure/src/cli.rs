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
    pub bintable: bool,

    /// Wether or not to include header in output csv
    #[structopt(short, long)]
    pub header: bool,

    /// if true, print only the csv header and exit
    #[structopt(long)]
    pub header_only: bool,
}

#[derive(Copy, Clone)]
pub enum CompressionAlgorithm {
    Baseline,
    DedupHash,
    DedupBTree,
    NS,
}

impl CompressionAlgorithm {
    pub fn str(self) -> String {
        match self {
            Self::Baseline => "baseline",
            Self::DedupHash => "dedup_hash",
            Self::DedupBTree => "dedup_btree",
            Self::NS => "ns",
        }
        .to_string()
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "baseline" => Ok(CompressionAlgorithm::Baseline),
            "dedup_hash" => Ok(CompressionAlgorithm::DedupHash),
            "dedup_btree" => Ok(CompressionAlgorithm::DedupBTree),
            "ns" => Ok(CompressionAlgorithm::NS),
            _ => Err(String::from("allowed: baseline dedup_hash dedup_btree ns")),
        }
    }
}
