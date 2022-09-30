use std::str::FromStr;

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "inverted-index-measure",
    about = "Measure memory usage of inverted index implementations."
)]
pub struct Config {
    /// file to write data about consumed
    /// rows vs. memory consumption into
    #[structopt(short, long)]
    pub output: Option<String>,

    #[structopt(short, long)]
    pub limit: usize,

    #[structopt(short, long)]
    pub compression: CompressionAlgorithm,

    #[structopt(default_value = "gittables_main_tokenized")]
    pub table: String,
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
