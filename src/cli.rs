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
    #[structopt(short,long)]
    pub output: String,

    #[structopt(short,long)]
    pub limit: usize,

    #[structopt(short,long)]
    pub compression: CompressionAlgorithm,

    #[structopt(default_value = "gittables_main_tokenized")]
    pub table: String,
}

#[derive(Copy, Clone)]
pub enum CompressionAlgorithm {
    Baseline,
    Duplicates,
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "baseline" | "base" => Ok(CompressionAlgorithm::Baseline),
            "duplicate" | "duplicates" | "dup" => Ok(CompressionAlgorithm::Duplicates),
            _ => Err(String::from("allowed: base baseline duplicate duplicates dup")),
        }
    }
}