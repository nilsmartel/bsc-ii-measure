use structopt::StructOpt;
use crate::kinds::CompressionAlgorithm;

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