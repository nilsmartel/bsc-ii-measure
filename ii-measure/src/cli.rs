use crate::kinds::CompressionAlgorithm;
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

    #[structopt(short, long)]
    pub label: Option<String>,

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

    /// Wether this run was performed in parrallel along with other instances
    #[structopt(short, long)]
    pub multi_proc: bool,
}
