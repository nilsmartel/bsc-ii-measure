use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "inverted-index-measure",
    about = "Measure memory usage of inverted index implementations."
)]
pub struct Config {
    /// file to write data about consumed
    /// rows vs. memory consumption into
    #[structopt()]
    pub output: String,

    #[structopt(default_value = "gittables_main_tokenized")]
    pub table: String,

    #[structopt()]
    pub limit: Option<usize>,
}
