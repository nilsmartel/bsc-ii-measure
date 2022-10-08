use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "verify bintables",
    about = "Compare bintables to original table"
)]
pub struct Config {
    #[structopt()]
    pub table: String,

    #[structopt()]
    pub bintable: String,
}
