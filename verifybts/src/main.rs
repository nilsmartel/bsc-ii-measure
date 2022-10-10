mod util;
use util::*;

mod table_lake;
use structopt::StructOpt;
use table_lake::*;

mod cli;
mod db;

use crate::cli::Config;

use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let Config { bintable, table } = cli::Config::from_args();

    let bt = indices_from_bintable(&bintable, None);
    let t = indices(&table, None);

    let mut i = 0;
    loop {
        let t = t.recv().expect("table empty");
        let bt = bt
            .recv()
            .expect("bintable is empty, while table still yields");

        if bt != t {
            eprintln!("{i} differing {} {}", bt.0, t.0);
        }

        if i & 0xff == 0 {
            eprintln!("{i}");
        }
        i += 1;
    }
}
