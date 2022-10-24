cd ii-measure
cargo build
cargo install --path . --force

cd ../table-to-file-2
cargo build
cargo install --path . --force

cd ../subportion-bintable
cargo install --path . --force

cd ../sort-bintable
cargo install --path . --force
