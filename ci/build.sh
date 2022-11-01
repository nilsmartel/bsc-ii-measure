
# cd table-to-file-2
# cargo install --path . --force &

cd ./subportion-bintable
cargo install --path . --force &

cd ../sort-bintable
cargo install --path . --force &

cd ../fastpfor
bash ./ci/**

cd ../ii-measure
cargo install --path . --force &
