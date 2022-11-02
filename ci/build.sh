bash -c '
cd fastpfor
bash ./ci/**'

for f in *
    bash -c "cd $f; cargo install --path . --force" &
