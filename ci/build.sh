bash -c '
cd fastpfor
bash ./ci/**'

for f in *
do
    bash -c "cd $f; cargo install --path . --force" &
done
