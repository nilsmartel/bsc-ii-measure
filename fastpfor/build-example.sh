# ./ci/build-shared-library.sh
# -g generates debug symbols
gcc -g example.c -l c-bridge -L ./cpp-lib/build
export LD_LIBRARY_PATH=./cpp-lib/build/:$LD_LIBRARY_PATH

echo running a.out
./a.out

echo done

# echo removing binary
# rm ./a.out
