// Example custom build script.
fn main() {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed=./cpp-lib/c-bridge.cpp");

    println!("cargo:rustc-link-lib=dylib=c-bridge");
    println!("cargo:rustc-link-search=./cpp-lib/build");
}
