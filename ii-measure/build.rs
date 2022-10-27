fn main() {
    println!("cargo:rustc-link-lib=dylib=c-bridge");
    println!("cargo:rustc-link-search=../fastpfor/cpp-lib/build");
}
