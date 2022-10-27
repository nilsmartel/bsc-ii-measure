#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("fastpfor/headers/fastpfor.h");

        type BlobstoreClient;

        fn new_blobstore_client() -> UniquePtr<BlobstoreClient>;
    }
}
