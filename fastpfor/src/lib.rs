mod bindgen;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_and_decompress_raw_bindings() {
        let n = 1024 * 1024usize;

        fn random_from_seed(seed: usize) -> u32 {
            let result = seed.wrapping_mul(438248) % 732819 + 7;
            let result = result & u32::MAX as usize;

            result as u32
        }

        let data = (0..n).map(random_from_seed).collect::<Vec<u32>>();
        // align vector to 16 bytes
        let needed_offset = data.as_ptr().align_offset(16);
        let data = &data[needed_offset..];

        let id = std::ffi::CString::new("simdfastpfor256").unwrap();
        let codec = unsafe { bindgen::CODECFactory_getFromName(id.as_ptr()) };

        let mut compressed_data: Vec<u32> = vec![0; n * 3];

        // align vector to 16 bytes
        let needed_offset = compressed_data.as_ptr().align_offset(16);
        let compressed_data = &mut compressed_data[needed_offset..];

        // Compress and return how many bytes were written.
        let bytes_written = unsafe {
            bindgen::CODEC_encodeArray(
                codec,
                data.as_ptr(),
                data.len() as u64,
                compressed_data.as_mut_ptr(),
                compressed_data.len() as u64,
            )
        } as usize;
        let compressed_data = &compressed_data[..bytes_written];

        assert!(bytes_written != 0, "expect more than 0 bytes to be written");

        let mut decompressed_data: Vec<u32> = vec![0; n + 16];

        // align vector to 16 bytes
        let needed_offset = decompressed_data.as_ptr().align_offset(16);
        let decompressed_data = &mut decompressed_data[needed_offset..];

        let bytes_written = unsafe {
            bindgen::CODEC_decodeArray(
                codec,
                compressed_data.as_ptr(),
                compressed_data.len() as u64,
                decompressed_data.as_mut_ptr(),
                decompressed_data.len() as u64,
            )
        } as usize;
        let decompressed_data = &decompressed_data[..bytes_written];

        assert_eq!(
            data, decompressed_data,
            "expect data to be the same after compression"
        );
    }
}
