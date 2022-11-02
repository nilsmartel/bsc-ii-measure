mod bindgen;

#[cfg(test)]
mod tests {
    use super::*;

    fn random_data(len: usize) -> Vec<u32> {
        fn random_from_seed(seed: usize) -> u32 {
            let result = seed.wrapping_mul(438248) % 732819 + 7;
            let result = result & u32::MAX as usize;

            result as u32
        }

        (0..len).map(random_from_seed).collect()
    }

    #[test]
    fn nothing() {}

    #[test]
    fn unaligned() {
        let codec = Codec::simdfastpfor256();
        let n = 1024;
        let data = random_data(n);

        let mut compressed_data = vec![0; n + 1];
        let mut compressed_data = &mut compressed_data[1..];

        let size = codec
            .compress(&data, &mut compressed_data)
            .expect("to compress data");
        compressed_data = &mut compressed_data[..size];

        let mut result = vec![0; n];

        let bytes_written = codec
            .decompress(&compressed_data, &mut result)
            .expect("enough space to transfer bytes");

        assert_eq!(n, bytes_written, "expect size of in and out to be the same");

        assert_eq!(data, result);
    }

    #[test]
    fn rust_interface() {
        let codec = Codec::simdfastpfor256();
        let n = 1024;
        let data = random_data(n);

        let mut compressed_data = vec![0; n];

        let size = codec
            .compress(&data, &mut compressed_data)
            .expect("to compress data");
        compressed_data.resize(size, 0);

        let mut result = vec![0; n];

        let bytes_written = codec
            .decompress(&compressed_data, &mut result)
            .expect("enough space to transfer bytes");

        assert_eq!(n, bytes_written, "expect size of in and out to be the same");

        assert_eq!(data, result);
    }

    #[test]
    fn compress_and_decompress_raw_bindings() {
        let n = 1024 * 1024usize;

        let data = random_data(n);

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

pub struct Codec(bindgen::IntegerCODECPtr);

// impl Drop for Codec {
//     fn drop(&mut self) {
//         unsafe { bindgen::INTEGERCODEC_destroy(self.0) };
//     }
// }

macro_rules! algo {
    ($i:ident, $s:expr) => {
        pub fn $i() -> Self {
            Self::get_from_name($s)
        }
    };
    ($i:ident) => {
        pub fn $i() -> Self {
            Self::get_from_name(stringify!($i))
        }
    };
}

impl Codec {
    pub fn free(self) {
        unsafe { bindgen::INTEGERCODEC_destroy(self.0) };
    }

    algo!(bp32, "BP32");
    algo!(copy);
    algo!(fastbinarypacking16);
    algo!(fastbinarypacking32);
    algo!(fastbinarypacking8);
    algo!(fastpfor128);
    algo!(fastpfor256);
    algo!(maskedvbyte);
    algo!(newpfor);
    algo!(optpfor);
    algo!(pfor);
    algo!(pfor2008);
    algo!(simdbinarypacking);
    algo!(simdfastpfor128);
    algo!(simdfastpfor256);
    algo!(simdgroupsimple);
    algo!(simdgroupsimple_ringbuf);
    algo!(simdnewpfor);
    algo!(simdsimplepfor);
    algo!(simple16);
    algo!(simple8b);
    algo!(simple8b_rle);
    algo!(simple9);
    algo!(simple9_rle);
    algo!(simplepfor);
    algo!(streamvbyte);
    algo!(varint);
    algo!(varintg8iu);
    algo!(varintgb);
    algo!(vbyte);
    algo!(vsenconding);

    pub fn get_from_name(name: &str) -> Codec {
        let name = std::ffi::CString::new(name).expect("valid c string");

        let codec = unsafe { bindgen::CODECFactory_getFromName(name.as_ptr()) };

        Codec(codec)
    }

    /// Expects input data to be 16 bytes aligned
    ///
    /// returns how many bytes were written to destination buffer.
    pub fn compress(
        &self,
        data: &[u32],
        destination: &mut [u32],
    ) -> Result<usize, BufferSizeError> {
        // assert_eq!(
        //     data.as_ptr().align_offset(16),
        //     0,
        //     "expected 16 bytes aligned input data"
        // );
        // assert_eq!(
        //     destination.as_ptr().align_offset(16),
        //     0,
        //     "expected 16 bytes aligned destination"
        // );

        // Compress and return how many bytes were written.
        let bytes_written = unsafe {
            bindgen::CODEC_encodeArray(
                self.0,
                data.as_ptr(),
                data.len() as u64,
                destination.as_mut_ptr(),
                destination.len() as u64,
            )
        } as usize;

        if bytes_written > destination.len() {
            return Err(BufferSizeError::TooSmall);
        }

        Ok(bytes_written)
    }

    pub fn decompress(
        &self,
        compressed_data: &[u32],
        destination: &mut [u32],
    ) -> Result<usize, BufferSizeError> {
        // assert_eq!(
        //     compressed_data.as_ptr().align_offset(16),
        //     0,
        //     "expected 16 bytes aligned compressed input data"
        // );
        // assert_eq!(
        //     destination.as_ptr().align_offset(16),
        //     0,
        //     "expected 16 bytes aligned destination"
        // );

        // Compress and return how many bytes were written.
        let bytes_written = unsafe {
            bindgen::CODEC_decodeArray(
                self.0,
                compressed_data.as_ptr(),
                compressed_data.len() as u64,
                destination.as_mut_ptr(),
                destination.len() as u64,
            )
        } as usize;

        if bytes_written > destination.len() {
            return Err(BufferSizeError::TooSmall);
        }

        Ok(bytes_written)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BufferSizeError {
    TooSmall,
}
