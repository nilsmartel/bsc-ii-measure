use anyhow::Result;
use fast_smaz::Smaz;
// use sqlx::{postgres::PgRow, FromRow, Row};
use std::io::Write;
use varint_compression::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub tokenized: String,
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u32,
}

#[derive(Debug, Copy, Clone)]
pub enum ReadError {
    InitialNumber,
    Needed(usize),
}

#[derive(Debug, Default)]
pub struct ParseAcc {
    pub last_tokenized: String,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Kind {
    Same,
    Compressed,
}

impl Kind {
    fn from(v: u8) -> Kind {
        match v {
            0 => Self::Same,
            1 => Self::Compressed,
            _ => unreachable!("invalid byte set"),
        }
    }

    fn byte(self) -> u8 {
        match self {
            Kind::Same => 0,
            Kind::Compressed => 1,
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use super::*;

    #[test]
    fn serde() {
        let mut input = vec![TableRow {
            tokenized: "".to_string(),
            tableid: 123,
            rowid: 2345678,
            colid: 321,
        }];

        for i in 0..1000 {
            let tokenized = format!("{i}");
            let row = TableRow {
                tokenized,
                tableid: i as u32,
                colid: i as u32,
                rowid: i as u32,
            };
            input.push(row.clone());
            input.push(row.clone());
            input.push(row.clone());
        }

        // serialize

        struct U8Reader(Vec<u8>);
        impl Write for U8Reader {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.extend(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut buffer = U8Reader(Vec::new());

        let mut acc = ParseAcc::default();
        for d in input.clone() {
            d.write_bin(&mut buffer, &mut acc).unwrap();
        }

        // deserialize
        let mut retrieved = Vec::new();

        let mut acc = ParseAcc::default();
        let mut data: &[u8] = &buffer.0;
        while !data.is_empty() {
            let (row, rest) = TableRow::from_bin(data, &mut acc).unwrap();
            data = rest;
            retrieved.push(row);
        }

        for i in 0..1001 {
            assert_eq!(retrieved[i], input[i], "testing element {i}");
        }
    }
}

impl TableRow {
    pub fn integers(&self) -> [u32; 3] {
        [self.tableid, self.colid, self.rowid]
    }

    pub fn from_bin<'b>(data: &'b [u8], acc: &mut ParseAcc) -> Result<(Self, &'b [u8]), ReadError> {
        let (need_length, rest) = match decompress(data) {
            Ok(d) => d,
            Err(_) => {
                return Err(ReadError::InitialNumber);
            }
        };

        let need_length = need_length as usize;

        if rest.len() < need_length {
            return Err(ReadError::Needed(need_length - rest.len()));
        }

        let v = TableRow::from_bin_raw(rest, acc);

        Ok((v, &rest[need_length..]))
    }

    pub fn from_bin_raw(data: &[u8], acc: &mut ParseAcc) -> Self {
        let kind = Kind::from(data[0]);
        let data = &data[1..];

        let (tokenized, data) = match kind {
            Kind::Same => (acc.last_tokenized.to_string(), data),
            Kind::Compressed => {
                let (len, data) = decompress(data).unwrap();
                let n = len as usize;
                let tokenized = &data[..n];
                let tokenized = tokenized.smaz_decompress().unwrap();
                let tokenized = unsafe { String::from_utf8_unchecked(tokenized) };

                acc.last_tokenized = tokenized.clone();

                (tokenized, &data[n..])
            }
        };

        let ([tableid, colid, rowid], _rest) = decompress_n(data).unwrap();

        let tableid = tableid as u32;
        let colid = colid as u32;
        let rowid = rowid as u32;

        Self {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }

    pub fn write_bin(&self, w: &mut impl Write, acc: &mut ParseAcc) -> Result<()> {
        let kind = if self.tokenized == acc.last_tokenized {
            Kind::Same
        } else {
            Kind::Compressed
        };

        let tokenized = if kind == Kind::Same {
            Vec::new()
        } else {
            acc.last_tokenized = self.tokenized.clone();
            self.tokenized.smaz_compress()
        };

        let len = compress(tokenized.len() as u64);
        let nums = compress_list(&[self.tableid as u64, self.colid as u64, self.rowid as u64]);

        let mut total_length = 1 + nums.len();
        if kind != Kind::Same {
            total_length += len.len() + tokenized.len();
        }
        let total_length = compress(total_length as u64);

        w.write_all(&total_length)?;
        w.write_all(&[kind.byte()])?;
        if kind != Kind::Same {
            w.write_all(&len)?;
            w.write_all(&tokenized)?;
        }
        w.write_all(&nums)?;
        Ok(())
    }
}

// impl<'r> FromRow<'r, PgRow> for TableRow {
//     fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
//         let tokenized: Option<String> = row.try_get(0)?;
//         let tokenized = tokenized.unwrap_or_default();

//         let tableid = get_number(row, 1) as u32;
//         let colid = get_number(row, 2) as u32;
//         let rowid = get_number(row, 3) as u32;

//         Ok(TableRow {
//             tokenized,
//             tableid,
//             colid,
//             rowid,
//         })
//     }
// }

// fn get_number(row: &PgRow, index: usize) -> i64 {
//     if let Ok(v) = row.try_get::<i64, usize>(index) {
//         return v;
//     }
//     if let Ok(v) = row.try_get::<i32, usize>(index) {
//         return v as i64;
//     }
//     if let Ok(v) = row.try_get::<i8, usize>(index) {
//         return v as i64;
//     }

//     row.try_get::<i16, usize>(index).unwrap() as i64
// }
