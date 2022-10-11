use anyhow::Result;
use fast_smaz::Smaz;
use sqlx::{FromRow, postgres::PgRow, Row};
use std::io::Write;
use varint_compression::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub tokenized: Vec<u8>,
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u64,
}

#[derive(Debug, Copy, Clone)]
pub enum ReadError {
    InitialNumber,
    Needed(usize),
}

pub struct ParseAcc<'a> {
    pub last_tokenized: &'a [u8]
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

impl TableRow {
    pub fn from_bin<'a, 'b>(data: &'b [u8], acc: &mut ParseAcc<'a>) -> Result<(Self, &'b[u8]), ReadError> {
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

        let v = TableRow::from_bin_raw(rest, &mut acc);

        Ok((v, &rest[need_length..]))
    }

    pub fn from_bin_raw<'a, 'b>(data: &'b [u8], acc: &mut ParseAcc<'a>) -> Self {
        let kind = Kind::from(data[0]);
        let data = &data[1..];

        let (tokenized, data) = match kind {
            Kind::Same => (acc.last_tokenized.to_vec(), data),
            Kind::Compressed => {
                let (len, data) = decompress(data).unwrap();
                let n = len as usize;
                let tokenized = &data[..n];
                let tokenized = tokenized.smaz_decompress().unwrap();
                
                acc.last_tokenized = &tokenized;

                (tokenized, &data[n..])
            }
        };

        let ([tableid, colid, rowid], _rest) = decompress_n(data).unwrap();

        let tableid = tableid as u32;
        let colid = colid as u32;
        let rowid = rowid as u64;

        Self {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }

    pub fn write_bin(&self, w: &mut impl Write) -> Result<()> {
        let tokenized = self.tokenized.smaz_compress();
        let len = compress(tokenized.len() as u64);
        let nums = compress_list(&[self.tableid as u64, self.colid as u64, self.rowid as u64]);

        let total_length = compress((len.len() + tokenized.len() + nums.len()) as u64);

        w.write_all(&total_length)?;
        w.write_all(&len)?;
        w.write_all(&tokenized)?;
        w.write_all(&nums)?;

        Ok(())
    }
}

impl<'r> FromRow<'r, PgRow> for TableRow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let tokenized: Option<Vec<u8>> = row.try_get(0)?;
        let tokenized = tokenized.unwrap_or_default();

        let tableid = get_number(row, 1) as u32;
        let colid = get_number(row, 2) as u32;
        let rowid = get_number(row, 3) as u64;

        Ok(TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        })
    }
}

fn get_number(row: &PgRow, index: usize) -> i64 {
    if let Ok(v) = row.try_get::<i64, usize>(index) {
        return v;
    }
    if let Ok(v) = row.try_get::<i32, usize>(index) {
        return v as i64;
    }
    if let Ok(v) = row.try_get::<i8, usize>(index) {
        return v as i64;
    }

    row.try_get::<i16, usize>(index).unwrap() as i64
}
