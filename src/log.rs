use anyhow::{Result, Ok};
use std::fs::File;
use std::io::Write;

/// Handles logging and formatting of information to file
pub struct Logger {
    /// File containing the csv formatted information about how the memory rises
    /// with respect to the amount of cell values inserted into the table
    mem_stats: File
}

impl Logger {
    pub fn new(output_file: impl Into<String>) -> Result<Self> {
        let mem_stats = output_file.into() + "-mem.csv";
        let mut mem_stats = File::create(mem_stats)?;

        writeln!(&mut mem_stats, "cells;bytes")?;

        Ok(Logger {mem_stats})
    }

    pub fn memory(&mut self, cells: usize, bytes: usize) -> Result<()> {
        writeln!(&mut self.mem_stats, "{cells};{bytes}")?;
        Ok(())
    }
}

