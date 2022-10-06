use std::time::Duration;

pub type MemData = (usize, usize, Duration, Duration);

/// Handles logging and formatting of information to file
pub struct Logger {
    memdata: Option<MemData>,
    retrieval: Option<Duration>,
    algorithm: String,
    table: String,
    header: bool,
}

pub fn print_header() {
    println!("cells;bytes;build_duration_nanosec;build_duration_total_nanosec;retr_duration_avg_nanosec;algorithm;table");
}

impl Logger {
    /// Starts a new logging server as a separate thread and opens the desired files.
    pub fn new(algorithm: String, table: String, header: bool) -> Self {
        Logger {
            memdata: None,
            retrieval: None,
            algorithm,
            table,
            header,
        }
    }

    pub fn print(&self) {
        if self.header {
            print_header();
        }
        let algorithm = &self.algorithm;
        let table = &self.table;

        let (cells, bytes, duration, total_duration) = self.memdata.expect("memdata");
        let duration = duration.as_nanos();
        let total_duration = total_duration.as_nanos();

        let retr_duration = self.retrieval.expect("retrieval information").as_nanos();

        println!("{cells};{bytes};{duration};{total_duration};{retr_duration};{algorithm};{table}");
    }

    pub fn memory_info(&mut self, data: MemData) {
        self.memdata = Some(data);
    }

    pub fn retrieval_info(&mut self, duration: Duration) {
        self.retrieval = Some(duration);
    }
}
