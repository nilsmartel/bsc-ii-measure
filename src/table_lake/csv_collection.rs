use crate::{Entry, TableIndex, TableLakeReader};
use std::sync::mpsc::{sync_channel, Sender};
use std::thread::spawn;

pub struct CSVCollection {
    pub documents: Vec<String>,
}

impl TableLakeReader for CSVCollection {
    fn read(&mut self, ch: Sender<Entry>) {
        use threadpool::ThreadPool;
        let (jobsender, jobreceiver) = sync_channel(12);

        spawn(|| {
            let pool = ThreadPool::new(12);

            for job in jobreceiver {
                pool.execute(job);
            }
        });

        for (table_id, filename) in self.documents.iter().enumerate() {
            use csv::ReaderBuilder;
            let filename = filename.clone();

            let ch = ch.clone();
            let job = move || {
                // TODO this is just for some light logging
                eprintln!("{}", basename(&filename));

                let mut table = ReaderBuilder::new()
                    .from_path(filename)
                    .expect("to read csv file");

                for (row_id, record) in table.records().enumerate() {
                    let record = if let Ok(r) = record { r } else { continue };

                    for (column_id, value) in record.iter().enumerate() {
                        let index = TableIndex {
                            table_id,
                            row_id,
                            column_id,
                        };
                        let value = value.to_string();

                        ch.send((value, index))
                            .expect("to send table entry via channel");
                    }
                }
            };

            // Fuck man, I think this is non blocking
            // TODO MAKE THIS BLOCKING
            // pool.execute(job);
            jobsender.send(job).unwrap();
        }

        drop(jobsender);
    }
}

fn basename(s: &str) -> &str {
    let index = s.rfind('/').unwrap_or(0);
    &s[index..]
}
