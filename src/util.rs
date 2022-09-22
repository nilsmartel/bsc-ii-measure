use crate::CSVCollection;

pub fn collect_tables_from_stdin() -> CSVCollection {
    let stdin_content = {
        let mut s = String::new();
        use std::io::Read;
        std::io::stdin()
            .read_to_string(&mut s)
            .expect("to read names of csv files from stdin");
        s
    };

    let documents = stdin_content
        .split('\n')
        .map(std::string::String::from)
        .collect();

    CSVCollection { documents }
}
