pub(crate) fn get_factor(path: &str) -> f32 {
    if let Some((_, f)) = path.rsplit_once('-') {
        f.parse().expect("valid float at the end of bintable name")
    } else {
        1.0
    }
}

/// Searches for the best version of a given corpus to use for streaming
/// and adjusts the factor.
/// For example: doubling the factor, if a bintable is found, that contains only 50% of the entire
/// collection
pub(crate) fn find_best_input(
    path: &str,
    corpus: &str,
    factor: f32,
) -> std::io::Result<(String, f32)> {
    use std::fs::read_dir;

    if path.ends_with('/') {
        panic!("path musn't end with /");
    }

    let mut bestfactor = 1.0;
    let mut bestfile = format!("{}/{}", path, corpus);

    for entry in read_dir(path)? {
        if entry.is_err() {
            continue;
        }
        let entry = entry?;

        let filename = entry.file_name().into_string().unwrap();
        if !filename.starts_with(&corpus) {
            continue;
        }

        let f = get_factor(&filename);

        if f < factor {
            continue;
        }

        if f < bestfactor {
            bestfactor = f;
            bestfile = format!("{}/{}", path, filename);
        }
    }

    Ok((bestfile, factor / bestfactor))
}
