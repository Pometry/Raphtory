mod csv {
    use std::collections::VecDeque;
    use std::{fs, io};
    use std::path::{Path, PathBuf};

    use itertools::Itertools;
    use regex::Regex;

    use crate::GraphDB;

    #[derive(Debug)]
    pub struct CsvErr(io::Error);

    #[derive(Debug)]
    pub struct CsvLoader {
        path: PathBuf,
        regex_filter: Option<Regex>,
    }

    impl CsvLoader {
        pub fn new<P: Into<PathBuf>>(p: P) -> Self {
            Self {
                path: p.into(),
                regex_filter: None,
            }
        }

        pub fn with_filter(mut self, r: Regex) -> Self {
            self.regex_filter = Some(r);
            self
        }

        fn is_dir<P: AsRef<Path>>(p: &P) -> bool {
            fs::metadata(p).unwrap().is_dir()
        }

        fn accept_file<P: Into<PathBuf>>(&self, path: P, paths: &mut Vec<PathBuf>) {
            let p: PathBuf = path.into();
            // this is an actual file so push it into the paths vec if it matches the pattern
            if let Some(pattern) = &self.regex_filter {
                let is_match = &p
                    .to_str()
                    .filter(|file_name| {
                        let matches = pattern.is_match(file_name);
                        println!("{file_name} {matches}");
                        matches
                    })
                    .is_some();
                if *is_match {
                    paths.push(p);
                }
            } else {
                paths.push(p)
            }
        }

        fn files_vec(&self) -> Result<Vec<PathBuf>, CsvErr> {
            let mut paths = vec![];
            let mut queue = VecDeque::from([self.path.to_path_buf()]);

            while let Some(ref path) = queue.pop_back() {
                match fs::read_dir(path) {
                    Ok(entries) => {
                        for entry in entries {
                            if let Ok(f_path) = entry {
                                let p = f_path.path();
                                if Self::is_dir(&p) {
                                    queue.push_back(p.clone())
                                } else {
                                    self.accept_file(f_path.path(), &mut paths);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if !Self::is_dir(path) {
                            self.accept_file(path.to_path_buf(), &mut paths);
                        }
                        return Err(CsvErr(err))
                    }
                }
            }

            Ok(paths)
        }

        pub fn load_into(&self, g: &GraphDB) -> Result<(), CsvErr> {
            let mut paths = self.files_vec();

            println!("PATHS {paths:?}");
            todo!()
        }

        pub fn load(&self) -> Result<GraphDB, CsvErr> {
            let g = GraphDB::new(2);
            self.load_into(&g)?;
            Ok(g)
        }
    }
}

#[cfg(test)]
mod csv_loader_test {
    use regex::Regex;

    use super::csv::CsvLoader;

    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        let text = "/home/murariuf/Offline/bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "/home/murariuf/Offline/bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    #[test]
    fn list_all_files() {
        let graph = CsvLoader::new("/home/murariuf/Offline/bitcoin")
            .with_filter(Regex::new(r".+address").unwrap())
            .load();
    }
}
