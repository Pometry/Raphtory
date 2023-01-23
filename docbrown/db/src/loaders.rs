pub mod csv {
    use flate2; // 1.0
    use flate2::read::GzDecoder;
    use serde::de::DeserializeOwned;
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    use rayon::prelude::*;
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
                        pattern.is_match(file_name)
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
                        return Err(CsvErr(err));
                    }
                }
            }

            Ok(paths)
        }

        pub fn load_into_graph<F, REC>(&self, g: &GraphDB, loader: F) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &GraphDB) -> () + Send + Sync,
        {
            let paths = self.files_vec()?;

            println!("LOADING {paths:?}");

            paths
                .par_iter()
                .try_for_each(move |path| self.load_file_into_graph(path, g, &loader))?;
            Ok(())
        }

        fn load_file_into_graph<F, REC, P: Into<PathBuf> + Debug>(
            &self,
            path: P,
            g: &GraphDB,
            loader: &F,
        ) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &GraphDB) -> (),
        {
            let file_path: PathBuf = path.into();

            let mut csv_reader = self.csv_reader(file_path);
            let mut records_iter = csv_reader.deserialize::<REC>();

            while let Some(rec) = records_iter.next() {
                let record = rec.map_err(|err| CsvErr(err.into()))?;
                loader(record, g)
            }

            Ok(())
        }

        fn csv_reader(&self, file_path: PathBuf) -> csv::Reader<Box<dyn io::Read>> {
            let is_gziped = file_path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".gz"))
                .is_some();

            let f = File::open(&file_path).expect(&format!("Can't open file {file_path:?}"));
            if is_gziped {
                csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(Box::new(BufReader::new(GzDecoder::new(f))))
            } else {
                csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(Box::new(f))
            }
        }

        pub fn load(&self) -> Result<GraphDB, CsvErr> {
            let g = GraphDB::new(2);
            // self.load_into(&g)?;
            Ok(g)
        }
    }
}

#[cfg(test)]
mod csv_loader_test {
    use chrono::{DateTime, Utc};
    use docbrown_core::Prop;
    use regex::Regex;
    use serde::Deserialize;

    use crate::GraphDB;

    use super::csv::CsvLoader;
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    // #[derive(Deserialize, std::fmt::Debug)]
    // pub struct Address {
    //     addr: String,
    // }

    #[derive(Deserialize, std::fmt::Debug)]
    pub struct Sent {
        addr: String,
        txn: String,
        amount_btc: u64,
        amount_usd: f64,
        #[serde(with = "custom_date_format")]
        time: DateTime<Utc>,
    }

    #[derive(Deserialize, std::fmt::Debug)]
    pub struct Received {
        txn: String,
        addr: String,
        amount_btc: u64,
        amount_usd: f64,
        #[serde(with = "custom_date_format")]
        time: DateTime<Utc>,
    }

    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        let text = "/home/murariuf/Offline/bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "/home/murariuf/Offline/bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    #[test]
    fn regex_match_2() {
        let r = Regex::new(r".+(sent|received)").unwrap();
        let text = "/home/murariuf/Offline/bitcoin/sent_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "/home/murariuf/Offline/bitcoin/received_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "/home/murariuf/Offline/bitcoin/address_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    // #[test]
    // fn list_all_files() {
    //     let g = GraphDB::new(2);
    //     let graph = CsvLoader::new("/home/murariuf/Offline/bitcoin")
    //         .with_filter(Regex::new(r".+sentx_.+1").unwrap())
    //         .load_into_graph(&g, |sent: Sent, g: &GraphDB| {
    //             let src = calculate_hash(&sent.addr);
    //             let dst = calculate_hash(&sent.txn);
    //             let t = sent.time.timestamp();

    //             g.add_edge(
    //                 src,
    //                 dst,
    //                 t.try_into().unwrap(),
    //                 &vec![("amount".to_string(), Prop::U64(sent.amount_btc))],
    //             )
    //         })
    //         .expect("");
    // }

    mod custom_date_format {
        use chrono::{DateTime, TimeZone, Utc};
        use serde::{self, Deserialize, Deserializer, Serializer};

        const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

        // The signature of a serialize_with function must follow the pattern:
        //
        //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
        //    where
        //        S: Serializer
        //
        // although it may also be generic over the input types T.
        pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let s = format!("{}", date.format(FORMAT));
            serializer.serialize_str(&s)
        }

        // The signature of a deserialize_with function must follow the pattern:
        //
        //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
        //    where
        //        D: Deserializer<'de>
        //
        // although it may also be generic over the output types T.
        pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Utc.datetime_from_str(&s, FORMAT)
                .map_err(serde::de::Error::custom)
        }
    }
}
