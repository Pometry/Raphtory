//! Module containing functions for loading CSV files into a graph.
//!
//! # Example
//! ```no_run
//! use std::path::{Path, PathBuf};
//! use regex::Regex;
//! use raphtory::core::utils::hashing::calculate_hash;
//! use raphtory::graph_loader::source::csv_loader::CsvLoader;
//! use raphtory::graph_loader::example::lotr_graph::Lotr;
//! use raphtory::prelude::*;
//!
//!  let g = Graph::new();
//!  let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
//!         .iter()
//!         .collect();
//!
//!  println!("path = {}", csv_path.as_path().to_str().unwrap());
//!  let csv_loader = CsvLoader::new(Path::new(&csv_path));
//!  let has_header = true;
//!  let r = Regex::new(r".+(lotr.csv)").unwrap();
//!  let delimiter = ",";
//!
//!  csv_loader
//!      .set_header(has_header)
//!      .set_delimiter(delimiter)
//!      .with_filter(r)
//!      .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
//!          let src_id = calculate_hash(&lotr.src_id);
//!          let dst_id = calculate_hash(&lotr.dst_id);
//!          let time = lotr.time;
//!
//!          g.add_vertex(
//!              time,
//!              src_id,
//!              [("name".to_string(), Prop::Str("Character".to_string()))],
//!          )
//!          .map_err(|err| println!("{:?}", err))
//!          .ok();
//!          g.add_vertex(
//!              time,
//!              dst_id,
//!              [("name".to_string(), Prop::Str("Character".to_string()))],
//!          )
//!          .map_err(|err| println!("{:?}", err))
//!          .ok();
//!          g.add_edge(
//!              time,
//!              src_id,
//!              dst_id,
//!              [(
//!                  "name".to_string(),
//!                  Prop::Str("Character Co-occurrence".to_string()),
//!              )],
//!              None,
//!          ).expect("Failed to add edge");
//!      })
//!      .expect("Csv did not parse.");
//! ```
//!

/// Module for loading CSV files into a graph.
use bzip2::read::BzDecoder;
use csv::StringRecord;
use flate2; // 1.0
use flate2::read::GzDecoder;
use rayon::prelude::*;
use regex::Regex;
use serde::de::DeserializeOwned;
use std::{
    collections::VecDeque,
    error::Error,
    fmt::{Debug, Display, Formatter},
    fs,
    fs::File,
    io,
    io::BufReader,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum CsvErr {
    /// An IO error that occurred during file read.
    IoError(io::Error),
    /// A CSV parsing error that occurred while parsing the CSV data.
    CsvError(csv::Error),
}

impl From<io::Error> for CsvErr {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<csv::Error> for CsvErr {
    fn from(value: csv::Error) -> Self {
        Self::CsvError(value)
    }
}

impl Display for CsvErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.source() {
            Some(error) => write!(f, "CSV loader failed with error: {}", error),
            None => write!(f, "CSV loader failed with unknown error"),
        }
    }
}

impl Error for CsvErr {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CsvErr::IoError(error) => Some(error),
            CsvErr::CsvError(error) => Some(error),
        }
    }
}

/// A struct that defines the CSV loader with configurable options.
#[derive(Debug)]
pub struct CsvLoader {
    /// Path of the CSV file or directory containing CSV files.
    path: PathBuf,
    /// Optional regex filter to select specific CSV files by name.
    regex_filter: Option<Regex>,
    /// Specifies whether the CSV file has a header.
    header: bool,
    /// The delimiter character used in the CSV file.
    delimiter: u8,
}

impl CsvLoader {
    /// Creates a new `CsvLoader` instance with the specified file path.
    ///
    /// # Arguments
    ///
    /// * `p` - A path of the CSV file or directory containing CSV files.
    ///
    /// # Example
    ///
    /// ```no_run
    ///
    /// use raphtory::graph_loader::source::csv_loader::CsvLoader;
    /// let loader = CsvLoader::new("/path/to/csv_file.csv");
    /// ```
    pub fn new<P: Into<PathBuf>>(p: P) -> Self {
        Self {
            path: p.into(),
            regex_filter: None,
            header: false,
            delimiter: b',',
        }
    }

    /// Sets whether the CSV file has a header.
    ///
    /// # Arguments
    ///
    /// * `h` - A boolean value indicating whether the CSV file has a header.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use raphtory::graph_loader::source::csv_loader::CsvLoader;
    /// let loader = CsvLoader::new("/path/to/csv_file.csv").set_header(true);
    /// ```
    pub fn set_header(mut self, h: bool) -> Self {
        self.header = h;
        self
    }

    /// Sets the delimiter character used in the CSV file.
    ///
    /// # Arguments
    ///
    /// * `d` - A string containing the delimiter character.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use raphtory::graph_loader::source::csv_loader::CsvLoader;
    /// let loader = CsvLoader::new("/path/to/csv_file.csv").set_delimiter("|");
    /// ```
    pub fn set_delimiter(mut self, d: &str) -> Self {
        self.delimiter = d.as_bytes()[0];
        self
    }

    /// Sets the regex filter to select specific CSV files by name.
    ///
    /// # Arguments
    ///
    /// * `r` - A regex pattern to filter CSV files by name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use regex::Regex;
    /// use raphtory::graph_loader::source::csv_loader::CsvLoader;
    ///
    /// let loader = CsvLoader::new("/path/to/csv_files")
    ///    .with_filter(Regex::new(r"file_name_pattern").unwrap());
    /// ```
    pub fn with_filter(mut self, r: Regex) -> Self {
        self.regex_filter = Some(r);
        self
    }

    /// Check if the provided path is a directory or not.
    ///
    /// # Arguments
    ///
    /// * `p` - A reference to the path to be checked.
    ///
    /// # Returns
    ///
    /// A Result containing a boolean value indicating whether the path is a directory or not.
    ///
    /// # Errors
    ///
    /// An error of type CsvErr is returned if an I/O error occurs while checking the path.
    ///
    fn is_dir<P: AsRef<Path>>(p: &P) -> Result<bool, CsvErr> {
        Ok(fs::metadata(p)?.is_dir())
    }

    /// Check if a file matches the specified regex pattern and add it to the provided vector of paths.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file to be checked.
    /// * `paths` - A mutable reference to the vector of paths where the file should be added.
    ///
    /// # Returns
    ///
    /// Nothing is returned, the function only modifies the provided vector of paths.
    ///
    fn accept_file<P: Into<PathBuf>>(&self, path: P, paths: &mut Vec<PathBuf>) {
        let p: PathBuf = path.into();
        // this is an actual file so push it into the paths vec if it matches the pattern
        if let Some(pattern) = &self.regex_filter {
            let is_match = &p
                .to_str()
                .filter(|file_name| pattern.is_match(file_name))
                .is_some();
            if *is_match {
                paths.push(p);
            }
        } else {
            paths.push(p)
        }
    }

    /// Traverse the directory recursively and return a vector of paths to all files in the directory.
    ///
    /// # Arguments
    ///
    /// * No arguments are required.
    ///
    /// # Returns
    ///
    /// A Result containing a vector of PathBuf objects representing the paths to all files in the directory.
    ///
    /// # Errors
    ///
    /// An error of type CsvErr is returned if an I/O error occurs while reading the directory.
    ///
    fn files_vec(&self) -> Result<Vec<PathBuf>, CsvErr> {
        let mut paths = vec![];
        let mut queue = VecDeque::from([self.path.to_path_buf()]);

        while let Some(ref path) = queue.pop_back() {
            match fs::read_dir(path) {
                Ok(entries) => {
                    for entry in entries {
                        if let Ok(f_path) = entry {
                            let p = f_path.path();
                            if Self::is_dir(&p)? {
                                queue.push_back(p.clone())
                            } else {
                                self.accept_file(f_path.path(), &mut paths);
                            }
                        }
                    }
                }
                Err(err) => {
                    if !Self::is_dir(path)? {
                        self.accept_file(path.to_path_buf(), &mut paths);
                    } else {
                        return Err(err.into());
                    }
                }
            }
        }

        Ok(paths)
    }

    /// Load data from all CSV files in the directory into a graph.
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph object where the data should be loaded.
    /// * `loader` - A closure that takes a deserialized record and the graph object as arguments and adds the record to the graph.
    ///
    /// # Returns
    ///
    /// A Result containing an empty Ok value if the data is loaded successfully.
    ///
    /// # Errors
    ///
    /// An error of type CsvErr is returned if an I/O error occurs while reading the files or parsing the CSV data.
    ///
    pub fn load_into_graph<F, REC, G>(&self, g: &G, loader: F) -> Result<(), CsvErr>
    where
        REC: DeserializeOwned + Debug,
        F: Fn(REC, &G) + Send + Sync,
        G: Sync,
    {
        //FIXME: loader function should return a result for reporting parsing errors
        let paths = self.files_vec()?;
        paths
            .par_iter()
            .try_for_each(move |path| self.load_file_into_graph(path, g, &loader))?;
        Ok(())
    }

    /// Load data from all CSV files in the directory into a graph.
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph object where the data should be loaded.
    /// * `loader` - A closure that takes a deserialized record and the graph object as arguments and adds the record to the graph.
    ///
    /// # Returns
    ///
    /// A Result containing an empty Ok value if the data is loaded successfully.
    ///
    /// # Errors
    ///
    /// An error of type CsvErr is returned if an I/O error occurs while reading the files or parsing the CSV data.
    ///
    pub fn load_rec_into_graph<F, G>(&self, g: &G, loader: F) -> Result<(), CsvErr>
    where
        F: Fn(StringRecord, &G) + Send + Sync,
        G: Sync,
    {
        //FIXME: loader function should return a result for reporting parsing errors
        let paths = self.files_vec()?;
        paths
            .par_iter()
            .try_for_each(move |path| self.load_file_into_graph_record(path, g, &loader))?;
        Ok(())
    }

    /// Loads a CSV file into a graph using the specified loader function.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the CSV file to load.
    /// * `g` - A reference to the graph to load the data into.
    /// * `loader` - The function to use for loading the CSV records into the graph.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or a `CsvErr` if there was an error.
    ///
    fn load_file_into_graph<F, REC, P: Into<PathBuf> + Debug, G>(
        &self,
        path: P,
        g: &G,
        loader: &F,
    ) -> Result<(), CsvErr>
    where
        REC: DeserializeOwned + Debug,
        F: Fn(REC, &G),
    {
        let file_path: PathBuf = path.into();

        let mut csv_reader = self.csv_reader(file_path)?;
        let records_iter = csv_reader.deserialize::<REC>();

        //TODO this needs better error handling for files without perfect data
        for rec in records_iter {
            let record = rec.expect("Failed to deserialize");
            loader(record, g)
        }

        Ok(())
    }

    fn load_file_into_graph_record<F, P: Into<PathBuf> + Debug, G>(
        &self,
        path: P,
        g: &G,
        loader: &F,
    ) -> Result<(), CsvErr>
    where
        F: Fn(StringRecord, &G),
    {
        let file_path: PathBuf = path.into();

        let mut csv_reader = self.csv_reader(file_path)?;
        for rec in csv_reader.records() {
            let record = rec?;
            loader(record, g)
        }

        Ok(())
    }

    /// Returns a `csv::Reader` for the specified file path, automatically detecting and handling gzip and bzip compression.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the CSV file to read.
    ///
    /// # Returns
    ///
    /// Returns a `csv::Reader` that can be used to read the CSV file, or a `CsvErr` if there was an error.
    ///
    fn csv_reader(&self, file_path: PathBuf) -> Result<csv::Reader<Box<dyn io::Read>>, CsvErr> {
        let is_gziped = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| name.ends_with(".gz"))
            .is_some();

        let is_bziped = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| name.ends_with(".bz2"))
            .is_some();

        let f = File::open(&file_path)?;
        if is_gziped {
            Ok(csv::ReaderBuilder::new()
                .has_headers(self.header)
                .delimiter(self.delimiter)
                .from_reader(Box::new(BufReader::new(GzDecoder::new(f)))))
        } else if is_bziped {
            Ok(csv::ReaderBuilder::new()
                .has_headers(self.header)
                .delimiter(self.delimiter)
                .from_reader(Box::new(BufReader::new(BzDecoder::new(f)))))
        } else {
            Ok(csv::ReaderBuilder::new()
                .has_headers(self.header)
                .delimiter(self.delimiter)
                .from_reader(Box::new(f)))
        }
    }
}

#[cfg(test)]
mod csv_loader_test {
    use crate::{
        core::utils::hashing::calculate_hash, graph_loader::source::csv_loader::CsvLoader,
        prelude::*,
    };
    use csv::StringRecord;
    use regex::Regex;
    use serde::Deserialize;
    use std::path::{Path, PathBuf};

    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(text));
    }

    #[test]
    fn regex_match_2() {
        let r = Regex::new(r".+(sent|received)").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/sent_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(!r.is_match(text));
    }

    #[derive(Deserialize, std::fmt::Debug)]
    pub struct Lotr {
        src_id: String,
        dst_id: String,
        time: i64,
    }

    fn lotr_test(g: Graph, csv_loader: CsvLoader, has_header: bool, delimiter: &str, r: Regex) {
        csv_loader
            .set_header(has_header)
            .set_delimiter(delimiter)
            .with_filter(r)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = calculate_hash(&lotr.src_id);
                let dst_id = calculate_hash(&lotr.dst_id);
                let time = lotr.time;

                g.add_vertex(
                    time,
                    src_id,
                    [("name".to_string(), Prop::Str("Character".to_string()))],
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_vertex(
                    time,
                    dst_id,
                    [("name".to_string(), Prop::Str("Character".to_string()))],
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    [(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                    None,
                )
                .unwrap();
            })
            .expect("Csv did not parse.");
    }

    fn lotr_test_rec(g: Graph, csv_loader: CsvLoader, has_header: bool, delimiter: &str, r: Regex) {
        csv_loader
            .set_header(has_header)
            .set_delimiter(delimiter)
            .with_filter(r)
            .load_rec_into_graph(&g, |lotr: StringRecord, g: &Graph| {
                let src_id = lotr.get(0).map(|s| calculate_hash(&(s.to_owned()))).unwrap();
                let dst_id = lotr.get(1).map(|s| calculate_hash(&(s.to_owned()))).unwrap();
                let time = lotr.get(2).map(|s| s.parse::<i64>().unwrap()).unwrap();

                g.add_vertex(
                    time,
                    src_id,
                    [("name".to_string(), Prop::Str("Character".to_string()))],
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_vertex(
                    time,
                    dst_id,
                    [("name".to_string(), Prop::Str("Character".to_string()))],
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    [(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                    None,
                )
                .unwrap();
            })
            .expect("Csv did not parse.");
    }

    #[test]
    fn test_headers_flag_and_delimiter() {
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();

        println!("path = {}", csv_path.as_path().to_str().unwrap());
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
        let g = Graph::new();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        lotr_test_rec(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_flag_file_with_header() {
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_flag_has_header_but_file_has_no_header() {
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr-without-header.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_names() {
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr-wrong.csv)").unwrap();
        let has_header = true;
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_delimiter() {
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let has_header = true;
        let delimiter = ".";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }
}
