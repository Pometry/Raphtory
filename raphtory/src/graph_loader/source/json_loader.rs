use crate::core::utils::errors::GraphError;
use bzip2::read::BzDecoder;
use flate2; // 1.0
use flate2::read::GzDecoder;
use rayon::prelude::*;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json::{de::IoRead, Deserializer};
use std::{
    collections::VecDeque,
    error::Error,
    fmt::{Display, Formatter},
    fs,
    fs::File,
    io,
    io::BufReader,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum JsonErr {
    /// An IO error that occurred during file read.
    IoError(io::Error),
    /// A CSV parsing error that occurred while parsing the CSV data.
    JsonError(serde_json::Error),
    /// A GraphError that occurred while loading the CSV data into the graph.
    GraphError(GraphError),
}

impl From<io::Error> for JsonErr {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<serde_json::Error> for JsonErr {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonError(value)
    }
}

impl From<GraphError> for JsonErr {
    fn from(value: GraphError) -> Self {
        Self::GraphError(value)
    }
}

impl Display for JsonErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.source() {
            Some(error) => write!(f, "CSV loader failed with error: {}", error),
            None => write!(f, "CSV loader failed with unknown error"),
        }
    }
}

impl Error for JsonErr {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            JsonErr::IoError(error) => Some(error),
            JsonErr::JsonError(error) => Some(error),
            JsonErr::GraphError(error) => Some(error),
        }
    }
}

/// A struct that defines the CSV loader with configurable options.
#[derive(Debug)]
pub struct JsonLinesLoader<REC: DeserializeOwned> {
    /// Path of the CSV file or directory containing CSV files.
    path: PathBuf,
    /// Optional regex filter to select specific CSV files by name.
    regex_filter: Option<Regex>,
    _a: std::marker::PhantomData<REC>,
    /// print the name of the file being loaded
    print_file_name: bool,
}

impl<REC: DeserializeOwned + std::fmt::Debug + Sync> JsonLinesLoader<REC> {
    /// Creates a new CSV loader with the given path.
    pub fn new(path: PathBuf, regex_filter: Option<Regex>) -> Self {
        Self {
            path,
            regex_filter,
            _a: std::marker::PhantomData,
            print_file_name: false,
        }
    }

    /// If set to true will print the file name as it reads it
    ///
    /// # Arguments
    ///
    /// * `p` - A boolean value indicating whether the CSV file has a header.
    ///
    pub fn set_print_file_name(mut self, p: bool) -> Self {
        self.print_file_name = p;
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
    fn is_dir<P: AsRef<Path>>(p: &P) -> Result<bool, JsonErr> {
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
    fn files_vec(&self) -> Result<Vec<PathBuf>, JsonErr> {
        let mut paths = vec![];
        let mut queue = VecDeque::from([self.path.to_path_buf()]);

        while let Some(ref path) = queue.pop_back() {
            match fs::read_dir(path) {
                Ok(entries) => {
                    for f_path in entries.flatten() {
                        let p = f_path.path();
                        if Self::is_dir(&p)? {
                            queue.push_back(p.clone())
                        } else {
                            self.accept_file(f_path.path(), &mut paths);
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

    /// Load data from all JSON files in the directory into a graph.
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
    pub fn load_into_graph<F, G>(&self, g: &G, loader: F) -> Result<(), JsonErr>
    where
        REC: DeserializeOwned + std::fmt::Debug,
        F: Fn(REC, &G) -> Result<(), GraphError> + Send + Sync,
        G: Sync,
    {
        //FIXME: loader function should return a result for reporting parsing errors
        let paths = self.files_vec()?;
        paths
            .par_iter()
            .try_for_each(move |path| self.load_file_into_graph(path, g, &loader))?;
        Ok(())
    }

    fn json_reader(
        &self,
        file_path: PathBuf,
    ) -> Result<Deserializer<IoRead<BufReader<Box<dyn io::Read>>>>, JsonErr> {
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
            Ok(Deserializer::from_reader(BufReader::new(Box::new(
                GzDecoder::new(f),
            ))))
        } else if is_bziped {
            Ok(Deserializer::from_reader(BufReader::new(Box::new(
                BzDecoder::new(f),
            ))))
        } else {
            Ok(Deserializer::from_reader(BufReader::new(Box::new(f))))
        }
    }

    /// Loads a JSON file into a graph using the specified loader function.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the JSON file to load.
    /// * `g` - A reference to the graph to load the data into.
    /// * `loader` - The function to use for loading the CSV records into the graph.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or a `CsvErr` if there was an error.
    ///
    fn load_file_into_graph<F, P: Into<PathBuf> + std::fmt::Debug, G>(
        &self,
        path: P,
        g: &G,
        loader: &F,
    ) -> Result<(), JsonErr>
    where
        F: Fn(REC, &G) -> Result<(), GraphError>,
    {
        let file_path: PathBuf = path.into();
        if self.print_file_name {
            println!("Loading file: {:?}", file_path);
        }

        let json_reader = self.json_reader(file_path)?;

        let records_iter = json_reader.into_iter::<REC>();

        //TODO this needs better error handling for files without perfect data
        for rec in records_iter {
            if let Ok(record) = rec {
                loader(record, g)?
            } else {
                println!("Error parsing record: {:?}", rec);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use bzip2::{write::BzEncoder, Compression as BzCompression};
    use flate2::{write::GzEncoder, Compression};
    use serde::Deserialize;
    use std::{fs::File, io::Write};
    use tempfile::tempdir;

    #[derive(Debug, Deserialize)]
    struct TestRecord {
        name: String,
        time: i64,
    }

    fn test_json_rec(g: Graph, loader: JsonLinesLoader<TestRecord>) {
        loader
            .load_into_graph(&g, |testrec: TestRecord, g: &Graph| {
                let _ = g.add_vertex(testrec.time.clone(), testrec.name.clone(), NO_PROPS);
                Ok(())
            })
            .expect("Unable to add vertex to graph");
        assert_eq!(g.num_vertices(), 3);
        assert_eq!(g.num_edges(), 0);
        let mut names = g.vertices().into_iter().name().collect::<Vec<String>>();
        names.sort();
        assert_eq!(names, vec!["test", "testbz", "testgz"]);
    }

    #[test]
    fn test_load_into_graph() {
        let dir = tempdir().unwrap();
        let plain_file = dir.path().join("test.json");
        let gzip_file = dir.path().join("test.json.gz");
        let bzip_file = dir.path().join("test.json.bz2");

        // Create plain json file
        File::create(&plain_file)
            .unwrap()
            .write_all(b"{\"name\": \"test\", \"time\": 1}\n")
            .expect("unable to make plain file");

        // Create gzip compressed json file
        let f = File::create(&gzip_file).unwrap();
        let mut gz = GzEncoder::new(f, Compression::fast());
        gz.write_all(b"{\"name\": \"testgz\", \"time\": 2}\n")
            .expect("unable to write to gz file");
        gz.finish().expect("Unable to write GZ file");

        // Create bzip2 compressed json file
        let f = File::create(&bzip_file).unwrap();
        let mut bz = BzEncoder::new(f, BzCompression::fast());
        bz.write_all(b"{\"name\": \"testbz\", \"time\": 3}\n")
            .expect("unable to write to bz file");
        bz.finish().expect("Unable to write BZ file");

        let g = Graph::new();
        let loader = JsonLinesLoader::<TestRecord>::new(dir.path().to_path_buf(), None);
        test_json_rec(g, loader);
    }
}
