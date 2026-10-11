//! Reading the Berlin SPARQL Benchmark (BSBM, <http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/>)
//! and the Sparqloscope feature queries.
//!
//! Used by the benchmark `raphtory-rdf-tests/benches/sparql.rs`, which includes this file and
//! `tests/common/data.rs` with `#[path]`.
//!
//! # The data
//!
//! The files are those oxigraph's own benchmark uses: the pre-generated BSBM v3.1 data of Zenodo
//! record 12663333 (Apache-2.0) and the Sparqloscope queries for BSBM 5000
//! (`sparqloscope-bsbm-5000.csv`). The benchmark reads them from
//! `$RAPHTORY_RDF_DATA/bsbm` ([`bsbm_dir`]).
//!
//! - `dataset-<n>.nt.bz2`: the data of `n` products, in N-Triples.
//! - `explore-<n>.csv.bz2`: the query mix, CSV `id,kind,content` where `id` is the query
//!   template, `kind` is `query` or `update` and `content` the SPARQL text ([`read_mix`]).
//! - `sparqloscope-bsbm-5000.csv`: `description,query`, queries that each exercise one SPARQL
//!   feature ([`read_sparqloscope`]).
use super::data::data_dir;
use bzip2::read::MultiBzDecoder;
use std::{
    collections::HashSet,
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

/// The time of the dataset.
pub const BASE_TIME: i64 = 1;

/// The BSBM directory `$RAPHTORY_RDF_DATA/bsbm`, or why there is none.
pub fn bsbm_dir() -> Result<PathBuf, String> {
    let dir = data_dir("bsbm")?;
    if dir.join("dataset-5000.nt.bz2").is_file() {
        Ok(dir)
    } else {
        Err(format!(
            "{} has no dataset-5000.nt.bz2 (the BSBM files of Zenodo record 12663333)",
            dir.display()
        ))
    }
}

/// Decompresses a `.bz2` file.
pub fn read_bz2(path: &Path) -> Result<Vec<u8>, String> {
    let file = fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let mut data = Vec::new();
    MultiBzDecoder::new(BufReader::new(file))
        .read_to_end(&mut data)
        .map_err(|e| format!("cannot decompress {}: {e}", path.display()))?;
    Ok(data)
}

/// A query of a query mix with its template id (the `id` column).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub template: u32,
    pub query: String,
}

/// Reads the queries of a query mix (`id,kind,content`), compressed with bzip2 if its name ends
/// in `.bz2`.
pub fn read_mix(path: &Path) -> Result<Vec<Entry>, String> {
    let data = if path.extension().is_some_and(|e| e == "bz2") {
        read_bz2(path)?
    } else {
        fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?
    };
    let mut entries = Vec::new();
    for record in csv::Reader::from_reader(data.as_slice()).records() {
        let record = record.map_err(|e| format!("{}: {e}", path.display()))?;
        let (Some(id), Some(kind), Some(content)) = (record.get(0), record.get(1), record.get(2))
        else {
            return Err(format!("{}: a row has fewer than 3 fields", path.display()));
        };
        if kind != "query" {
            continue;
        }
        let template = id
            .parse()
            .map_err(|_| format!("{}: bad template id {id:?}", path.display()))?;
        entries.push(Entry {
            template,
            query: content.to_owned(),
        });
    }
    Ok(entries)
}

/// Reads the Sparqloscope queries (`description,query`).
pub fn read_sparqloscope(path: &Path) -> Result<Vec<(String, String)>, String> {
    let data = fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    csv::Reader::from_reader(data.as_slice())
        .records()
        .map(|record| {
            let record = record.map_err(|e| format!("{}: {e}", path.display()))?;
            match (record.get(0), record.get(1)) {
                (Some(description), Some(query)) => Ok((description.to_owned(), query.to_owned())),
                _ => Err(format!("{}: a row has fewer than 2 fields", path.display())),
            }
        })
        .collect()
}

/// The distinct queries of a mix, in order of first appearance, with their template id.
pub fn distinct_queries(entries: &[Entry]) -> Vec<(u32, &str)> {
    let mut seen = HashSet::new();
    entries
        .iter()
        .filter(|entry| seen.insert(entry.query.as_str()))
        .map(|entry| (entry.template, entry.query.as_str()))
        .collect()
}

/// The template ids of a mix, sorted.
pub fn templates(entries: &[Entry]) -> Vec<u32> {
    let mut ids: Vec<u32> = entries.iter().map(|e| e.template).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// The first `n` distinct queries of a template.
pub fn instances(entries: &[Entry], template: u32, n: usize) -> Vec<String> {
    distinct_queries(entries)
        .into_iter()
        .filter(|(t, _)| *t == template)
        .take(n)
        .map(|(_, q)| q.to_owned())
        .collect()
}

/// The files of one BSBM scale.
pub struct Bsbm {
    /// The number of products the files were generated for.
    pub products: usize,
    /// `dataset-<n>.nt`, decompressed.
    pub dataset: Vec<u8>,
    /// The number of triples (lines) of the dataset.
    pub dataset_triples: usize,
    /// `explore-<n>`.
    pub explore: Vec<Entry>,
}

impl Bsbm {
    /// The path of a file of the scale.
    pub fn file(dir: &Path, name: &str, products: usize, extension: &str) -> PathBuf {
        dir.join(format!("{name}-{products}.{extension}"))
    }

    /// Reads the files of a scale.
    pub fn read(dir: &Path, products: usize) -> Result<Self, String> {
        let dataset = read_bz2(&Self::file(dir, "dataset", products, "nt.bz2"))?;
        let dataset_triples = dataset
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .count();
        let explore = read_mix(&Self::file(dir, "explore", products, "csv.bz2"))?;
        Ok(Self {
            products,
            dataset,
            dataset_triples,
            explore,
        })
    }
}
