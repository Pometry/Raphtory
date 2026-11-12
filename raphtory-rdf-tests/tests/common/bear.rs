//! Reading BEAR-B, the DBpedia Live part of BEAR (the BEnchmark of RDF ARchives,
//! <https://aic.ai.wu.ac.at/qadlod/bear.html>), and loading its versions into a
//! `RdfGraph`.
//!
//! Included with `#[path]`, with `data.rs`, by `tests/bear.rs` and `benches/temporal.rs` of
//! `raphtory-rdf-tests`, so it only uses the dependencies of its `rdf` feature.
//!
//! # The data
//!
//! `make bear-b-fetch` downloads the archives into `$(RDF_DATA_DIR)/bear-b`; the tests and the
//! benchmark read `$RAPHTORY_RDF_DATA/bear-b` ([`bear_b_dir`]).
//!
//! The change-based archive of a granularity, `datasets/<day|instant>/CB/alldata.CB.nt.tar.gz`,
//! holds for every pair of consecutive versions `i -> i+1` (numbered from 1) the triples added
//! (`data-added_<i>-<i+1>.nt.gz`) and deleted (`data-deleted_<i>-<i+1>.nt.gz`) between them.
//!
//! # Version 1 is not in the archive
//!
//! [`Changes::read`] reconstructs the part of version 1 that ever changes: every triple a delta
//! deletes before any delta adds it. The rest is its *static core*, which no delta mentions. For
//! the single-triple-pattern lookups (`?s <p> ?o`, `?s <p> <o>`), leaving it out changes no Diff,
//! and changes every Mat and Ver result by the same rows. A join can match a triple of the static
//! core with one that changes, so its results can differ: the reconstructed versions are not
//! BEAR-B's own.
//!
//! # Versions are times
//!
//! [`load`] writes version `k` at time `t = k`: version 1 at `t = 1`, and for every pair
//! `i -> i+1` the deleted triples are retracted and then the added ones asserted, both at
//! `t = i + 1`. Retractions go first so that a triple a delta deletes and adds stays, as in
//! `(V \ deleted) ∪ added`.
#![allow(
    dead_code,
    reason = "each includer uses a different part of this module"
)]
use super::data::{data_dir, ntriples, parse};
use flate2::read::{GzDecoder, MultiGzDecoder};
use raphtory_rdf::{RdfError, RdfFormat, RdfGraph, Triple, ASOF_NS};
use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    ffi::OsStr,
    fmt,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use tar::Archive;
use zip::ZipArchive;

/// The BEAR-B directory `$RAPHTORY_RDF_DATA/bear-b`, or why there is none.
pub fn bear_b_dir() -> Result<PathBuf, String> {
    let dir = data_dir("bear-b")?;
    if dir.join("datasets").is_dir() {
        Ok(dir)
    } else {
        Err(format!(
            "{} has no datasets/ directory (`make bear-b-fetch` downloads BEAR-B)",
            dir.display()
        ))
    }
}

/// The granularities of BEAR-B read here: the DBpedia Live changes aggregated by day, and every
/// single change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Granularity {
    Day,
    Instant,
}

impl Granularity {
    pub fn name(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Instant => "instant",
        }
    }

    /// The change-based archive of this granularity under the BEAR-B directory.
    pub fn archive(self, bear_b: &Path) -> PathBuf {
        bear_b
            .join("datasets")
            .join(self.name())
            .join("CB")
            .join("alldata.CB.nt.tar.gz")
    }
}

/// The changes between two consecutive versions, as N-Triples documents.
#[derive(Clone, Debug)]
pub struct Delta {
    pub added: Vec<u8>,
    pub deleted: Vec<u8>,
}

/// The versions of an archive: the reconstructed part of version 1 and the deltas.
#[derive(Debug)]
pub struct Changes {
    /// The triples of version 1 that some delta deletes (see the [module docs](self)).
    pub base: Vec<Triple>,
    /// `base` as an N-Triples document.
    pub base_doc: Vec<u8>,
    /// `deltas[i]` turns version `i + 1` into version `i + 2`.
    pub deltas: Vec<Delta>,
    /// The number of triples in all the deltas, added and deleted.
    pub delta_triples: usize,
}

impl Changes {
    /// Reads a change-based archive and reconstructs version 1.
    pub fn read(archive: &Path) -> Result<Self, String> {
        let deltas = read_deltas(archive).map_err(|e| format!("{}: {e}", archive.display()))?;
        Self::from_deltas(deltas)
    }

    /// Reconstructs version 1 from the deltas: the triples that a delta deletes when no earlier
    /// delta has added them (or after an earlier delta deleted them as well).
    pub fn from_deltas(deltas: Vec<Delta>) -> Result<Self, String> {
        // the triples of the versions so far, without the unknown part of version 1
        let mut present: HashSet<Triple> = HashSet::new();
        let mut in_base: HashSet<Triple> = HashSet::new();
        let mut base = Vec::new();
        let mut delta_triples = 0;
        for (i, delta) in deltas.iter().enumerate() {
            let deleted = parse(&delta.deleted, RdfFormat::NTriples)
                .map_err(|e| format!("delta {}: {e}", i + 1))?;
            let added = parse(&delta.added, RdfFormat::NTriples)
                .map_err(|e| format!("delta {}: {e}", i + 1))?;
            delta_triples += deleted.len() + added.len();
            for triple in deleted {
                if !present.remove(&triple) && in_base.insert(triple.clone()) {
                    base.push(triple);
                }
            }
            present.extend(added);
        }
        let base_doc = ntriples(&base);
        Ok(Self {
            base,
            base_doc,
            deltas,
            delta_triples,
        })
    }

    /// The number of versions, including version 1.
    pub fn versions(&self) -> usize {
        self.deltas.len() + 1
    }

    /// The triples written by [`load`]: the base, then every delta.
    pub fn triples(&self) -> usize {
        self.base.len() + self.delta_triples
    }
}

/// Reads the deltas of a change-based archive, a `.tar.gz` of `data-added_<i>-<i+1>.nt.gz` and
/// `data-deleted_<i>-<i+1>.nt.gz` files, which must be there for every `i` from 1 on.
pub fn read_deltas(archive: &Path) -> Result<Vec<Delta>, Box<dyn Error>> {
    let mut added = BTreeMap::new();
    let mut deleted = BTreeMap::new();
    for entry in Archive::new(GzDecoder::new(File::open(archive)?)).entries()? {
        let mut entry = entry?;
        let Some(file) = entry
            .path()?
            .file_name()
            .and_then(OsStr::to_str)
            .map(str::to_owned)
        else {
            continue;
        };
        let Some((adds, from)) = delta_name(&file) else {
            continue;
        };
        let mut doc = Vec::new();
        let read = if file.ends_with(".gz") {
            MultiGzDecoder::new(&mut entry).read_to_end(&mut doc)
        } else {
            entry.read_to_end(&mut doc)
        };
        read.map_err(|e| format!("{file}: {e}"))?;
        let map = if adds { &mut added } else { &mut deleted };
        if map.insert(from, doc).is_some() {
            return Err(format!("{file} is in the archive twice").into());
        }
    }
    let n = added.len().max(deleted.len());
    for i in 1..=n {
        if !added.contains_key(&i) || !deleted.contains_key(&i) {
            return Err(format!("the changes from version {i} to {} are missing", i + 1).into());
        }
    }
    Ok(added
        .into_values()
        .zip(deleted.into_values())
        .map(|(added, deleted)| Delta { added, deleted })
        .collect())
}

/// `(adds, i)` for the file of the delta `i -> i+1`, where `adds` is whether it holds the added
/// triples rather than the deleted ones.
fn delta_name(file: &str) -> Option<(bool, usize)> {
    let stem = file
        .strip_suffix(".nt.gz")
        .or_else(|| file.strip_suffix(".nt"))?;
    let (adds, range) = if let Some(range) = stem.strip_prefix("data-added_") {
        (true, range)
    } else {
        (false, stem.strip_prefix("data-deleted_")?)
    };
    let (from, to) = range.split_once('-')?;
    let (from, to): (usize, usize) = (from.parse().ok()?, to.parse().ok()?);
    (to == from + 1 && from >= 1).then_some((adds, from))
}

/// Writes the versions into `g` (see the [module docs](self)).
pub fn load(g: &RdfGraph, changes: &Changes) -> Result<(), RdfError> {
    g.load_rdf(1, changes.base_doc.as_slice(), RdfFormat::NTriples, None)?;
    for (i, delta) in changes.deltas.iter().enumerate() {
        let t = i as i64 + 2;
        g.retract_rdf(t, delta.deleted.as_slice(), RdfFormat::NTriples, None)?;
        g.load_rdf(t, delta.added.as_slice(), RdfFormat::NTriples, None)?;
    }
    Ok(())
}

/// The kinds of BEAR-B queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// `?s <p> ?o`
    P,
    /// `?s <p> <o>`
    Po,
    /// Two triple patterns sharing a variable.
    Join,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::P => "p",
            Self::Po => "po",
            Self::Join => "join",
        }
    }
}

/// A query of the suite.
#[derive(Clone, Debug)]
pub struct BearQuery {
    pub kind: Kind,
    /// Its number in its file (from 1), as the official results name it.
    pub number: usize,
    /// The graph pattern (the body of `SELECT * { .. }`).
    pub pattern: String,
}

impl fmt::Display for BearQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.kind.name(), self.number)
    }
}

/// The IRI of the time graph of version `t`.
pub fn asof(t: i64) -> String {
    format!("<{ASOF_NS}{t}>")
}

impl BearQuery {
    pub fn mat(&self) -> String {
        format!("SELECT * {{ {} }}", self.pattern)
    }

    pub fn mat_asof(&self, t: i64) -> String {
        format!("SELECT * {{ GRAPH {} {{ {} }} }}", asof(t), self.pattern)
    }

    /// The Diff template between versions `i` and `j`.
    pub fn diff(&self, i: i64, j: i64) -> String {
        let (gi, gj, q) = (asof(i), asof(j), &self.pattern);
        format!(
            "SELECT * {{
                {{ GRAPH {gj} {{ {q} }} FILTER NOT EXISTS {{ GRAPH {gi} {{ {q} }} }} BIND(\"+\" AS ?op) }}
                UNION
                {{ GRAPH {gi} {{ {q} }} FILTER NOT EXISTS {{ GRAPH {gj} {{ {q} }} }} BIND(\"-\" AS ?op) }}
            }}"
        )
    }

    pub fn ver_values(&self, versions: &[i64]) -> String {
        let graphs: Vec<String> = versions.iter().map(|&t| asof(t)).collect();
        format!(
            "SELECT * {{ VALUES ?g {{ {} }} GRAPH ?g {{ {} }} }}",
            graphs.join(" "),
            self.pattern
        )
    }
}

/// Reads the queries of the suite: `Queries/p/p.txt`, `Queries/po/po.txt` and the
/// `joinN.txt` files of `Queries/joins.zip`.
pub fn read_queries(dir: &Path) -> Result<Vec<BearQuery>, String> {
    let mut queries = Vec::new();
    for (kind, file) in [
        (Kind::P, "Queries/p/p.txt"),
        (Kind::Po, "Queries/po/po.txt"),
    ] {
        let path = dir.join(file);
        let text = fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let patterns = text.lines().map(str::trim).filter(|l| !l.is_empty());
        for (i, pattern) in patterns.enumerate() {
            queries.push(BearQuery {
                kind,
                number: i + 1,
                pattern: pattern.to_owned(),
            });
        }
    }
    let joins = dir.join("Queries/joins.zip");
    let mut numbered = BTreeMap::new();
    for (name, text) in read_zip(&joins)? {
        let file = name.rsplit('/').next().unwrap_or(&name);
        let Some(number) = file
            .strip_prefix("join")
            .and_then(|f| f.strip_suffix(".txt"))
            .and_then(|n| n.parse::<usize>().ok())
        else {
            continue;
        };
        let pattern = text.trim();
        let pattern = pattern
            .strip_prefix('{')
            .and_then(|p| p.strip_suffix('}'))
            .ok_or_else(|| format!("{name} is not a group graph pattern"))?
            .trim();
        numbered.insert(number, pattern.to_owned());
    }
    for (number, pattern) in numbered {
        queries.push(BearQuery {
            kind: Kind::Join,
            number,
            pattern,
        });
    }
    Ok(queries)
}

/// The files of a zip archive: names and contents (decoded as UTF-8, lossily).
pub fn read_zip(path: &Path) -> Result<Vec<(String, String)>, String> {
    let file = File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let mut archive = ZipArchive::new(file).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry
            .name()
            .map_err(|e| format!("{}: {e}", path.display()))?
            .into_owned();
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        files.push((name, String::from_utf8_lossy(&data).into_owned()));
    }
    Ok(files)
}
