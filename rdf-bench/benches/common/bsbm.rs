//! Reading the Berlin SPARQL Benchmark (BSBM, <http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/>)
//! and the Sparqloscope feature queries.
//!
//! Used by the benchmark `rdf-bench/benches/sparql.rs`, which includes this file and
//! `raphtory-rdf-tests/tests/common/data.rs` with `#[path]`, so it only uses `raphtory`,
//! `oxigraph`, `spargebra`, `bzip2`, `csv` and `std`.
//!
//! # The data
//!
//! The files are those oxigraph's own benchmark uses: the pre-generated BSBM v3.1 data of Zenodo
//! record 12663333 (Apache-2.0) and the Sparqloscope queries for BSBM 5000
//! (`sparqloscope-bsbm-5000.csv`). The benchmark reads them from
//! `$RAPHTORY_RDF_DATA/bsbm` ([`bsbm_dir`]).
//!
//! - `dataset-<n>.nt.bz2`: the data of `n` products, in N-Triples.
//! - `explore-<n>.csv.bz2` and `businessIntelligence-<n>.csv.bz2`: the query mixes, CSV
//!   `id,kind,content` where `id` is the query template, `kind` is `query` or `update` and
//!   `content` the SPARQL text ([`read_mix`]). The business intelligence queries
//!   are on one line and template 3 has a `#` that would comment out the rest, so every `"# "` is
//!   removed from them.
//! - `sparqloscope-bsbm-5000.csv`: `description,query`, queries that each exercise one SPARQL
//!   feature ([`read_sparqloscope`]).
#![allow(dead_code)]
use super::data::{data_dir, ntriples};
use bzip2::read::MultiBzDecoder;
use raphtory::rdf::model::{NamedNode, Triple};
use spargebra::{
    algebra::GraphPattern,
    term::{
        GraphName, GraphNamePattern, GroundQuadPattern, GroundTermPattern, NamedNodePattern,
        TermPattern, TriplePattern,
    },
    GraphUpdateOperation, SparqlParser,
};
use std::{
    collections::HashSet,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

/// The time of the dataset.
pub const BASE_TIME: i64 = 1;

/// The BSBM directory `$RAPHTORY_RDF_DATA/bsbm`, or why there is none.
pub fn bsbm_dir() -> Result<PathBuf, String> {
    let dir = data_dir("bsbm")?;
    if dir.join("dataset-1000.nt.bz2").is_file() {
        Ok(dir)
    } else {
        Err(format!(
            "{} has no dataset-1000.nt.bz2 (the BSBM files of Zenodo record 12663333)",
            dir.display()
        ))
    }
}

/// Decompresses a `.bz2` file.
pub fn read_bz2(path: &Path) -> Result<Vec<u8>, String> {
    let file = fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let mut data = Vec::new();
    MultiBzDecoder::new(std::io::BufReader::new(file))
        .read_to_end(&mut data)
        .map_err(|e| format!("cannot decompress {}: {e}", path.display()))?;
    Ok(data)
}

/// One operation of a query mix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// A SPARQL query.
    Query(String),
    /// `INSERT DATA { triples }`: its text, its triples and the triples as N-Triples.
    Insert {
        text: String,
        triples: Vec<Triple>,
        doc: Vec<u8>,
    },
    /// `DELETE WHERE { <subject> ?p ?o }`: its text and its subject.
    Delete { text: String, subject: NamedNode },
}

impl Op {
    /// The SPARQL text of the operation.
    pub fn text(&self) -> &str {
        match self {
            Op::Query(text) | Op::Insert { text, .. } | Op::Delete { text, .. } => text,
        }
    }

    /// Whether the operation is an update.
    pub fn is_update(&self) -> bool {
        !matches!(self, Op::Query(_))
    }
}

/// An operation of a query mix with its template id (the `id` column).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub template: u32,
    pub op: Op,
}

/// Maps a SPARQL update of the BSBM explore-and-update mix onto an [`Op`]. Only the two forms
/// the mix uses are accepted: `INSERT DATA { triples }` and `DELETE WHERE { <s> ?p ?o }`.
pub fn parse_update(text: &str) -> Result<Op, String> {
    let update = SparqlParser::new()
        .parse_update(text)
        .map_err(|e| format!("cannot parse {}: {e}", short(text)))?;
    match update.operations.as_slice() {
        [GraphUpdateOperation::InsertData { data }]
            if data.iter().all(|q| q.graph_name == GraphName::DefaultGraph) =>
        {
            let triples: Vec<Triple> = data
                .iter()
                .map(|q| Triple::new(q.subject.clone(), q.predicate.clone(), q.object.clone()))
                .collect();
            let doc = ntriples(&triples);
            Ok(Op::Insert {
                text: text.to_owned(),
                triples,
                doc,
            })
        }
        [GraphUpdateOperation::DeleteInsert {
            delete,
            insert,
            using: None,
            pattern,
        }] if insert.is_empty() => match (delete.as_slice(), pattern.as_ref()) {
            (
                [GroundQuadPattern {
                    subject: GroundTermPattern::NamedNode(subject),
                    predicate: NamedNodePattern::Variable(p),
                    object: GroundTermPattern::Variable(o),
                    graph_name: GraphNamePattern::DefaultGraph,
                }],
                GraphPattern::Bgp { patterns },
            ) if p != o
                && matches!(patterns.as_slice(), [TriplePattern {
                    subject: TermPattern::NamedNode(s),
                    predicate: NamedNodePattern::Variable(wp),
                    object: TermPattern::Variable(wo),
                }] if s == subject && wp == p && wo == o) =>
            {
                Ok(Op::Delete {
                    text: text.to_owned(),
                    subject: subject.clone(),
                })
            }
            _ => Err(format!("unexpected DELETE WHERE form: {}", short(text))),
        },
        _ => Err(format!("unsupported update: {}", short(text))),
    }
}

/// The start of a long text, for messages.
pub fn short(text: &str) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() > 200 {
        format!("{}...", text.chars().take(200).collect::<String>())
    } else {
        text
    }
}

/// Reads a query mix (`id,kind,content`), compressed with bzip2 if its name ends in `.bz2`.
/// `strip_comments` removes every `"# "` from the queries, as oxigraph does for the business
/// intelligence mix (see the [module docs](self)).
pub fn read_mix(path: &Path, strip_comments: bool) -> Result<Vec<Entry>, String> {
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
        let template = id
            .parse()
            .map_err(|_| format!("{}: bad template id {id:?}", path.display()))?;
        let op = match kind {
            "query" if strip_comments => Op::Query(content.replace("# ", "")),
            "query" => Op::Query(content.to_owned()),
            "update" => parse_update(content)?,
            _ => {
                return Err(format!(
                    "{}: unknown operation kind {kind:?}",
                    path.display()
                ))
            }
        };
        entries.push(Entry { template, op });
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
        .filter_map(|entry| match &entry.op {
            Op::Query(q) if seen.insert(q.as_str()) => Some((entry.template, q.as_str())),
            _ => None,
        })
        .collect()
}

/// The template ids of a mix, sorted.
pub fn templates(entries: &[Entry]) -> Vec<u32> {
    let mut ids: Vec<u32> = entries
        .iter()
        .filter(|e| !e.op.is_update())
        .map(|e| e.template)
        .collect();
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
    /// The number of products the files were generated for (`1000` or `5000`).
    pub products: usize,
    /// `dataset-<n>.nt`, decompressed.
    pub dataset: Vec<u8>,
    /// The number of triples (lines) of the dataset.
    pub dataset_triples: usize,
    /// `explore-<n>`.
    pub explore: Vec<Entry>,
    /// `businessIntelligence-<n>`.
    pub bi: Vec<Entry>,
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
        let explore = read_mix(&Self::file(dir, "explore", products, "csv.bz2"), false)?;
        let bi = read_mix(
            &Self::file(dir, "businessIntelligence", products, "csv.bz2"),
            true,
        )?;
        Ok(Self {
            products,
            dataset,
            dataset_triples,
            explore,
            bi,
        })
    }
}
