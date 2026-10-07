//! Reading the Berlin SPARQL Benchmark (BSBM, <http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/>)
//! and the Sparqloscope feature queries, and loading BSBM into a `PersistentGraph` and into an
//! `oxrdf::Dataset`.
//!
//! Shared by the correctness test (`raphtory-rdf-tests/tests/bsbm.rs`) and the benchmark
//! (`rdf-bench/benches/sparql.rs`; both include this file with `#[path]`), so it
//! only uses `raphtory`, `bzip2`, `csv` and `std`.
//!
//! # The data
//!
//! The files are those oxigraph's own benchmark uses: the pre-generated BSBM v3.1 data of Zenodo
//! record 12663333 (Apache-2.0) and the Sparqloscope queries for BSBM 5000
//! (`sparqloscope-bsbm-5000.csv`). The tests and the benchmark read them from
//! `$RAPHTORY_RDF_DATA/bsbm` ([`bsbm_dir`]).
//!
//! - `dataset-<n>.nt.bz2`: the data of `n` products, in N-Triples.
//! - `explore-<n>.csv.bz2`, `businessIntelligence-<n>.csv.bz2` and `exploreAndUpdate-<n>.csv.bz2`:
//!   the query mixes, CSV `id,kind,content` where `id` is the query template, `kind` is `query`
//!   or `update` and `content` the SPARQL text ([`read_mix`]). The business intelligence queries
//!   are on one line and template 3 has a `#` that would comment out the rest, so every `"# "` is
//!   removed from them.
//! - `sparqloscope-bsbm-5000.csv`: `description,query`, queries that each exercise one SPARQL
//!   feature ([`read_sparqloscope`]).
//!
//! # The update stream holds the products
//!
//! `dataset-1000` holds no products, offers or reviews; they are all inserted by the
//! `INSERT DATA` operations of `exploreAndUpdate-1000`, which also has
//! `DELETE WHERE { <offer> ?p ?o }` operations. So the 1,000-product data is `dataset-1000`
//! followed by its updates ([`Bsbm::updates`]). `dataset-5000` holds most of its products and is
//! used without updates.
//!
//! The updates are mapped onto RDF writes ([`apply_raphtory`]): `INSERT DATA` is `load_rdf`, and
//! `DELETE WHERE { <s> ?p ?o }` retracts what `SELECT ?p ?o { <s> ?p ?o }` finds. The dataset is
//! written at [`BASE_TIME`] and update `k` at [`update_time`]`(k)`, so the state after `u`
//! updates is `pg.snapshot_at(BASE_TIME + u)`. The oracle applies them with [`apply_dataset`];
//! the benchmark times them with [`apply_dataset_like_raphtory`], which does the same work as
//! Raphtory.
#![allow(dead_code)]
use bzip2::read::MultiBzDecoder;
use raphtory::{
    prelude::*,
    rdf::{
        evaluator,
        model::{Dataset, GraphName, GraphNameRef, NamedNode, Quad, QuadRef, Term, Triple},
        QueryResults, RdfFormat, RdfMutationOps, RdfParser, RdfViewOps, SparqlResults,
    },
};
use std::{
    collections::HashSet,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

/// The environment variable that points at the directory with the downloaded RDF data (the
/// `RDF_DATA_DIR` of the Makefile); BSBM is in its `bsbm` subdirectory.
pub const DATA_ENV: &str = "RAPHTORY_RDF_DATA";

/// The time of the dataset; update `k` is written at [`update_time`]`(k)`.
pub const BASE_TIME: i64 = 1;

/// The time of update `k` (from 0).
pub fn update_time(k: usize) -> i64 {
    BASE_TIME + 1 + k as i64
}

/// The BSBM directory `$RAPHTORY_RDF_DATA/bsbm`, or why there is none.
pub fn bsbm_dir() -> Result<PathBuf, String> {
    let Some(root) = std::env::var_os(DATA_ENV) else {
        return Err(format!(
            "{DATA_ENV} is not set; set it to the directory that holds bsbm/ \
             (~/.cache/raphtory-rdf by default)"
        ));
    };
    let dir = PathBuf::from(root).join("bsbm");
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

/// The triples of an N-Triples (or, with `turtle`, Turtle) document.
pub fn parse(doc: &[u8], turtle: bool) -> Result<Vec<Triple>, String> {
    let format = if turtle {
        RdfFormat::Turtle
    } else {
        RdfFormat::NTriples
    };
    RdfParser::from_format(format)
        .for_slice(doc)
        .map(|quad| quad.map(Triple::from).map_err(|e| e.to_string()))
        .collect()
}

/// Triples as an N-Triples document.
pub fn ntriples<'a>(triples: impl IntoIterator<Item = &'a Triple>) -> Vec<u8> {
    let mut doc = String::new();
    for triple in triples {
        doc.push_str(&triple.to_string());
        doc.push_str(" .\n");
    }
    doc.into_bytes()
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
    let body = |rest: &str| -> Result<String, String> {
        rest.trim()
            .strip_prefix('{')
            .and_then(|b| b.trim_end().strip_suffix('}'))
            .map(str::to_owned)
            .ok_or_else(|| format!("update without a {{ body }}: {}", short(text)))
    };
    let trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix("INSERT DATA") {
        // the triples are written with full IRIs, as in Turtle
        let triples = parse(body(rest)?.as_bytes(), true)
            .map_err(|e| format!("cannot read the triples of {}: {e}", short(text)))?;
        let doc = ntriples(&triples);
        Ok(Op::Insert {
            text: text.to_owned(),
            triples,
            doc,
        })
    } else if let Some(rest) = trimmed.strip_prefix("DELETE WHERE") {
        let body = body(rest)?;
        let body = body.trim();
        let subject = body
            .strip_prefix('<')
            .and_then(|b| b.split_once('>'))
            .filter(|(_, pattern)| {
                let pattern = pattern.trim();
                pattern == "?p ?o" || pattern == "?p ?o ."
            })
            .map(|(iri, _)| iri)
            .ok_or_else(|| format!("unexpected DELETE WHERE form: {}", short(text)))?;
        let subject =
            NamedNode::new(subject).map_err(|e| format!("bad IRI in {}: {e}", short(text)))?;
        Ok(Op::Delete {
            text: text.to_owned(),
            subject,
        })
    } else {
        Err(format!("unsupported update: {}", short(text)))
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
    /// `exploreAndUpdate-<n>`, empty when the file is not there.
    pub explore_update: Vec<Entry>,
}

impl Bsbm {
    /// The path of a file of the scale.
    pub fn file(dir: &Path, name: &str, products: usize, extension: &str) -> PathBuf {
        dir.join(format!("{name}-{products}.{extension}"))
    }

    /// Reads the files of a scale; `exploreAndUpdate` is optional.
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
        let update_file = Self::file(dir, "exploreAndUpdate", products, "csv.bz2");
        let explore_update = if update_file.is_file() {
            read_mix(&update_file, false)?
        } else {
            Vec::new()
        };
        Ok(Self {
            products,
            dataset,
            dataset_triples,
            explore,
            bi,
            explore_update,
        })
    }

    /// The updates of the explore-and-update mix, in order.
    pub fn updates(&self) -> Vec<&Op> {
        self.explore_update
            .iter()
            .map(|e| &e.op)
            .filter(|op| op.is_update())
            .collect()
    }

    /// The number of triples the updates insert (counting a triple once per insert).
    pub fn inserted_triples(&self) -> usize {
        self.updates()
            .iter()
            .map(|op| match op {
                Op::Insert { triples, .. } => triples.len(),
                _ => 0,
            })
            .sum()
    }
}

/// Applies an update to a graph at time `t` and returns the number of triples written: `INSERT
/// DATA` with `load_rdf`, `DELETE WHERE { <s> ?p ?o }` by retracting each triple that
/// `SELECT ?p ?o { <s> ?p ?o }` finds in the current state. Queries are not applied.
pub fn apply_raphtory(pg: &PersistentGraph, t: i64, op: &Op) -> Result<usize, String> {
    match op {
        Op::Query(_) => Ok(0),
        Op::Insert { doc, .. } => pg
            .load_rdf(t, doc.as_slice(), RdfFormat::NTriples, None)
            .map_err(|e| format!("load_rdf: {e}")),
        Op::Delete { subject, .. } => {
            let query = format!("SELECT ?p ?o WHERE {{ {subject} ?p ?o }}");
            let SparqlResults::Solutions { rows, .. } =
                pg.sparql(&query).map_err(|e| format!("{query}: {e}"))?
            else {
                return Err(format!("{query}: not solutions"));
            };
            for row in &rows {
                let (Some(Term::NamedNode(p)), Some(o)) = (&row[0], &row[1]) else {
                    return Err(format!("{query}: unexpected row {row:?}"));
                };
                let triple = Triple::new(subject.clone(), p.clone(), o.clone());
                pg.delete_triple(t, &triple)
                    .map_err(|e| format!("delete_triple: {e}"))?;
            }
            Ok(rows.len())
        }
    }
}

/// Writes the dataset at [`BASE_TIME`] and update `k` at [`update_time`]`(k)`.
pub fn load_raphtory(pg: &PersistentGraph, bsbm: &Bsbm) -> Result<(), String> {
    pg.load_rdf(
        BASE_TIME,
        bsbm.dataset.as_slice(),
        RdfFormat::NTriples,
        None,
    )
    .map_err(|e| format!("load_rdf: {e}"))?;
    for (k, op) in bsbm.updates().into_iter().enumerate() {
        apply_raphtory(pg, update_time(k), op)?;
    }
    Ok(())
}

/// The triples of an N-Triples document in the default graph of a dataset.
pub fn load_dataset(doc: &[u8]) -> Result<Dataset, String> {
    let mut dataset = Dataset::new();
    for quad in RdfParser::from_format(RdfFormat::NTriples).for_slice(doc) {
        dataset.insert(&quad.map_err(|e| e.to_string())?);
    }
    Ok(dataset)
}

/// Applies an update to the default graph of a dataset. Queries are not applied.
pub fn apply_dataset(dataset: &mut Dataset, op: &Op) {
    match op {
        Op::Query(_) => {}
        Op::Insert { triples, .. } => {
            for t in triples {
                dataset.insert(QuadRef::new(
                    &t.subject,
                    &t.predicate,
                    &t.object,
                    GraphNameRef::DefaultGraph,
                ));
            }
        }
        Op::Delete { subject, .. } => {
            let quads: Vec<Quad> = dataset
                .quads_for_subject(subject)
                .filter(|q| q.graph_name.is_default_graph())
                .map(QuadRef::into_owned)
                .collect();
            for quad in &quads {
                dataset.remove(quad);
            }
        }
    }
}

/// Applies an update to the default graph of a dataset doing the same work as
/// [`apply_raphtory`] (parsing `doc`, running the `SELECT`), for like-for-like timing; returns
/// the number of triples written. The result equals [`apply_dataset`]'s.
pub fn apply_dataset_like_raphtory(dataset: &mut Dataset, op: &Op) -> Result<usize, String> {
    match op {
        Op::Query(_) => Ok(0),
        Op::Insert { doc, .. } => {
            let mut written = 0;
            for quad in RdfParser::from_format(RdfFormat::NTriples).for_slice(doc.as_slice()) {
                dataset.insert(&quad.map_err(|e| e.to_string())?);
                written += 1;
            }
            Ok(written)
        }
        Op::Delete { subject, .. } => {
            let query = format!("SELECT ?p ?o WHERE {{ {subject} ?p ?o }}");
            let mut quads = Vec::new();
            {
                let results = evaluator()
                    .parse_query(&query)
                    .map_err(|e| format!("{query}: {e}"))?
                    .on_queryable_dataset(&*dataset)
                    .execute()
                    .map_err(|e| format!("{query}: {e}"))?;
                let QueryResults::Solutions(solutions) = results else {
                    return Err(format!("{query}: not solutions"));
                };
                for solution in solutions {
                    let solution = solution.map_err(|e| format!("{query}: {e}"))?;
                    let (Some(Term::NamedNode(p)), Some(o)) =
                        (solution.get("p"), solution.get("o"))
                    else {
                        return Err(format!("{query}: unexpected solution {solution:?}"));
                    };
                    quads.push(Quad::new(
                        subject.clone(),
                        p.clone(),
                        o.clone(),
                        GraphName::DefaultGraph,
                    ));
                }
            }
            for quad in &quads {
                dataset.remove(quad);
            }
            Ok(quads.len())
        }
    }
}

/// The dataset and every update in a dataset (the final state).
pub fn final_dataset(bsbm: &Bsbm) -> Result<Dataset, String> {
    let mut dataset = load_dataset(&bsbm.dataset)?;
    for op in bsbm.updates() {
        apply_dataset(&mut dataset, op);
    }
    Ok(dataset)
}
