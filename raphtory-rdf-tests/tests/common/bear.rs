//! Reading BEAR-B, the DBpedia Live part of BEAR (the BEnchmark of RDF ARchives,
//! <https://aic.ai.wu.ac.at/qadlod/bear.html>), and loading its versions into a
//! `PersistentGraph`.
//!
//! Shared by `raphtory-rdf-tests/tests/bear.rs` and `rdf-bench/benches/temporal.rs` (both
//! include this file with `#[path]`), so it only uses `raphtory`, `flate2`, `zip` and
//! `std`.
//!
//! # The data
//!
//! `make bear-b-fetch` downloads the archives into `$(RDF_DATA_DIR)/bear-b`; the tests and the
//! benchmark read `$RAPHTORY_RDF_DATA/bear-b` ([`bear_b_dir`]).
//!
//! The change-based archive of a granularity, `datasets/<day|hour|instant>/CB/alldata.CB.nt.tar.gz`,
//! holds for every pair of consecutive versions `i -> i+1` (numbered from 1) the triples added
//! (`data-added_<i>-<i+1>.nt.gz`) and deleted (`data-deleted_<i>-<i+1>.nt.gz`) between them.
//!
//! # Version 1 is not in the archive
//!
//! [`Changes::read`] reconstructs the part of version 1 that ever changes: every triple a delta
//! deletes before any delta adds it. The rest is its *static core*, which no delta mentions;
//! leaving it out changes no Diff, and changes every Mat and Ver result by the same rows.
//!
//! # Versions are times
//!
//! [`load`] writes version `k` at time `t = k`: version 1 at `t = 1`, and for every pair
//! `i -> i+1` the deleted triples are retracted and then the added ones asserted, both at
//! `t = i + 1`. Retractions go first so that a triple a delta deletes and adds stays, as in
//! `(V \ deleted) ∪ added`. BEAR's own results number versions from 0 (`t = v + 1`).
#![allow(dead_code)]
use flate2::read::{GzDecoder, MultiGzDecoder};
use raphtory::{
    api::core::storage::timeindex::{AsTime, EventTime},
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{NamedNode, Term, Triple},
        name_of, RdfFormat, RdfMutationOps, RdfParser, ASOF_NS,
    },
};
use std::{
    collections::{BTreeMap, HashSet},
    fmt, fs,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
};

/// The environment variable that points at the directory with the downloaded RDF data (the
/// `RDF_DATA_DIR` of the Makefile); BEAR-B is in its `bear-b` subdirectory.
pub const DATA_ENV: &str = "RAPHTORY_RDF_DATA";

/// The BEAR-B directory `$RAPHTORY_RDF_DATA/bear-b`, or why there is none.
pub fn bear_b_dir() -> Result<PathBuf, String> {
    let Some(root) = std::env::var_os(DATA_ENV) else {
        return Err(format!(
            "{DATA_ENV} is not set; set it to the directory `make bear-b-fetch` downloads to \
             (~/.cache/raphtory-rdf by default)"
        ));
    };
    let dir = PathBuf::from(root).join("bear-b");
    if dir.join("datasets").is_dir() {
        Ok(dir)
    } else {
        Err(format!(
            "{} has no datasets/ directory (`make bear-b-fetch` downloads BEAR-B)",
            dir.display()
        ))
    }
}

/// The three granularities of BEAR-B: the DBpedia Live changes aggregated by day and by hour,
/// and every single change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Granularity {
    Day,
    Hour,
    Instant,
}

impl Granularity {
    pub const ALL: [Self; 3] = [Self::Day, Self::Hour, Self::Instant];

    pub fn name(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Hour => "hour",
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
#[derive(Clone, Debug, Default)]
pub struct Delta {
    pub added: Vec<u8>,
    pub deleted: Vec<u8>,
}

/// The versions of an archive: the reconstructed part of version 1 and the deltas.
#[derive(Clone, Debug)]
pub struct Changes {
    /// The triples of version 1 that some delta deletes (see the [module docs](self)).
    pub base: Vec<Triple>,
    /// `base` as an N-Triples document.
    pub base_doc: Vec<u8>,
    /// `deltas[i]` turns version `i + 1` into version `i + 2`.
    pub deltas: Vec<Delta>,
    /// The number of triples in all the deltas, added and deleted.
    pub delta_triples: usize,
    /// Whether any triple has a blank node; [`load`] then keeps their labels.
    pub blank_nodes: bool,
}

impl Changes {
    /// Reads a change-based archive and reconstructs version 1.
    pub fn read(archive: &Path) -> Result<Self, String> {
        Self::from_deltas(read_deltas(archive)?)
    }

    /// Reconstructs version 1 from the deltas: the triples that a delta deletes when no earlier
    /// delta has added them (or after an earlier delta deleted them as well).
    pub fn from_deltas(deltas: Vec<Delta>) -> Result<Self, String> {
        // the triples of the versions so far, without the unknown part of version 1
        let mut present: HashSet<Triple> = HashSet::new();
        let mut in_base: HashSet<Triple> = HashSet::new();
        let mut base = Vec::new();
        let mut delta_triples = 0;
        let mut blank_nodes = false;
        for (i, delta) in deltas.iter().enumerate() {
            let deleted = parse(&delta.deleted).map_err(|e| format!("delta {}: {e}", i + 1))?;
            let added = parse(&delta.added).map_err(|e| format!("delta {}: {e}", i + 1))?;
            delta_triples += deleted.len() + added.len();
            blank_nodes |= deleted.iter().chain(&added).any(has_blank_node);
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
            blank_nodes,
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

/// Whether a triple has a blank node.
pub fn has_blank_node(triple: &Triple) -> bool {
    triple.subject.is_blank_node() || triple.object.is_blank_node()
}

/// The triples of an N-Triples document.
pub fn parse(doc: &[u8]) -> Result<Vec<Triple>, String> {
    RdfParser::from_format(RdfFormat::NTriples)
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

/// Reads the deltas of a change-based archive (a `.tar.gz` of `.nt.gz` files), in order.
pub fn read_deltas(archive: &Path) -> Result<Vec<Delta>, String> {
    let gz = fs::read(archive).map_err(|e| format!("cannot read {}: {e}", archive.display()))?;
    let mut tar = Vec::new();
    GzDecoder::new(gz.as_slice())
        .read_to_end(&mut tar)
        .map_err(|e| format!("{}: {e}", archive.display()))?;
    deltas_of_tar(&tar).map_err(|e| format!("{}: {e}", archive.display()))
}

/// The deltas in a tar archive of `data-added_<i>-<i+1>.nt[.gz]` and
/// `data-deleted_<i>-<i+1>.nt[.gz]` files, which must be there for every `i` from 1 on.
pub fn deltas_of_tar(tar: &[u8]) -> Result<Vec<Delta>, String> {
    let mut added = BTreeMap::new();
    let mut deleted = BTreeMap::new();
    for (path, data) in untar(tar)? {
        let file = path.rsplit('/').next().unwrap_or(&path);
        let Some((kind, from)) = delta_name(file) else {
            continue;
        };
        let doc = if file.ends_with(".gz") {
            let mut doc = Vec::new();
            MultiGzDecoder::new(data)
                .read_to_end(&mut doc)
                .map_err(|e| format!("{path}: {e}"))?;
            doc
        } else {
            data.to_vec()
        };
        let map = if kind == "added" {
            &mut added
        } else {
            &mut deleted
        };
        if map.insert(from, doc).is_some() {
            return Err(format!("{file} is in the archive twice"));
        }
    }
    let n = added.len().max(deleted.len());
    for i in 1..=n {
        if !added.contains_key(&i) || !deleted.contains_key(&i) {
            return Err(format!(
                "the changes from version {i} to {} are missing",
                i + 1
            ));
        }
    }
    Ok(added
        .into_values()
        .zip(deleted.into_values())
        .map(|(added, deleted)| Delta { added, deleted })
        .collect())
}

/// `("added" | "deleted", i)` for the file of the delta `i -> i+1`.
fn delta_name(file: &str) -> Option<(&'static str, usize)> {
    let stem = file
        .strip_suffix(".nt.gz")
        .or_else(|| file.strip_suffix(".nt"))?;
    let (kind, range) = if let Some(range) = stem.strip_prefix("data-added_") {
        ("added", range)
    } else {
        ("deleted", stem.strip_prefix("data-deleted_")?)
    };
    let (from, to) = range.split_once('-')?;
    let (from, to): (usize, usize) = (from.parse().ok()?, to.parse().ok()?);
    (to == from + 1 && from >= 1).then_some((kind, from))
}

/// The regular files of a tar archive (ustar, with GNU long names and pax paths): their paths
/// and contents.
pub fn untar(tar: &[u8]) -> Result<Vec<(String, &[u8])>, String> {
    fn field(header: &[u8]) -> &[u8] {
        let end = header.iter().position(|&b| b == 0).unwrap_or(header.len());
        &header[..end]
    }
    let mut files = Vec::new();
    let mut pos = 0;
    let mut long_name: Option<String> = None;
    while pos + 512 <= tar.len() {
        let header = &tar[pos..pos + 512];
        if header.iter().all(|&b| b == 0) {
            break;
        }
        let size = std::str::from_utf8(field(&header[124..136]))
            .ok()
            .map(|s| s.trim_matches([' ', '\0']))
            .and_then(|s| usize::from_str_radix(s, 8).ok())
            .ok_or_else(|| format!("bad tar header at byte {pos}"))?;
        let start = pos + 512;
        let data = tar
            .get(start..start + size)
            .ok_or_else(|| format!("the tar archive ends inside an entry at byte {pos}"))?;
        let mut name = String::from_utf8_lossy(field(&header[..100])).into_owned();
        if &header[257..262] == b"ustar" {
            let prefix = field(&header[345..500]);
            if !prefix.is_empty() {
                name = format!("{}/{name}", String::from_utf8_lossy(prefix));
            }
        }
        if let Some(long) = long_name.take() {
            name = long;
        }
        match header[156] {
            // GNU long name of the next entry
            b'L' => long_name = Some(String::from_utf8_lossy(field(data)).into_owned()),
            // pax extended header: its `path` is the name of the next entry
            b'x' => {
                long_name = String::from_utf8_lossy(data).lines().find_map(|record| {
                    let (_, kv) = record.split_once(' ')?;
                    kv.strip_prefix("path=").map(str::to_owned)
                })
            }
            b'0' | 0 => files.push((name, data)),
            _ => {}
        }
        pos = start + size.div_ceil(512) * 512;
    }
    Ok(files)
}

/// Writes the versions into `g` (see the [module docs](self)). With blank nodes it writes
/// triple by triple, since `load_rdf` renames blank nodes and later deletions would not match.
pub fn load<G: RdfMutationOps>(g: &G, changes: &Changes) -> Result<(), GraphError> {
    let write = |t: i64, doc: &[u8], retract: bool| -> Result<(), GraphError> {
        if changes.blank_nodes {
            for triple in parse(doc).expect("the documents were parsed by Changes::from_deltas") {
                if retract {
                    g.delete_triple(t, &triple)?;
                } else {
                    g.add_triple(t, &triple)?;
                }
            }
        } else if retract {
            g.retract_rdf(t, doc, RdfFormat::NTriples, None)?;
        } else {
            g.load_rdf(t, doc, RdfFormat::NTriples, None)?;
        }
        Ok(())
    };
    write(1, &changes.base_doc, false)?;
    for (i, delta) in changes.deltas.iter().enumerate() {
        let t = i as i64 + 2;
        write(t, &delta.deleted, true)?;
        write(t, &delta.added, false)?;
    }
    Ok(())
}

/// A BEAR-B lookup query: `?s <p> ?o .` (`?P?`, `Queries/p/p.txt`) or `?s <p> <o> .`
/// (`?PO`, `Queries/po/po.txt`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    /// The triple pattern, as written.
    pub pattern: String,
    pub predicate: NamedNode,
    /// The object of a `?PO` query.
    pub object: Option<Term>,
}

impl Lookup {
    /// Parses one line of `p.txt` or `po.txt`.
    pub fn parse(line: &str) -> Result<Self, String> {
        let pattern = line.trim();
        let body = pattern.strip_suffix('.').unwrap_or(pattern).trim();
        let rest = body
            .strip_prefix("?s")
            .ok_or_else(|| format!("not a BEAR-B lookup: {line}"))?
            .trim_start();
        let end = rest
            .find('>')
            .ok_or_else(|| format!("no predicate IRI: {line}"))?;
        let predicate = NamedNode::from_str(&rest[..=end]).map_err(|e| format!("{line}: {e}"))?;
        let object = match rest[end + 1..].trim() {
            "?o" => None,
            object => Some(Term::from_str(object).map_err(|e| format!("{line}: {e}"))?),
        };
        Ok(Self {
            pattern: pattern.to_owned(),
            predicate,
            object,
        })
    }

    /// The queries of a query file, one per non-empty line.
    pub fn read_all(path: &Path) -> Result<Vec<Self>, String> {
        fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(Self::parse)
            .collect()
    }

    /// The layer of the predicate.
    pub fn layer(&self) -> &str {
        self.predicate.as_str()
    }

    /// The node name of the object of a `?PO` query.
    pub fn object_name(&self) -> Option<String> {
        self.object.as_ref().and_then(|o| name_of(o.as_ref()))
    }

    /// The triple pattern with every variable bound to the terms of a triple.
    pub fn triple(&self, subject: Term, object: Term) -> String {
        format!("{subject} {} {object} .", self.predicate)
    }
}

/// A triple of a lookup, by node names: `(subject, object)`.
pub type Pair = (String, String);

/// `?P?` and `?PO` natively: the `(subject, object)` names of the triples of a lookup visible
/// as of `t`, from the edges of its layer in `pg.snapshot_at(t)`.
pub fn native_mat(pg: &PersistentGraph, q: &Lookup, t: i64) -> Vec<Pair> {
    let view = pg.snapshot_at(t).valid_layers(q.layer()).valid();
    match q.object_name() {
        Some(o) => match view.node(o.as_str()) {
            Some(node) => node
                .in_edges()
                .iter()
                .map(|e| (e.src().name(), o.clone()))
                .collect(),
            None => Vec::new(),
        },
        None => view
            .edges()
            .iter()
            .map(|e| (e.src().name(), e.dst().name()))
            .collect(),
    }
}

/// The validity runs `[from, to)` of a triple from its events (`to` is `None` while the run is
/// open): it holds at `t` if its latest event at or before `t` is an assertion, and at an
/// identical event time a retraction wins (as `raphtory:validFromTime` reads them).
pub fn runs(mut events: Events) -> Vec<(i64, Option<i64>)> {
    events.sort_unstable_by_key(|&(t, asserted)| (t, !asserted));
    let mut runs = Vec::new();
    let mut open: Option<i64> = None;
    for group in events.chunk_by(|(a, _), (b, _)| a.t() == b.t()) {
        let t = group[0].0.t();
        let holds = group[group.len() - 1].1;
        match (open, holds) {
            (None, true) => open = Some(t),
            (Some(from), false) => {
                runs.push((from, Some(t)));
                open = None;
            }
            _ => {}
        }
    }
    if let Some(from) = open {
        runs.push((from, None));
    }
    runs
}

/// Whether a run list holds at `t`.
pub fn holds_at(runs: &[(i64, Option<i64>)], t: i64) -> bool {
    runs.iter()
        .any(|&(from, to)| from <= t && to.is_none_or(|to| t < to))
}

/// The events of a triple: `(time, asserted)`.
pub type Events = Vec<(EventTime, bool)>;

/// Every triple of a lookup in the whole history of `pg` (deleted ones too), with its events:
/// `(subject, object, events)`, from the edges of its layer.
fn histories(pg: &PersistentGraph, q: &Lookup) -> Vec<(String, String, Events)> {
    let layer = pg.valid_layers(q.layer());
    let edges = match q.object_name() {
        Some(o) => match layer.node(o.as_str()) {
            Some(node) => node.in_edges().into_iter().collect::<Vec<_>>(),
            None => Vec::new(),
        },
        None => layer.edges().into_iter().collect(),
    };
    edges
        .into_iter()
        .map(|e| {
            let mut events: Vec<_> = e.history().iter().map(|t| (t, true)).collect();
            events.extend(e.deletions().iter().map(|t| (t, false)));
            (e.src().name(), e.dst().name(), events)
        })
        .collect()
}

/// `Ver` natively: every validity run of every triple of a lookup, `(subject, object, from,
/// to)`, read from the history of the edges of its layer.
pub fn native_ver(pg: &PersistentGraph, q: &Lookup) -> Vec<(String, String, i64, Option<i64>)> {
    let mut rows = Vec::new();
    for (s, o, events) in histories(pg, q) {
        for (from, to) in runs(events) {
            rows.push((s.clone(), o.clone(), from, to));
        }
    }
    rows
}

/// `Diff` natively: the triples of a lookup that hold at `j` and not at `i` (added) and those
/// that hold at `i` and not at `j` (deleted), from the history of the edges of its layer.
pub fn native_diff(pg: &PersistentGraph, q: &Lookup, i: i64, j: i64) -> (Vec<Pair>, Vec<Pair>) {
    let (mut added, mut deleted) = (Vec::new(), Vec::new());
    for (s, o, events) in histories(pg, q) {
        let runs = runs(events);
        match (holds_at(&runs, i), holds_at(&runs, j)) {
            (false, true) => added.push((s, o)),
            (true, false) => deleted.push((s, o)),
            _ => {}
        }
    }
    (added, deleted)
}

// ---------------------------------------------------------------------------------------------
// Queries: the lookups, the joins and the forms of Mat, Diff and Ver
// ---------------------------------------------------------------------------------------------

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
    /// The lookup of a `?P?` or `?PO` query.
    pub lookup: Option<Lookup>,
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

    pub fn ver_from_named(&self, versions: &[i64]) -> String {
        let graphs: String = versions
            .iter()
            .map(|&t| format!("FROM NAMED {} ", asof(t)))
            .collect();
        format!("SELECT * {graphs}{{ GRAPH ?g {{ {} }} }}", self.pattern)
    }

    /// The interval form of Ver for a lookup: one row per validity run.
    pub fn ver_intervals(&self, versions: &[i64]) -> Option<String> {
        let lookup = self.lookup.as_ref()?;
        let graphs: Vec<String> = versions.iter().map(|&t| asof(t)).collect();
        let p = &lookup.predicate;
        let (o, projected) = match &lookup.object {
            Some(o) => (o.to_string(), "?s"),
            None => ("?o".to_owned(), "?s ?o"),
        };
        Some(format!(
            "SELECT DISTINCT {projected} ?from ?to {{
                VALUES ?g {{ {} }}
                GRAPH ?g {{ ?s {p} {o} }}
                BIND(raphtory:validFromTime(?s, {p}, {o}, ?g) AS ?from)
                BIND(raphtory:validToTime(?s, {p}, {o}, ?g) AS ?to)
            }}",
            graphs.join(" ")
        ))
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
        for (i, lookup) in Lookup::read_all(&dir.join(file))?.into_iter().enumerate() {
            queries.push(BearQuery {
                kind,
                number: i + 1,
                pattern: lookup.pattern.clone(),
                lookup: Some(lookup),
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
            lookup: None,
        });
    }
    Ok(queries)
}

/// The files of a zip archive: names and contents (decoded as UTF-8, lossily).
pub fn read_zip(path: &Path) -> Result<Vec<(String, String)>, String> {
    let file = fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if entry.is_dir() {
            continue;
        }
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        files.push((
            entry.name().to_owned(),
            String::from_utf8_lossy(&data).into_owned(),
        ));
    }
    Ok(files)
}
