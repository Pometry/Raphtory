//! The Gene Ontology (GO, <https://geneontology.org/docs/download-ontology/>) as versioned RDF:
//! reading releases of `go.owl`, skolemizing their blank nodes so that releases can be diffed,
//! loading them into a `PersistentGraph` and an oxigraph `Store`, and the queries to run.
//!
//! Shared by `raphtory-rdf-tests/tests/go.rs` and `rdf-bench/benches/go.rs` (both include this
//! file with `#[path]`), so it only uses `raphtory`, `oxigraph` and `std`.
//!
//! # The data
//!
//! The releases are those of `go.sha256`. `make go-fetch` downloads each from
//! `https://release.geneontology.org/<date>/ontology/go.owl` (OWL in RDF/XML) into
//! `$RAPHTORY_RDF_DATA/go/<date>/go.owl` ([`go_dir`]).
//!
//! # Skolemization
//!
//! OWL axioms are blank nodes (`owl:Axiom` annotations, `owl:Restriction`s, RDF lists), and a
//! parser labels blank nodes afresh on every read. [`skolemize`] replaces each with an IRI under
//! [`GENID`], a hash of
//!
//! - for an `owl:Axiom`: its `owl:annotatedSource`, `owl:annotatedProperty` and
//!   `owl:annotatedTarget`;
//! - for a node with one parent: the parent's IRI, the predicate linking them and the node's
//!   content (its predicates and objects, blank objects by their content);
//! - for any other node: its content.
//!
//! Nodes that would get the same IRI are numbered. An axiom or restriction that does not change
//! keeps its IRI, so the diff of two releases holds only what changed. `isBlank()` matches none
//! of these IRIs; queries exclude them with `STRSTARTS(STR(?x), GENID)`.
//!
//! # Versions
//!
//! Release `k` is time [`millis`]`(date_k)`. [`load_raphtory`] writes the first release at its
//! time and every later one as its changes, retracting the removed triples and asserting the
//! added ones at its time, so `pg.snapshot_at(millis(date_k))` and `<raphtory:asof:date_k>` are
//! release `k`. [`load_store`] puts release `k` in the named graph [`release_graph`]`(date_k)`.
#![allow(dead_code)]
use oxigraph::store::Store;
use raphtory::{
    errors::GraphError,
    rdf::{
        model::{NamedNode, NamedOrBlankNode, Term, Triple},
        RdfFormat, RdfMutationOps, RdfParser, ASOF_NS,
    },
};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    fs,
    hash::{Hash, Hasher},
    io::BufReader,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

/// The environment variable that points at the directory with the downloaded RDF data (the
/// `RDF_DATA_DIR` of the Makefile); GO is in its `go` subdirectory.
pub const DATA_ENV: &str = "RAPHTORY_RDF_DATA";

/// The releases and their SHA-256 checksums: `<sha256>  <date>/go.owl`.
pub const SUMS: &str = include_str!("go.sha256");

/// The namespace of the skolem IRIs.
pub const GENID: &str = "http://example.org/.well-known/genid/";

/// The namespace of the Store's release graphs.
pub const RELEASE_NS: &str = "http://example.org/go/release/";

const ANNOTATED_SOURCE: &str = "http://www.w3.org/2002/07/owl#annotatedSource";
const ANNOTATED_PROPERTY: &str = "http://www.w3.org/2002/07/owl#annotatedProperty";
const ANNOTATED_TARGET: &str = "http://www.w3.org/2002/07/owl#annotatedTarget";

/// The dates of the releases of a checksum file, oldest first.
pub fn release_dates(sums: &str) -> Vec<String> {
    let mut dates: Vec<String> = sums
        .lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter_map(|path| path.trim_start_matches("./").strip_suffix("/go.owl"))
        .map(str::to_owned)
        .collect();
    dates.sort_unstable();
    dates
}

/// The GO directory `$RAPHTORY_RDF_DATA/go` with every release of [`SUMS`], or why there is none.
pub fn go_dir() -> Result<PathBuf, String> {
    let Some(root) = std::env::var_os(DATA_ENV) else {
        return Err(format!(
            "{DATA_ENV} is not set; set it to the directory `make go-fetch` downloads to \
             (~/.cache/raphtory-rdf by default)"
        ));
    };
    let dir = PathBuf::from(root).join("go");
    let missing: Vec<String> = release_dates(SUMS)
        .into_iter()
        .filter(|date| !owl_file(&dir, date).is_file())
        .collect();
    if missing.is_empty() {
        Ok(dir)
    } else {
        Err(format!(
            "{} lacks the releases {missing:?} (`make go-fetch` downloads them)",
            dir.display()
        ))
    }
}

pub fn owl_file(dir: &Path, date: &str) -> PathBuf {
    dir.join(date).join("go.owl")
}

/// Checks the files of a checksum file under `dir` with `sha256sum` or `shasum`.
pub fn verify(dir: &Path, sums: &str) -> Result<(), String> {
    let sha256sum = Command::new("sha256sum")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    let (tool, args): (&str, &[&str]) = if sha256sum {
        ("sha256sum", &[])
    } else {
        ("shasum", &["-a", "256"])
    };
    let entries: Vec<(&str, &str)> = sums
        .lines()
        .filter_map(|line| line.split_once(char::is_whitespace))
        .map(|(sum, file)| (sum, file.trim().trim_start_matches("./")))
        .collect();
    std::thread::scope(|s| {
        let checks: Vec<_> = entries
            .iter()
            .map(|&(expected, file)| {
                s.spawn(move || {
                    let path = dir.join(file);
                    let output = Command::new(tool)
                        .args(args)
                        .arg(&path)
                        .output()
                        .map_err(|e| format!("cannot run {tool}: {e}"))?;
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    match stdout.split_whitespace().next() {
                        Some(sum) if output.status.success() && sum == expected => Ok(()),
                        Some(sum) if output.status.success() => Err(format!(
                            "{}: SHA-256 {sum}, expected {expected}",
                            path.display()
                        )),
                        _ => Err(format!(
                            "{}: {}",
                            path.display(),
                            String::from_utf8_lossy(&output.stderr).trim()
                        )),
                    }
                })
            })
            .collect();
        checks
            .into_iter()
            .try_for_each(|check| check.join().expect("checksum thread"))
    })
}

/// A `YYYY-MM-DD` date as epoch milliseconds (midnight UTC).
pub fn millis(date: &str) -> i64 {
    let field = |i: usize| -> i64 {
        date.split('-')
            .nth(i)
            .and_then(|f| f.parse().ok())
            .unwrap_or_else(|| panic!("not a YYYY-MM-DD date: {date}"))
    };
    let (m, d) = (field(1), field(2));
    let y = if m <= 2 { field(0) - 1 } else { field(0) };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era * 146_097 + day_of_era - 719_468) * 86_400_000
}

/// The Store's graph of a release, as a SPARQL IRI.
pub fn release_graph(date: &str) -> String {
    format!("<{RELEASE_NS}{date}>")
}

/// Raphtory's time graph of a release, as a SPARQL IRI.
pub fn asof_graph(date: &str) -> String {
    format!("<{ASOF_NS}{date}>")
}

/// A term with the graph IRIs of both engines written as `<release:date>`.
pub fn same_graphs(term: &str) -> String {
    term.replace(&format!("<{ASOF_NS}"), "<release:")
        .replace(&format!("<{RELEASE_NS}"), "<release:")
}

/// The triples of an RDF/XML file.
pub fn read_release(path: &Path) -> Result<Vec<Triple>, String> {
    let file = fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    RdfParser::from_format(RdfFormat::RdfXml)
        .for_reader(BufReader::new(file))
        .map(|quad| {
            quad.map(Triple::from)
                .map_err(|e| format!("{}: {e}", path.display()))
        })
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

pub fn is_genid(term: &Term) -> bool {
    matches!(term, Term::NamedNode(n) if n.as_str().starts_with(GENID))
}

pub fn hash128(write: impl Fn(&mut DefaultHasher)) -> u128 {
    let half = |seed: u8| {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        write(&mut hasher);
        hasher.finish() as u128
    };
    half(0) << 64 | half(1)
}

/// The object of a triple for hashing: a blank node by its content.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Value {
    Term(String),
    Blank(u128),
}

const NO_BLANK: u32 = u32::MAX;

/// Replaces every blank node with an IRI under [`GENID`] (see the [module docs](self)).
pub fn skolemize(triples: Vec<Triple>) -> Result<Vec<Triple>, String> {
    // the blank subject and object of every triple, numbered
    let mut subject_of = vec![NO_BLANK; triples.len()];
    let mut object_of = vec![NO_BLANK; triples.len()];
    let mut index = HashMap::new();
    for (i, t) in triples.iter().enumerate() {
        if let NamedOrBlankNode::BlankNode(b) = &t.subject {
            let next = index.len() as u32;
            subject_of[i] = *index.entry(b).or_insert(next);
        }
        if let Term::BlankNode(b) = &t.object {
            let next = index.len() as u32;
            object_of[i] = *index.entry(b).or_insert(next);
        }
    }
    let n = index.len();
    drop(index);
    let mut out = vec![Vec::new(); n];
    let mut parents = vec![Vec::new(); n];
    for i in 0..triples.len() {
        if subject_of[i] != NO_BLANK {
            out[subject_of[i] as usize].push(i);
        }
        if object_of[i] != NO_BLANK {
            parents[object_of[i] as usize].push(i);
        }
    }

    // every node after the nodes it links to
    let mut order = Vec::with_capacity(n);
    let mut state = vec![0u8; n];
    for root in 0..n {
        if state[root] != 0 {
            continue;
        }
        state[root] = 1;
        let mut stack = vec![(root, 0)];
        while let Some(&(b, next)) = stack.last() {
            let Some(&i) = out[b].get(next) else {
                state[b] = 2;
                order.push(b);
                stack.pop();
                continue;
            };
            stack.last_mut().expect("not empty").1 += 1;
            let child = object_of[i];
            if child == NO_BLANK {
                continue;
            }
            match state[child as usize] {
                0 => {
                    state[child as usize] = 1;
                    stack.push((child as usize, 0));
                }
                1 => return Err("the blank nodes have a cycle".to_owned()),
                _ => {}
            }
        }
    }

    let value = |i: usize, content: &[u128]| match object_of[i] {
        NO_BLANK => Value::Term(triples[i].object.to_string()),
        o => Value::Blank(content[o as usize]),
    };
    let mut content = vec![0u128; n];
    for &b in &order {
        let mut lines: Vec<(&str, Value)> = out[b]
            .iter()
            .map(|&i| (triples[i].predicate.as_str(), value(i, &content)))
            .collect();
        lines.sort_unstable();
        content[b] = hash128(|h| {
            "content".hash(h);
            lines.hash(h);
        });
    }

    let axiom = |b: usize| -> Option<u128> {
        let mut annotated: [Option<Value>; 3] = [None, None, None];
        for &i in &out[b] {
            let slot = match triples[i].predicate.as_str() {
                ANNOTATED_SOURCE => 0,
                ANNOTATED_PROPERTY => 1,
                ANNOTATED_TARGET => 2,
                _ => continue,
            };
            if annotated[slot].replace(value(i, &content)).is_some() {
                return None;
            }
        }
        let [Some(_), Some(_), Some(_)] = &annotated else {
            return None;
        };
        Some(hash128(|h| {
            "axiom".hash(h);
            annotated.hash(h);
        }))
    };
    let mut ids = vec![0u128; n];
    let mut roots: HashMap<u128, Vec<usize>> = HashMap::new();
    for b in (0..n).filter(|&b| parents[b].len() != 1) {
        let key = axiom(b).unwrap_or_else(|| {
            hash128(|h| {
                "root".hash(h);
                content[b].hash(h);
            })
        });
        roots.entry(key).or_default().push(b);
    }
    for (key, mut group) in roots {
        group.sort_unstable_by_key(|&b| content[b]);
        for (k, b) in group.into_iter().enumerate() {
            ids[b] = hash128(|h| (key, k).hash(h));
        }
    }
    let mut siblings: HashMap<u128, usize> = HashMap::new();
    for &b in order.iter().rev() {
        let &[i] = parents[b].as_slice() else {
            continue;
        };
        let parent = match (&triples[i].subject, subject_of[i]) {
            (NamedOrBlankNode::NamedNode(s), _) => hash128(|h| ("iri", s.as_str()).hash(h)),
            (_, p) => ids[p as usize],
        };
        let key = hash128(|h| ("child", parent, triples[i].predicate.as_str(), content[b]).hash(h));
        let k = siblings.entry(key).or_default();
        ids[b] = hash128(|h| (key, *k).hash(h));
        *k += 1;
    }

    let iri = |b: u32| NamedNode::new_unchecked(format!("{GENID}{:032x}", ids[b as usize]));
    Ok(triples
        .into_iter()
        .enumerate()
        .map(|(i, t)| Triple {
            subject: match subject_of[i] {
                NO_BLANK => t.subject,
                b => iri(b).into(),
            },
            predicate: t.predicate,
            object: match object_of[i] {
                NO_BLANK => t.object,
                b => iri(b).into(),
            },
        })
        .collect())
}

/// What changed from one release to the next, in document order.
#[derive(Clone, Debug, Default)]
pub struct Change {
    pub removed: Vec<Triple>,
    pub added: Vec<Triple>,
}

/// Skolemized releases: the first in full and the changes to every later one.
#[derive(Clone, Debug)]
pub struct Versions {
    pub dates: Vec<String>,
    /// The first release, in document order.
    pub first: Vec<Triple>,
    /// `changes[k]` turns release `k` into release `k + 1`.
    pub changes: Vec<Change>,
    /// The number of triples of each release.
    pub sizes: Vec<usize>,
    /// The last release, in document order.
    pub last: Vec<Triple>,
    /// The time spent reading and skolemizing the releases.
    pub read_time: Duration,
    /// The time spent diffing them.
    pub diff_time: Duration,
}

/// A skolemized release, each triple once, with the fingerprints of its triples.
struct Release {
    triples: Vec<Triple>,
    prints: Vec<u128>,
    set: HashSet<u128>,
}

impl Release {
    fn new(triples: Vec<Triple>) -> Self {
        let mut release = Self {
            triples: Vec::with_capacity(triples.len()),
            prints: Vec::with_capacity(triples.len()),
            set: HashSet::with_capacity(triples.len()),
        };
        for t in triples {
            let print = hash128(|h| t.hash(h));
            if release.set.insert(print) {
                release.prints.push(print);
                release.triples.push(t);
            }
        }
        release
    }

    /// The triples of `self` that `other` does not have.
    fn minus(&self, other: &Self) -> Vec<Triple> {
        self.triples
            .iter()
            .zip(&self.prints)
            .filter(|(_, p)| !other.set.contains(p))
            .map(|(t, _)| t.clone())
            .collect()
    }
}

/// Releases read at the same time.
const PARALLEL: usize = 4;

impl Versions {
    /// Reads, skolemizes and diffs the releases of `dates` under `dir`.
    pub fn read(dir: &Path, dates: &[String]) -> Result<Self, String> {
        let mut first = None;
        let mut prev: Option<Release> = None;
        let mut changes = Vec::new();
        let mut sizes = Vec::new();
        let (mut read_time, mut diff_time) = (Duration::ZERO, Duration::ZERO);
        for chunk in dates.chunks(PARALLEL) {
            let start = Instant::now();
            let read: Vec<Vec<Triple>> = std::thread::scope(|s| {
                let readers: Vec<_> = chunk
                    .iter()
                    .map(|date| s.spawn(move || skolemize(read_release(&owl_file(dir, date))?)))
                    .collect();
                readers
                    .into_iter()
                    .map(|r| r.join().expect("reader thread"))
                    .collect::<Result<_, String>>()
            })?;
            read_time += start.elapsed();
            let start = Instant::now();
            let releases: Vec<Release> = std::thread::scope(|s| {
                let printers: Vec<_> = read
                    .into_iter()
                    .map(|triples| s.spawn(move || Release::new(triples)))
                    .collect();
                printers
                    .into_iter()
                    .map(|r| r.join().expect("fingerprint thread"))
                    .collect()
            });
            for release in releases {
                sizes.push(release.triples.len());
                if let Some(prev) = &prev {
                    changes.push(Change {
                        removed: prev.minus(&release),
                        added: release.minus(prev),
                    });
                }
                if let Some(old) = prev.replace(release) {
                    first.get_or_insert(old.triples);
                }
            }
            diff_time += start.elapsed();
        }
        let last = prev.ok_or_else(|| "no releases".to_owned())?.triples;
        Ok(Self {
            dates: dates.to_vec(),
            first: first.unwrap_or_else(|| last.clone()),
            changes,
            sizes,
            last,
            read_time,
            diff_time,
        })
    }

    /// Calls `f(k, release k)` for every release, oldest first.
    pub fn for_each_release(
        &self,
        mut f: impl FnMut(usize, &HashSet<&Triple>) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut release: HashSet<&Triple> = self.first.iter().collect();
        f(0, &release)?;
        for (k, change) in self.changes.iter().enumerate() {
            for t in &change.removed {
                release.remove(t);
            }
            release.extend(&change.added);
            f(k + 1, &release)?;
        }
        Ok(())
    }

    /// The documents [`load_raphtory`] writes.
    pub fn docs(&self) -> Docs {
        Docs {
            first: ntriples(&self.first),
            changes: self
                .changes
                .iter()
                .map(|c| (ntriples(&c.removed), ntriples(&c.added)))
                .collect(),
        }
    }

    /// The number of triples written by [`load_raphtory`].
    pub fn events(&self) -> usize {
        self.first.len()
            + self
                .changes
                .iter()
                .map(|c| c.removed.len() + c.added.len())
                .sum::<usize>()
    }
}

/// The first release and the changes as N-Triples documents: `(removed, added)`.
pub struct Docs {
    pub first: Vec<u8>,
    pub changes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Writes the versions into `g` (see the [module docs](self)) and returns the number of triples
/// written.
pub fn load_raphtory<G: RdfMutationOps>(
    g: &G,
    dates: &[String],
    docs: &Docs,
) -> Result<usize, GraphError> {
    let mut written = g.load_rdf(
        millis(&dates[0]),
        docs.first.as_slice(),
        RdfFormat::NTriples,
        None,
    )?;
    for (date, (removed, added)) in dates[1..].iter().zip(&docs.changes) {
        let t = millis(date);
        written += g.retract_rdf(t, removed.as_slice(), RdfFormat::NTriples, None)?;
        written += g.load_rdf(t, added.as_slice(), RdfFormat::NTriples, None)?;
    }
    Ok(written)
}

/// Loads an N-Triples document into the graph of a release.
pub fn load_store_release(store: &Store, date: &str, doc: &[u8]) -> Result<(), String> {
    let graph = NamedNode::new(format!("{RELEASE_NS}{date}")).map_err(|e| e.to_string())?;
    let mut loader = store.bulk_loader();
    loader
        .load_from_slice(
            RdfParser::from_format(RdfFormat::NTriples).with_default_graph(graph),
            doc,
        )
        .map_err(|e| e.to_string())?;
    loader.commit().map_err(|e| e.to_string())
}

/// Every release in its graph.
pub fn load_store(store: &Store, versions: &Versions) -> Result<(), String> {
    versions.for_each_release(|k, release| {
        let doc = ntriples(release.iter().copied());
        load_store_release(store, &versions.dates[k], &doc)
    })
}

// ---------------------------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------------------------

pub const PREFIXES: &str = "PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX oio: <http://www.geneontology.org/formats/oboInOwl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX go: <http://purl.obolibrary.org/obo/go#>
";

/// A query in the form for each engine.
#[derive(Clone, Debug)]
pub struct GoQuery {
    pub name: String,
    pub what: &'static str,
    pub raphtory: String,
    pub store: String,
    /// It has an `ORDER BY` (always a total order).
    pub ordered: bool,
}

impl GoQuery {
    fn new(name: String, what: &'static str, raphtory: &str, store: &str) -> Self {
        let full = |q: &str| format!("{PREFIXES}{}", q.replace("{GENID}", GENID));
        Self {
            name,
            what,
            ordered: raphtory.contains("ORDER BY"),
            raphtory: full(raphtory),
            store: full(store),
        }
    }
}

/// Queries on one release: `{FROM}` is empty, or `FROM <the release>` for [`queries_asof`].
/// GO_0005739 is mitochondrion, GO_0008150 biological_process, GO_0006915 apoptotic process,
/// BFO_0000050 part of and RO_0002211 regulates.
const SINGLE: &[(&str, &str, &str)] = &[
    (
        "S01",
        "label of a term",
        "SELECT ?label {FROM} WHERE { obo:GO_0005739 rdfs:label ?label }",
    ),
    (
        "S02",
        "term by label",
        "SELECT ?c {FROM} WHERE { ?c rdfs:label \"mitochondrion\" }",
    ),
    (
        "S03",
        "term by OBO id",
        "SELECT ?c {FROM} WHERE { ?c oio:id \"GO:0005739\" }",
    ),
    (
        "S04",
        "literal annotations of a term",
        "SELECT ?p ?o {FROM} WHERE { obo:GO_0005739 ?p ?o FILTER(isLiteral(?o)) }",
    ),
    (
        "S05",
        "definition and its references",
        "SELECT ?def ?xref {FROM} WHERE {
            obo:GO_0005739 obo:IAO_0000115 ?def .
            ?ax owl:annotatedSource obo:GO_0005739 ; owl:annotatedProperty obo:IAO_0000115 ;
                owl:annotatedTarget ?def ; oio:hasDbXref ?xref
        }",
    ),
    (
        "S06",
        "synonym text search",
        "SELECT ?c ?syn {FROM} WHERE {
            ?c oio:hasExactSynonym ?syn FILTER(CONTAINS(LCASE(STR(?syn)), \"mitochondri\"))
        }",
    ),
    (
        "S07",
        "label prefix search",
        "SELECT ?c ?l {FROM} WHERE { ?c rdfs:label ?l FILTER(STRSTARTS(STR(?l), \"mitochondrial\")) }",
    ),
    (
        "S08",
        "definition text search",
        "SELECT ?c {FROM} WHERE { ?c obo:IAO_0000115 ?d FILTER(CONTAINS(STR(?d), \"apoptotic\")) }",
    ),
    (
        "S09",
        "is_a ancestors with labels",
        "SELECT DISTINCT ?anc ?label {FROM} WHERE {
            obo:GO_0005739 rdfs:subClassOf+ ?anc . ?anc rdfs:label ?label
        }",
    ),
    (
        "S10",
        "is_a descendants of biological_process",
        "SELECT (COUNT(DISTINCT ?d) AS ?n) {FROM} WHERE { ?d rdfs:subClassOf+ obo:GO_0008150 }",
    ),
    (
        "S11",
        "is_a descendants of apoptotic process",
        "SELECT (COUNT(DISTINCT ?d) AS ?n) {FROM} WHERE { ?d rdfs:subClassOf+ obo:GO_0006915 }",
    ),
    (
        "S12",
        "direct part_of children",
        "SELECT ?part ?l {FROM} WHERE {
            ?part rdfs:subClassOf ?r .
            ?r owl:onProperty obo:BFO_0000050 ; owl:someValuesFrom obo:GO_0005739 .
            ?part rdfs:label ?l
        }",
    ),
    (
        "S13",
        "ancestors over is_a and restrictions",
        "SELECT (COUNT(DISTINCT ?anc) AS ?n) {FROM} WHERE {
            obo:GO_0005739 (rdfs:subClassOf|(rdfs:subClassOf/owl:someValuesFrom))+ ?anc
            FILTER(!STRSTARTS(STR(?anc), \"{GENID}\"))
        }",
    ),
    (
        "S14",
        "descendants over is_a and restrictions",
        "SELECT (COUNT(DISTINCT ?d) AS ?n) {FROM} WHERE {
            ?d (rdfs:subClassOf|(rdfs:subClassOf/owl:someValuesFrom))+ obo:GO_0005739
        }",
    ),
    (
        "S15",
        "restrictions per property",
        "SELECT ?prop (COUNT(*) AS ?n) {FROM} WHERE {
            ?c rdfs:subClassOf ?r . ?r owl:onProperty ?prop ; owl:someValuesFrom ?t
        } GROUP BY ?prop ORDER BY DESC(?n) ?prop",
    ),
    (
        "S16",
        "regulators of apoptosis",
        "SELECT (COUNT(DISTINCT ?reg) AS ?n) {FROM} WHERE {
            ?reg rdfs:subClassOf ?r . ?r owl:onProperty obo:RO_0002211 ; owl:someValuesFrom ?t .
            ?t rdfs:subClassOf* obo:GO_0006915
        }",
    ),
    (
        "S17",
        "genus of logical definitions",
        "SELECT (COUNT(*) AS ?n) {FROM} WHERE {
            ?c owl:equivalentClass/owl:intersectionOf/rdf:first ?genus
        }",
    ),
    (
        "S18",
        "obsolete terms",
        "SELECT (COUNT(?c) AS ?n) {FROM} WHERE { ?c owl:deprecated true }",
    ),
    (
        "S19",
        "obsolete terms with a replacement",
        "SELECT (COUNT(*) AS ?n) {FROM} WHERE { ?c owl:deprecated true ; obo:IAO_0100001 ?r }",
    ),
    (
        "S20",
        "terms per namespace",
        "SELECT ?ns (COUNT(?c) AS ?n) {FROM} WHERE { ?c oio:hasOBONamespace ?ns }
         GROUP BY ?ns ORDER BY DESC(?n) ?ns",
    ),
    (
        "S21",
        "cross-reference prefixes",
        "SELECT ?prefix (COUNT(*) AS ?n) {FROM} WHERE {
            ?c a owl:Class ; oio:hasDbXref ?x . BIND(STRBEFORE(STR(?x), \":\") AS ?prefix)
        } GROUP BY ?prefix ORDER BY DESC(?n) ?prefix LIMIT 20",
    ),
    (
        "S22",
        "classes with the most is_a children",
        "SELECT ?p (COUNT(?c) AS ?n) {FROM} WHERE {
            ?c rdfs:subClassOf ?p . ?p a owl:Class FILTER(!STRSTARTS(STR(?p), \"{GENID}\"))
        } GROUP BY ?p ORDER BY DESC(?n) ?p LIMIT 10",
    ),
    (
        "S23",
        "terms of a GO slim",
        "SELECT ?c ?l {FROM} WHERE { ?c oio:inSubset go:goslim_generic ; rdfs:label ?l }",
    ),
    (
        "S24",
        "all triples",
        "SELECT (COUNT(*) AS ?n) {FROM} WHERE { ?s ?p ?o }",
    ),
    (
        "S25",
        "triples per predicate",
        "SELECT ?p (COUNT(*) AS ?n) {FROM} WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY DESC(?n) ?p",
    ),
    ("S26", "describe a term", "DESCRIBE obo:GO_0005739 {FROM}"),
];

/// The queries on one release, in the default graph of both engines.
pub fn queries() -> Vec<GoQuery> {
    SINGLE
        .iter()
        .map(|&(name, what, q)| {
            let q = q.replace("{FROM}", "");
            GoQuery::new(name.to_owned(), what, &q, &q)
        })
        .collect()
}

/// The queries on one release, as of the release of `date`.
pub fn queries_asof(date: &str) -> Vec<GoQuery> {
    SINGLE
        .iter()
        .map(|&(name, what, q)| {
            let from = |graph: String| q.replace("{FROM}", &format!("FROM {graph}"));
            GoQuery::new(
                format!("{name}@{date}"),
                what,
                &from(asof_graph(date)),
                &from(release_graph(date)),
            )
        })
        .collect()
}

/// When obsolete terms became obsolete, from the validity of the triple.
const SINCE_RAPHTORY: &str = "SELECT ?c ?since WHERE {
    ?c owl:deprecated true BIND(raphtory:validFrom(?c, owl:deprecated, true) AS ?since)
}";

/// The same over the release graphs: the first release from which the triple is in every
/// later release.
const SINCE_STORE: &str = "SELECT ?c (MIN(?d) AS ?since) WHERE {
    GRAPH {G:last} { ?c owl:deprecated true }
    VALUES (?g ?d) { {DATED} }
    GRAPH ?g { ?c owl:deprecated true }
    FILTER NOT EXISTS {
        VALUES (?g2 ?d2) { {DATED} }
        FILTER(?d2 > ?d)
        FILTER NOT EXISTS { GRAPH ?g2 { ?c owl:deprecated true } }
    }
} GROUP BY ?c";

/// Queries across releases: `{NAMED}` names every release, `{PAIRS}` lists consecutive releases,
/// `{DATED}` pairs every release with its date and `{G:first}`, `{G:mid}`, `{G:prev}` and
/// `{G:last}` are single releases. The last element is the Store's form when it differs.
const VERSIONED: &[(&str, &str, &str, Option<&str>)] = &[
    (
        "V01",
        "a label in every release",
        "SELECT ?g ?label {NAMED} WHERE { GRAPH ?g { obo:GO_0005739 rdfs:label ?label } }",
        None,
    ),
    (
        "V02",
        "classes per release",
        "SELECT ?g (COUNT(?c) AS ?n) {NAMED} WHERE {
            GRAPH ?g { ?c a owl:Class } FILTER(!STRSTARTS(STR(?c), \"{GENID}\"))
        } GROUP BY ?g",
        None,
    ),
    (
        "V03",
        "new terms in the last release",
        "SELECT ?c ?l WHERE {
            GRAPH {G:last} { ?c a owl:Class ; rdfs:label ?l }
            FILTER NOT EXISTS { GRAPH {G:prev} { ?c a owl:Class } }
        }",
        None,
    ),
    (
        "V04",
        "terms obsoleted in the last release",
        "SELECT ?c WHERE {
            GRAPH {G:last} { ?c owl:deprecated true }
            FILTER NOT EXISTS { GRAPH {G:prev} { ?c owl:deprecated true } }
        }",
        None,
    ),
    (
        "V05",
        "term labels changed since the first release",
        "SELECT ?c ?l1 ?l2 WHERE {
            GRAPH {G:first} { ?c rdfs:label ?l1 } GRAPH {G:last} { ?c rdfs:label ?l2 }
            FILTER(?l1 != ?l2 && !STRSTARTS(STR(?c), \"{GENID}\"))
        }",
        None,
    ),
    (
        "V06",
        "new is_a links in the last release",
        "SELECT ?c ?p WHERE {
            GRAPH {G:last} { ?c rdfs:subClassOf ?p }
            FILTER NOT EXISTS { GRAPH {G:prev} { ?c rdfs:subClassOf ?p } }
            FILTER(!STRSTARTS(STR(?p), \"{GENID}\"))
        }",
        None,
    ),
    (
        "V07",
        "a term's triples in every release",
        "SELECT ?g ?p ?o {NAMED} WHERE { GRAPH ?g { obo:GO_0005739 ?p ?o } }",
        None,
    ),
    (
        "V08",
        "since when obsolete terms are obsolete",
        SINCE_RAPHTORY,
        Some(SINCE_STORE),
    ),
    (
        "V09",
        "term labels added per release",
        "SELECT ?g2 (COUNT(*) AS ?n) {NAMED} WHERE {
            VALUES (?g1 ?g2) { {PAIRS} }
            GRAPH ?g2 { ?c rdfs:label ?l } FILTER NOT EXISTS { GRAPH ?g1 { ?c rdfs:label ?l } }
            FILTER(!STRSTARTS(STR(?c), \"{GENID}\"))
        } GROUP BY ?g2",
        None,
    ),
    (
        "V10",
        "terms per namespace as of the middle release",
        "SELECT ?ns (COUNT(?c) AS ?n) FROM {G:mid} WHERE { ?c oio:hasOBONamespace ?ns } GROUP BY ?ns",
        None,
    ),
    (
        "V11",
        "is_a ancestors as of the first release",
        "SELECT DISTINCT ?anc ?l WHERE {
            GRAPH {G:first} { obo:GO_0005739 rdfs:subClassOf+ ?anc . ?anc rdfs:label ?l }
        }",
        None,
    ),
    (
        "V12",
        "is_a descendants of biological_process as of the first release",
        "SELECT (COUNT(DISTINCT ?d) AS ?n) FROM {G:first} WHERE { ?d rdfs:subClassOf+ obo:GO_0008150 }",
        None,
    ),
    (
        "V13",
        "terms obsoleted per release",
        "SELECT ?since (COUNT(*) AS ?n) WHERE { {SINCE} } GROUP BY ?since",
        None,
    ),
    (
        "V14",
        "triples per release",
        "SELECT ?g (COUNT(*) AS ?n) {NAMED} WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
        None,
    ),
];

/// The queries across the releases of `dates`.
pub fn versioned_queries(dates: &[String]) -> Vec<GoQuery> {
    let fill = |template: &str, graph: fn(&str) -> String, since: &str| -> String {
        let all = |f: &dyn Fn(&String) -> String| dates.iter().map(f).collect::<String>();
        let named = all(&|d| format!("FROM NAMED {} ", graph(d)));
        let dated = all(&|d| format!("({} \"{d}T00:00:00Z\"^^xsd:dateTime) ", graph(d)));
        let pairs: String = dates
            .windows(2)
            .map(|w| format!("({} {}) ", graph(&w[0]), graph(&w[1])))
            .collect();
        let at = |k: usize| graph(&dates[k]);
        template
            .replace("{SINCE}", &format!("{{ {since} }}"))
            .replace("{NAMED}", &named)
            .replace("{DATED}", &dated)
            .replace("{PAIRS}", &pairs)
            .replace("{G:first}", &at(0))
            .replace("{G:mid}", &at(dates.len() / 2))
            .replace("{G:prev}", &at(dates.len().saturating_sub(2)))
            .replace("{G:last}", &at(dates.len() - 1))
    };
    VERSIONED
        .iter()
        .map(|&(name, what, raphtory, store)| {
            GoQuery::new(
                name.to_owned(),
                what,
                &fill(raphtory, asof_graph, SINCE_RAPHTORY),
                &fill(store.unwrap_or(raphtory), release_graph, SINCE_STORE),
            )
        })
        .collect()
}
