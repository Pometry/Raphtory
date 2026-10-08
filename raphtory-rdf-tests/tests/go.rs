//! The Gene Ontology (GO) releases, skolemized (see the [`go`] module), in a `PersistentGraph` and
//! an oxigraph `Store`: every query must give the same results on both.
//!
//! - [`go_current_release`]: the last release, its sizes and [`go::queries`].
//! - [`go_versions`]: every release as versions of one graph against a named graph per release:
//!   the pinned sizes ([`PINNED`]), the triples as of each release, [`go::versioned_queries`] and
//!   [`go::queries_asof`] as of four releases (`RAPHTORY_GO_FULL=1`: every release).
//! - [`go_skolem_iris_are_stable`]: unchanged axioms and restrictions keep their IRIs.
//!
//! Rows are compared as multisets, in sequence under `ORDER BY`; `DESCRIBE` as sets of triples.
//!
//! `make rust-test-rdf-go` downloads the data and runs this file. Without the data the GO tests
//! print a message and pass; the self-tests run on a small generated ontology.
#[path = "common/go.rs"]
mod go;

use go::{
    go_dir, hash128, is_genid, load_raphtory, load_store, millis, ntriples, owl_file, queries,
    queries_asof, read_release, release_dates, skolemize, verify, versioned_queries, GoQuery,
    Versions, GENID, RELEASE_NS, SUMS,
};
use oxigraph::{model::NamedNodeRef, sparql::SparqlEvaluator, store::Store};
use raphtory::{
    prelude::*,
    rdf::{
        model::{NamedOrBlankNode, Term, Triple},
        term_of, RdfFormat, RdfViewOps, SparqlResults,
    },
};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};

/// Every release: (date, triples, removed since the previous release, added).
const PINNED: &[(&str, usize, usize, usize)] = &[
    ("2024-06-17", 1_424_278, 0, 0),
    ("2024-09-08", 1_420_891, 23_834, 20_447),
    ("2024-11-03", 1_420_821, 8_622, 8_552),
    ("2025-02-06", 1_423_478, 13_074, 15_731),
    ("2025-03-16", 1_424_137, 7_427, 8_086),
    ("2025-06-01", 1_425_122, 5_079, 6_064),
    ("2025-07-22", 1_426_925, 4_930, 6_733),
    ("2025-10-10", 1_425_391, 11_482, 9_948),
    ("2026-01-23", 1_436_273, 19_369, 30_251),
    ("2026-03-25", 1_444_037, 9_515, 17_279),
    ("2026-05-19", 1_445_417, 7_054, 8_434),
    ("2026-06-19", 1_444_892, 1_894, 1_369),
    ("2026-08-05", 1_445_043, 2_997, 3_148),
];

/// Runs the queries of one release as of every release when set to anything but `0`; by
/// default only as of the first, middle and last two.
const FULL_ENV: &str = "RAPHTORY_GO_FULL";

fn is_full() -> bool {
    std::env::var(FULL_ENV).is_ok_and(|v| !v.is_empty() && v != "0")
}

/// The stack of the threads that run queries.
const STACK: usize = 64 << 20;

static DATA: Mutex<()> = Mutex::new(());

static VERSIONS: OnceLock<Result<Versions, String>> = OnceLock::new();

/// The data directory and the versions, read once, holding the lock that runs the data tests one
/// at a time.
fn data(what: &str) -> Option<(MutexGuard<'static, ()>, PathBuf, &'static Versions)> {
    let dir = match go_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping GO {what}: {why}");
            return None;
        }
    };
    let lock = DATA.lock().unwrap_or_else(|e| e.into_inner());
    let versions = VERSIONS.get_or_init(|| {
        let start = Instant::now();
        verify(&dir, SUMS)?;
        println!("GO: checksums verified in {:.1} s", secs(start));
        let start = Instant::now();
        let versions = Versions::read(&dir, &release_dates(SUMS))?;
        println!(
            "GO: {} releases read, skolemized and diffed in {:.1} s (diffing {:.1} s)",
            versions.dates.len(),
            secs(start),
            versions.diff_time.as_secs_f64()
        );
        Ok(versions)
    });
    let versions = versions
        .as_ref()
        .unwrap_or_else(|e| panic!("GO {what}: {e}"));
    Some((lock, dir, versions))
}

#[test]
fn go_current_release() {
    let Some((_lock, _, versions)) = data("current release") else {
        return;
    };
    finish(in_pool(|| check_current(versions)));
}

#[test]
fn go_versions() {
    let Some((_lock, _, versions)) = data("versions") else {
        return;
    };
    finish(in_pool(|| {
        check_versions(versions, Some(PINNED), is_full())
    }));
}

#[test]
fn go_skolem_iris_are_stable() {
    let Some((_lock, dir, versions)) = data("skolemization") else {
        return;
    };
    finish(in_pool(|| check_skolem(&dir, versions)));
}

fn secs(start: Instant) -> f64 {
    start.elapsed().as_secs_f64()
}

fn in_pool(f: impl FnOnce() -> Result<Report, String> + Send) -> Result<Report, String> {
    rayon::ThreadPoolBuilder::new()
        .stack_size(STACK)
        .build()
        .map_err(|e| e.to_string())?
        .install(f)
}

fn finish(report: Result<Report, String>) {
    let report = report.unwrap_or_else(|e| panic!("GO: {e}"));
    println!("{report}");
    assert!(
        report.problems.is_empty(),
        "{} problem(s) in GO {}:\n{}",
        report.problems.len(),
        report.name,
        report.problems.join("\n")
    );
}

// ---------------------------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Report {
    name: String,
    lines: Vec<String>,
    queries: usize,
    problems: Vec<String>,
    timings: Vec<(&'static str, Duration)>,
    /// The slowest queries: (seconds on both engines, name).
    slowest: Vec<(f64, String)>,
}

impl Report {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    fn line(&mut self, line: String) {
        self.lines.push(line);
    }

    fn expect(&mut self, what: impl fmt::Display, actual: usize, expected: usize) {
        if actual != expected {
            self.problems
                .push(format!("{what}: {actual}, expected {expected}"));
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timings: Vec<String> = self
            .timings
            .iter()
            .map(|(what, time)| format!("{what} {:.1} s", time.as_secs_f64()))
            .collect();
        writeln!(
            f,
            "GO {}: {} queries, {} problems ({})",
            self.name,
            self.queries,
            self.problems.len(),
            timings.join(", ")
        )?;
        for line in &self.lines {
            writeln!(f, "  {line}")?;
        }
        for (seconds, name) in &self.slowest {
            writeln!(f, "  slow: {seconds:.1} s {name}")?;
        }
        for problem in self.problems.iter().take(20) {
            writeln!(f, "  PROBLEM: {problem}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------------------------

/// Query results in a form that can be compared: values as N-Triples terms (`UNDEF` where
/// unbound), graph IRIs as `<release:date>`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Normal {
    /// Variable names sorted, rows in result order.
    Rows {
        variables: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Boolean(bool),
    /// Triples, sorted.
    Graph(Vec<String>),
}

impl Normal {
    fn of(results: SparqlResults) -> Self {
        let value = |term: &Option<Term>| match term {
            None => "UNDEF".to_owned(),
            Some(term) => go::same_graphs(&term.to_string()),
        };
        match results {
            SparqlResults::Solutions { variables, rows } => {
                let mut order: Vec<usize> = (0..variables.len()).collect();
                order.sort_by_key(|&i| variables[i].as_str());
                Normal::Rows {
                    variables: order
                        .iter()
                        .map(|&i| variables[i].as_str().to_owned())
                        .collect(),
                    rows: rows
                        .iter()
                        .map(|row| order.iter().map(|&i| value(&row[i])).collect())
                        .collect(),
                }
            }
            SparqlResults::Boolean(b) => Normal::Boolean(b),
            SparqlResults::Graph(triples) => {
                let mut triples: Vec<String> = triples.iter().map(Triple::to_string).collect();
                triples.sort_unstable();
                Normal::Graph(triples)
            }
        }
    }
}

/// The first items, for messages.
fn sample(items: &[String]) -> String {
    let shown: String = items
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ")
        .chars()
        .take(500)
        .collect();
    if items.len() > 3 {
        format!("[{shown} | ... {} more]", items.len() - 3)
    } else {
        format!("[{shown}]")
    }
}

/// The rows of `x` that are not in `y`, as multisets.
fn only(x: &[Vec<String>], y: &[Vec<String>]) -> String {
    let mut left: HashMap<&Vec<String>, usize> = HashMap::new();
    for row in y {
        *left.entry(row).or_default() += 1;
    }
    let rows: Vec<String> = x
        .iter()
        .filter(|row| match left.get_mut(row) {
            Some(n) if *n > 0 => {
                *n -= 1;
                false
            }
            _ => true,
        })
        .map(|row| row.join(" "))
        .collect();
    sample(&rows)
}

fn compare(ours: Normal, theirs: Normal, ordered: bool) -> Result<(), String> {
    match (ours, theirs) {
        (
            Normal::Rows {
                variables: va,
                rows: mut a,
            },
            Normal::Rows {
                variables: vb,
                rows: mut b,
            },
        ) => {
            if va != vb {
                return Err(format!("variables {va:?}, the Store's {vb:?}"));
            }
            if ordered && a == b {
                return Ok(());
            }
            a.sort_unstable();
            b.sort_unstable();
            if a == b {
                return if ordered {
                    Err("the same rows in another sequence".to_owned())
                } else {
                    Ok(())
                };
            }
            Err(format!(
                "{} rows, the Store {}; only Raphtory: {}; only the Store: {}",
                a.len(),
                b.len(),
                only(&a, &b),
                only(&b, &a)
            ))
        }
        (a, b) if a == b => Ok(()),
        (Normal::Graph(a), Normal::Graph(b)) => {
            let only = |x: &[String], y: &[String]| {
                let y: HashSet<&String> = y.iter().collect();
                let x: Vec<String> = x.iter().filter(|t| !y.contains(t)).cloned().collect();
                sample(&x)
            };
            Err(format!(
                "{} triples, the Store {}; only Raphtory: {}; only the Store: {}",
                a.len(),
                b.len(),
                only(&a, &b),
                only(&b, &a)
            ))
        }
        (a, b) => Err(format!("{a:?}, the Store {b:?}")),
    }
}

/// Runs a query on Raphtory and on the Store and compares the results.
fn check(pg: &PersistentGraph, store: &Store, q: &GoQuery) -> Result<(), String> {
    let ours = pg
        .sparql(&q.raphtory)
        .map(Normal::of)
        .map_err(|e| format!("Raphtory: {e}"))?;
    let results = SparqlEvaluator::new()
        .parse_query(&q.store)
        .map_err(|e| format!("the Store: {e}"))?
        .on_store(store)
        .execute()
        .map_err(|e| format!("the Store: {e}"))?;
    let theirs = SparqlResults::from_query_results(results)
        .map(Normal::of)
        .map_err(|e| format!("the Store: {e}"))?;
    compare(ours, theirs, q.ordered)
}

/// Checks queries in parallel.
fn check_all(report: &mut Report, pg: &PersistentGraph, store: &Store, queries: &[GoQuery]) {
    let start = Instant::now();
    let outcomes: Vec<(Result<(), String>, f64)> = queries
        .par_iter()
        .map(|q| {
            let start = Instant::now();
            (check(pg, store, q), secs(start))
        })
        .collect();
    for (q, (outcome, seconds)) in queries.iter().zip(outcomes) {
        report.queries += 1;
        if seconds >= 1.0 {
            report.slowest.push((seconds, q.name.clone()));
            report.slowest.sort_by(|a, b| b.0.total_cmp(&a.0));
            report.slowest.truncate(5);
        }
        if let Err(e) = outcome {
            report
                .problems
                .push(format!("{} ({}): {e}", q.name, q.what));
        }
    }
    report.timings.push(("queries", start.elapsed()));
}

// ---------------------------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------------------------

fn check_current(versions: &Versions) -> Result<Report, String> {
    let date = versions.dates.last().ok_or("no releases")?;
    let mut report = Report::new(format!("current release {date}"));
    let start = Instant::now();
    let doc = ntriples(&versions.last);
    let pg = PersistentGraph::new();
    pg.load_rdf(millis(date), doc.as_slice(), RdfFormat::NTriples, None)
        .map_err(|e| format!("load_rdf: {e}"))?;
    let store = Store::new().map_err(|e| e.to_string())?;
    let mut loader = store.bulk_loader();
    loader
        .load_from_slice(RdfFormat::NTriples, doc.as_slice())
        .map_err(|e| e.to_string())?;
    loader.commit().map_err(|e| e.to_string())?;
    report.timings.push(("load", start.elapsed()));

    let triples = versions.last.len();
    let nodes: HashSet<String> = versions
        .last
        .iter()
        .flat_map(|t| [t.subject.to_string(), t.object.to_string()])
        .collect();
    let layers: HashSet<&str> = versions.last.iter().map(|t| t.predicate.as_str()).collect();
    report.line(format!(
        "{triples} triples, {} nodes, {} layers",
        nodes.len(),
        layers.len()
    ));
    let visible = pg.valid().edges().explode_layers().iter().count();
    report.expect("triples in Raphtory", visible, triples);
    report.expect(
        "quads in the Store",
        store.len().map_err(|e| e.to_string())?,
        triples,
    );
    report.expect("nodes", pg.count_nodes(), nodes.len());
    report.expect("layers", pg.unique_layers().count(), layers.len());
    check_all(&mut report, &pg, &store, &queries());
    Ok(report)
}

/// The triples visible as of `t`, as N-Triples.
fn triples_at(pg: &PersistentGraph, t: i64) -> Result<HashSet<String>, String> {
    pg.snapshot_at(t)
        .valid()
        .edges()
        .explode_layers()
        .iter()
        .map(|e| {
            let layer = e.layer_name().map_err(|e| e.to_string())?;
            Ok(format!(
                "{} <{layer}> {}",
                term_of(&e.src().name()),
                term_of(&e.dst().name())
            ))
        })
        .collect()
}

fn store_graph_len(store: &Store, date: &str) -> usize {
    let graph = format!("{RELEASE_NS}{date}");
    store
        .quads_for_pattern(
            None,
            None,
            None,
            Some(NamedNodeRef::new_unchecked(&graph).into()),
        )
        .count()
}

fn check_versions(
    versions: &Versions,
    pinned: Option<&[(&str, usize, usize, usize)]>,
    full: bool,
) -> Result<Report, String> {
    let dates = &versions.dates;
    let last = dates.len() - 1;
    let mut report = Report::new(format!("versions, {} releases", dates.len()));
    let changes: Vec<(usize, usize)> = std::iter::once((0, 0))
        .chain(
            versions
                .changes
                .iter()
                .map(|c| (c.removed.len(), c.added.len())),
        )
        .collect();
    let found: Vec<(&str, usize, usize, usize)> = dates
        .iter()
        .zip(&versions.sizes)
        .zip(&changes)
        .map(|((d, &size), &(removed, added))| (d.as_str(), size, removed, added))
        .collect();
    for (date, size, removed, added) in &found {
        report.line(format!("{date}: {size} triples, -{removed} +{added}"));
    }
    if let Some(pinned) = pinned {
        if found != pinned {
            report.problems.push(format!(
                "the releases and their changes are not the pinned ones: {found:?}"
            ));
        }
    }

    let start = Instant::now();
    let pg = PersistentGraph::new();
    let written = load_raphtory(&pg, dates, &versions.docs()).map_err(|e| e.to_string())?;
    report.timings.push(("Raphtory load", start.elapsed()));
    report.expect("triples written", written, versions.events());
    let start = Instant::now();
    let store = Store::new().map_err(|e| e.to_string())?;
    load_store(&store, versions)?;
    report.timings.push(("Store load", start.elapsed()));

    let start = Instant::now();
    let sizes: Vec<(usize, usize)> = dates
        .par_iter()
        .map(|d| {
            let ours = pg
                .snapshot_at(millis(d))
                .valid()
                .edges()
                .explode_layers()
                .iter()
                .count();
            (ours, store_graph_len(&store, d))
        })
        .collect();
    for ((date, &expected), (ours, theirs)) in dates.iter().zip(&versions.sizes).zip(sizes) {
        report.expect(format!("triples as of {date}"), ours, expected);
        report.expect(format!("quads in the graph of {date}"), theirs, expected);
    }
    let sampled = [0, dates.len() / 2, last];
    versions.for_each_release(|k, release| {
        if !sampled.contains(&k) {
            return Ok(());
        }
        let ours = triples_at(&pg, millis(&dates[k]))?;
        let expected: HashSet<String> = release.iter().map(|t| t.to_string()).collect();
        if ours != expected {
            let only = |x: &HashSet<String>, y: &HashSet<String>| {
                sample(&x.difference(y).cloned().collect::<Vec<_>>())
            };
            report.problems.push(format!(
                "the triples as of {}: only Raphtory {}, only the release {}",
                dates[k],
                only(&ours, &expected),
                only(&expected, &ours)
            ));
        }
        Ok(())
    })?;
    report.timings.push(("sizes", start.elapsed()));

    let mut all = versioned_queries(dates);
    let mut asof: Vec<usize> = if full {
        (0..dates.len()).collect()
    } else {
        vec![0, dates.len() / 2, last.saturating_sub(1), last]
    };
    asof.dedup();
    for &k in &asof {
        all.extend(queries_asof(&dates[k]));
    }
    report.line(format!(
        "the queries of one release as of {} releases ({FULL_ENV}=1: all)",
        asof.len()
    ));
    check_all(&mut report, &pg, &store, &all);
    Ok(report)
}

/// The trees of skolem IRIs of a release that no other skolem IRI links to, by context (the
/// annotated triple of an axiom, or the named parent and predicate) and content, with a hash of
/// their exact triples.
struct Trees {
    trees: HashMap<(u128, u128), Vec<u128>>,
    iris: HashSet<String>,
}

impl Trees {
    fn of(release: &HashSet<&Triple>) -> Self {
        let genid_subject = |t: &Triple| match &t.subject {
            NamedOrBlankNode::NamedNode(s) => s.as_str().starts_with(GENID),
            _ => false,
        };
        let mut out: HashMap<&str, Vec<&Triple>> = HashMap::new();
        let mut inner: HashSet<&str> = HashSet::new();
        let mut parents: HashMap<&str, Vec<&Triple>> = HashMap::new();
        for &t in release {
            if let (NamedOrBlankNode::NamedNode(s), true) = (&t.subject, genid_subject(t)) {
                out.entry(s.as_str()).or_default().push(t);
            }
            if let (Term::NamedNode(o), true) = (&t.object, is_genid(&t.object)) {
                if genid_subject(t) {
                    inner.insert(o.as_str());
                } else {
                    parents.entry(o.as_str()).or_default().push(t);
                }
            }
        }
        let mut memo = HashMap::new();
        let mut trees: HashMap<(u128, u128), Vec<u128>> = HashMap::new();
        for (&iri, triples) in &out {
            if inner.contains(iri) {
                continue;
            }
            let (content, exact) = hashes(iri, &out, &mut memo);
            let annotated: Vec<(&str, String)> = triples
                .iter()
                .filter(|t| t.predicate.as_str().contains("#annotated"))
                .map(|t| {
                    let o = match &t.object {
                        Term::NamedNode(o) if is_genid(&t.object) => {
                            format!("{:032x}", hashes(o.as_str(), &out, &mut memo).0)
                        }
                        o => o.to_string(),
                    };
                    (t.predicate.as_str(), o)
                })
                .collect();
            let context = match parents.get(iri).map(Vec::as_slice) {
                _ if annotated.len() == 3 => key(&("axiom", sorted(annotated))),
                Some([t]) => key(&("child", t.subject.to_string(), t.predicate.as_str())),
                _ => key(&"other"),
            };
            trees.entry((context, content)).or_default().push(exact);
        }
        for exact in trees.values_mut() {
            exact.sort_unstable();
        }
        let iris = release
            .iter()
            .flat_map(|t| [Term::from(t.subject.clone()), t.object.clone()])
            .filter(is_genid)
            .map(|t| t.to_string())
            .collect();
        Self { trees, iris }
    }
}

fn key(value: &impl Hash) -> u128 {
    hash128(|h| value.hash(h))
}

fn sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items
}

/// The content of a tree (blank nodes by content) and its exact triples, hashed.
fn hashes<'a>(
    iri: &'a str,
    out: &HashMap<&'a str, Vec<&'a Triple>>,
    memo: &mut HashMap<&'a str, (u128, u128)>,
) -> (u128, u128) {
    if let Some(&h) = memo.get(iri) {
        return h;
    }
    let mut content = Vec::new();
    let mut exact = Vec::new();
    for t in out.get(iri).into_iter().flatten() {
        let p = t.predicate.as_str();
        match &t.object {
            Term::NamedNode(o) if o.as_str().starts_with(GENID) => {
                let (c, e) = hashes(o.as_str(), out, memo);
                content.push((p, format!("{c:032x}")));
                exact.push((p, format!("{o} {e:032x}")));
            }
            o => {
                content.push((p, o.to_string()));
                exact.push((p, o.to_string()));
            }
        }
    }
    let h = (key(&sorted(content)), key(&(iri, sorted(exact))));
    memo.insert(iri, h);
    h
}

fn check_skolem(dir: &Path, versions: &Versions) -> Result<Report, String> {
    let mut report = Report::new("skolemization");
    let start = Instant::now();
    let date = versions.dates.last().ok_or("no releases")?;
    let mut seen = HashSet::new();
    let again: Vec<Triple> = skolemize(read_release(&owl_file(dir, date))?)?
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect();
    if again != versions.last {
        let at = again
            .iter()
            .zip(&versions.last)
            .position(|(a, b)| a != b)
            .unwrap_or(again.len().min(versions.last.len()));
        report.problems.push(format!(
            "{date} read twice: {} and {} triples, first difference at triple {at}",
            again.len(),
            versions.last.len()
        ));
    }
    report.timings.push(("again", start.elapsed()));

    let start = Instant::now();
    let mut prev: Option<Trees> = None;
    versions.for_each_release(|k, release| {
        let trees = Trees::of(release);
        if let Some(prev) = &prev {
            let mut unchanged = 0;
            for (key, exact) in &trees.trees {
                let Some(before) = prev.trees.get(key) else {
                    continue;
                };
                unchanged += exact.len().min(before.len());
                if exact != before {
                    report.problems.push(format!(
                        "{}: an unchanged tree got other IRIs or triples",
                        versions.dates[k]
                    ));
                }
            }
            let shared = trees.iris.intersection(&prev.iris).count();
            report.line(format!(
                "{}: {unchanged} of {} trees unchanged, {shared} of {} skolem IRIs ({:.2}%) in the previous release",
                versions.dates[k],
                trees.trees.values().map(Vec::len).sum::<usize>(),
                trees.iris.len(),
                100.0 * shared as f64 / trees.iris.len().max(1) as f64
            ));
        }
        prev = Some(trees);
        Ok(())
    })?;
    report.timings.push(("trees", start.elapsed()));
    Ok(report)
}

// ---------------------------------------------------------------------------------------------
// Self-tests: they run without the data
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod self_tests {
    use super::*;
    use std::fs;

    const OBO: &str = "http://purl.obolibrary.org/obo/";

    fn class(id: &str, label: &str, namespace: &str, body: &str) -> String {
        format!(
            r#"<owl:Class rdf:about="{OBO}{id}">
        <oboInOwl:hasOBONamespace>{namespace}</oboInOwl:hasOBONamespace>
        <oboInOwl:id>{}</oboInOwl:id>
        <rdfs:label>{label}</rdfs:label>
        {body}
    </owl:Class>
"#,
            id.replace('_', ":")
        )
    }

    fn is_a(id: &str) -> String {
        format!(r#"<rdfs:subClassOf rdf:resource="{OBO}{id}"/>"#)
    }

    fn some(property: &str, id: &str) -> String {
        format!(
            r#"<owl:Restriction>
            <owl:onProperty rdf:resource="{OBO}{property}"/>
            <owl:someValuesFrom rdf:resource="{OBO}{id}"/>
        </owl:Restriction>"#
        )
    }

    const DEPRECATED: &str = r#"<owl:deprecated rdf:datatype="http://www.w3.org/2001/XMLSchema#boolean">true</owl:deprecated>"#;

    /// A small GO in RDF/XML. Release 2 relabels cytoplasm, adds a reference to the definition
    /// of mitochondrion, adds a term and obsoletes one; release 3 restores the label and
    /// obsoletes another term.
    fn small_go(release: u8) -> String {
        let cc = "cellular_component";
        let bp = "biological_process";
        let cytoplasm = if release == 2 {
            "cytoplasmic region"
        } else {
            "cytoplasm"
        };
        let definition = "A semiautonomous, self replicating organelle.";
        let mut classes = vec![
            class("GO_0005575", cc, cc, ""),
            class("GO_0005737", cytoplasm, cc, &is_a("GO_0005575")),
            class("GO_0043226", "organelle", cc, &is_a("GO_0005575")),
            class("GO_0016020", "membrane", cc, &is_a("GO_0005575")),
            class(
                "GO_0005739",
                "mitochondrion",
                cc,
                &format!(
                    r#"{}
        <rdfs:subClassOf>{}</rdfs:subClassOf>
        <obo:IAO_0000115>{definition}</obo:IAO_0000115>
        <oboInOwl:hasDbXref>Wikipedia:Mitochondrion</oboInOwl:hasDbXref>
        <oboInOwl:hasExactSynonym>mitochondria</oboInOwl:hasExactSynonym>
        <oboInOwl:inSubset rdf:resource="http://purl.obolibrary.org/obo/go#goslim_generic"/>"#,
                    is_a("GO_0043226"),
                    some("BFO_0000050", "GO_0005737")
                ),
            ),
            class(
                "GO_0005740",
                "mitochondrial envelope",
                cc,
                &format!(
                    "{}<rdfs:subClassOf>{}</rdfs:subClassOf>{}",
                    is_a("GO_0005575"),
                    some("BFO_0000050", "GO_0005739"),
                    if release == 3 { DEPRECATED } else { "" }
                ),
            ),
            class(
                "GO_0031966",
                "mitochondrial membrane",
                cc,
                &format!(
                    r#"{}
        <owl:equivalentClass>
            <owl:Class>
                <owl:intersectionOf rdf:parseType="Collection">
                    <rdf:Description rdf:about="{OBO}GO_0016020"/>
                    {}
                </owl:intersectionOf>
            </owl:Class>
        </owl:equivalentClass>"#,
                    is_a("GO_0016020"),
                    some("BFO_0000050", "GO_0005740")
                ),
            ),
            class("GO_0008150", bp, bp, ""),
            class(
                "GO_0006915",
                "apoptotic process",
                bp,
                &format!(
                    "{}<obo:IAO_0000115>A programmed cell death process: apoptotic.</obo:IAO_0000115>",
                    is_a("GO_0008150")
                ),
            ),
            class(
                "GO_0043065",
                "positive regulation of apoptotic process",
                bp,
                &format!(
                    "{}<rdfs:subClassOf>{}</rdfs:subClassOf>{}",
                    is_a("GO_0008150"),
                    some("RO_0002211", "GO_0006915"),
                    if release >= 2 { DEPRECATED } else { "" }
                ),
            ),
            class(
                "GO_0000001",
                "obsolete mitochondrion inheritance",
                bp,
                &format!(r#"{DEPRECATED}<obo:IAO_0100001 rdf:resource="{OBO}GO_0005739"/>"#),
            ),
        ];
        if release >= 2 {
            classes.push(class(
                "GO_0099999",
                "mitochondrial thing",
                cc,
                &is_a("GO_0005739"),
            ));
        }
        let extra = if release >= 2 {
            "<oboInOwl:hasDbXref>PMID:1</oboInOwl:hasDbXref>"
        } else {
            ""
        };
        format!(
            r#"<?xml version="1.0"?>
<rdf:RDF xmlns="http://purl.obolibrary.org/obo/go.owl#"
     xml:base="http://purl.obolibrary.org/obo/go.owl"
     xmlns:obo="{OBO}"
     xmlns:owl="http://www.w3.org/2002/07/owl#"
     xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
     xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
     xmlns:oboInOwl="http://www.geneontology.org/formats/oboInOwl#">
    <owl:Ontology rdf:about="{OBO}go.owl"/>
    <owl:ObjectProperty rdf:about="{OBO}BFO_0000050"><rdfs:label>part of</rdfs:label></owl:ObjectProperty>
    <owl:ObjectProperty rdf:about="{OBO}RO_0002211"><rdfs:label>regulates</rdfs:label></owl:ObjectProperty>
    {}
    <owl:Axiom>
        <owl:annotatedSource rdf:resource="{OBO}GO_0005739"/>
        <owl:annotatedProperty rdf:resource="{OBO}IAO_0000115"/>
        <owl:annotatedTarget>{definition}</owl:annotatedTarget>
        <oboInOwl:hasDbXref>GOC:giardia</oboInOwl:hasDbXref>
        {extra}
    </owl:Axiom>
    <owl:Axiom>
        <owl:annotatedSource rdf:resource="{OBO}GO_0005739"/>
        <owl:annotatedProperty rdf:resource="http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"/>
        <owl:annotatedTarget>mitochondria</owl:annotatedTarget>
        <oboInOwl:hasDbXref>NIF:1</oboInOwl:hasDbXref>
    </owl:Axiom>
</rdf:RDF>
"#,
            classes.join("    ")
        )
    }

    const DATES: [&str; 3] = ["2000-01-01", "2000-02-01", "2000-03-01"];

    fn write_small_go(dir: &Path) -> Vec<String> {
        for (k, date) in DATES.iter().enumerate() {
            fs::create_dir_all(dir.join(date)).unwrap();
            fs::write(owl_file(dir, date), small_go(k as u8 + 1)).unwrap();
        }
        DATES.iter().map(|d| d.to_string()).collect()
    }

    #[test]
    fn dates_and_times() {
        let dates = release_dates(SUMS);
        assert_eq!(dates.len(), 13);
        assert_eq!(dates[0], "2024-06-17");
        assert_eq!(dates[12], "2026-08-05");
        assert!(dates.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(millis("1970-01-01"), 0);
        assert_eq!(millis("2000-03-01"), 951_868_800_000);
        assert_eq!(millis("2024-06-17"), 1_718_582_400_000);
    }

    #[test]
    fn checksums_are_verified() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("2000-01-01")).unwrap();
        fs::write(owl_file(dir.path(), "2000-01-01"), "abc").unwrap();
        let sum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert_eq!(
            verify(dir.path(), &format!("{sum}  2000-01-01/go.owl\n")),
            Ok(())
        );
        let wrong = verify(
            dir.path(),
            &format!("{}  2000-01-01/go.owl\n", sum.replace('b', "c")),
        );
        assert!(wrong.is_err_and(|e| e.contains("expected")));
        assert!(verify(dir.path(), &format!("{sum}  2000-02-01/go.owl\n")).is_err());
    }

    #[test]
    fn skolem_iris_follow_the_content() {
        let dir = tempfile::tempdir().unwrap();
        let dates = write_small_go(dir.path());
        let read = |date: &str| skolemize(read_release(&owl_file(dir.path(), date)).unwrap());
        let first = read(&dates[0]).unwrap();
        assert_eq!(read(&dates[0]).unwrap(), first);
        let raw = read_release(&owl_file(dir.path(), &dates[0])).unwrap();
        let blank: HashSet<String> = raw
            .iter()
            .flat_map(|t| [Term::from(t.subject.clone()), t.object.clone()])
            .filter(Term::is_blank_node)
            .map(|t| t.to_string())
            .collect();
        let genid: HashSet<String> = first
            .iter()
            .flat_map(|t| [Term::from(t.subject.clone()), t.object.clone()])
            .filter(is_genid)
            .map(|t| t.to_string())
            .collect();
        // two axioms, four restrictions, an intersection class and its two list nodes
        assert_eq!(blank.len(), 9);
        assert_eq!(genid.len(), blank.len());
        assert!(first
            .iter()
            .all(|t| !t.subject.is_blank_node() && !t.object.is_blank_node()));

        let versions = Versions::read(dir.path(), &dates).unwrap();
        assert_eq!(versions.first, first);
        let counts: Vec<(usize, usize)> = versions
            .changes
            .iter()
            .map(|c| (c.removed.len(), c.added.len()))
            .collect();
        // the label; the label, the reference, the obsoletion and the new term's 5 triples
        assert_eq!(counts, [(1, 8), (1, 2)]);
        // the changed axiom keeps its IRI
        let reference = versions.changes[0]
            .added
            .iter()
            .find(|t| t.object.to_string() == "\"PMID:1\"")
            .unwrap();
        assert!(first.iter().any(|t| t.subject == reference.subject));
        assert_eq!(versions.sizes[1], versions.sizes[0] + 7);
    }

    #[test]
    fn suite_runs_on_a_small_go() {
        let dir = tempfile::tempdir().unwrap();
        let dates = write_small_go(dir.path());
        let versions = Versions::read(dir.path(), &dates).unwrap();
        let reports = [
            in_pool(|| check_current(&versions)).unwrap(),
            in_pool(|| check_versions(&versions, None, true)).unwrap(),
            in_pool(|| check_skolem(dir.path(), &versions)).unwrap(),
        ];
        for report in &reports {
            println!("{report}");
            assert!(report.problems.is_empty(), "{report}");
        }
        assert_eq!(reports[0].queries, 26);
        assert_eq!(reports[1].queries, 14 + 3 * 26);
        // the pinned changes are checked
        let wrong = in_pool(|| check_versions(&versions, Some(&[("2000-01-01", 1, 0, 0)]), false));
        assert!(wrong.unwrap().problems[0].contains("pinned"));
    }

    #[test]
    fn differences_are_found() {
        let rows = |rows: &[&str]| Normal::Rows {
            variables: vec!["x".to_owned()],
            rows: rows.iter().map(|r| vec![r.to_string()]).collect(),
        };
        assert!(compare(rows(&["a", "b"]), rows(&["b", "a"]), false).is_ok());
        assert!(compare(rows(&["a", "b"]), rows(&["b", "a"]), true).is_err());
        assert!(compare(rows(&["a", "a"]), rows(&["a"]), false).is_err());
        assert!(compare(Normal::Boolean(true), rows(&[]), false).is_err());
        assert_eq!(
            go::same_graphs("<raphtory:asof:2024-06-17>"),
            go::same_graphs(&go::release_graph("2024-06-17"))
        );
    }
}
