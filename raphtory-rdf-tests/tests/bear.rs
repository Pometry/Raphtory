//! BEAR-B as a temporal correctness suite for Raphtory's RDF support: the versions of DBpedia
//! Live resources loaded into one `PersistentGraph`, and BEAR's three kinds of versioned queries
//! (Fernández et al., "Evaluating query and storage strategies for RDF archives", SWJ 2019):
//!
//! - **Mat** (version materialisation, VM): the results of a query in version `k`;
//! - **Diff** (delta materialisation, DM): the results that differ between versions `i` and `j`;
//! - **Ver** (version queries, V): the results of a query in every version, with their version.
//!
//! # Running
//!
//! `make bear-b-fetch` downloads the data into `~/.cache/raphtory-rdf/bear-b` and
//! `make rust-test-rdf-bear` runs this file with `RAPHTORY_RDF_DATA` set, printing a report per
//! granularity. Without `RAPHTORY_RDF_DATA` the BEAR-B tests print a message and pass; the
//! self-tests at the end, which run the suite on a small generated archive, always run.
//! By default ([`Plan::sample`]) every version of day is checked and hour and instant are
//! sampled; `RAPHTORY_BEAR_FULL=1` uses [`Plan::full`] (slow).
//!
//! # The model
//!
//! Version `k` is time `k` (see the [`bear`] module, shared with
//! `rdf-bench/benches/temporal.rs`): each triple is an edge in the layer of its
//! predicate, a delta retracts its deleted triples and then asserts its added ones at the time of
//! its version, and "version `k`" is `pg.snapshot_at(k)` or the time graph `<raphtory:asof:k>`.
//! The archives do not hold version 1, so it is reconstructed from the deltas (see [`bear`]).
//!
//! # What is checked
//!
//! An *oracle* replays the same versions on an `oxrdf::Dataset` and evaluates queries with
//! spareval. Every check compares Raphtory with the oracle as multisets of solutions; any
//! difference is a bug in `raphtory::rdf`.
//!
//! - **Sanity gate:** the `(edge, layer)` pairs of `pg.snapshot_at(k).valid()`, read as triples,
//!   and `pg.snapshot_at(k)` exported with `to_rdf`, are version `k`.
//! - **Mat:** for each query `Q` (the `?s <p> ?o` and `?s <p> <o>` lookups and the two-pattern
//!   joins), `pg.snapshot_at(k).sparql(Q)` and `SELECT * { GRAPH <raphtory:asof:k> { Q } }` on
//!   `pg`; lookups are also answered natively from the layer's edges.
//! - **Diff:** jumps from version 1 to later versions, with the template `{ GRAPH <asof:j> {Q}
//!   FILTER NOT EXISTS { GRAPH <asof:i> {Q} } BIND("+" AS ?op) } UNION { GRAPH <asof:i> {Q}
//!   FILTER NOT EXISTS { GRAPH <asof:j> {Q} } BIND("-" AS ?op) }`. It must match the oracle on a
//!   dataset with the two versions as named graphs, and the set differences of the two Mat
//!   results. Lookups are also answered natively from edge history.
//! - **Ver:** `SELECT * { VALUES ?g { <raphtory:asof:1> ... } GRAPH ?g { Q } }`, and the same with
//!   `FROM NAMED`, must be every Mat result tagged with its version. For lookups, the interval
//!   form with `raphtory:validFromTime` / `raphtory:validToTime` must give the validity runs of
//!   the triples (unbound where two objects of the subject share a canonical form), and the
//!   native form reads every run from edge history.
//!
//! Every version is loaded and replayed, but checks run only at the versions of a [`Plan`].
//!
//! # BEAR's official results
//!
//! BEAR publishes Mat, Diff and Ver results of the lookups for day and hour. They are a second,
//! weaker check, since they do not fit the change-based archive exactly; the report counts how
//! many agree and the counts are pinned in [`OFFICIAL`].
//!
//! - They hold the subject and the lexical form of the object only, with non-ASCII characters
//!   written as `?`, so results are compared in that form, as multisets. Their versions are
//!   numbered from 0 (`t = v + 1`); versions the archive does not have are not compared.
//! - The static core of version 1 is taken from the official version 0: each expected result is
//!   ours plus the official version-0 rows our reconstructed version 1 lacks (`version 1 in v0`
//!   checks the reconstruction is a subset).
//! - `versions` counts agreeing checked versions (but the first); `changes` counts consecutive
//!   checked pairs where either side changed, agreeing when both changed the same way;
//!   `diff jumps` counts BEAR's jumps from version 0 whose added and deleted rows agree.
//! - The official versions drift from the deltas (rows appear or vanish with no delta), so
//!   once a query drifts every later version differs; hour `?P?` Diff jumps are compared without
//!   the static core.
//! - The Ver files are the Mat files, so they are checked the same way.
//! - The join results are not used: they are not consistent with the join condition.
#[path = "common/bear.rs"]
mod bear;

use bear::{
    asof, bear_b_dir, read_queries, read_zip, BearQuery, Changes, Granularity, Kind, Lookup, Pair,
};
use flate2::{write::GzEncoder, Compression};
use raphtory::{
    prelude::*,
    rdf::{
        evaluator,
        model::{
            Dataset, GraphName, GraphNameRef, Literal, NamedNode, Quad, QuadRef, Term, Triple,
        },
        term_of, RdfFormat, RdfViewOps, SparqlResults, ASOF_NS,
    },
};
use rayon::prelude::*;
use spareval::ExpressionTerm;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt, fs,
    io::Write,
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};

/// The agreement with BEAR's official results under the default plan: (granularity, check,
/// agreeing, compared). Not checked with `RAPHTORY_BEAR_FULL`.
const OFFICIAL: &[(&str, &str, usize, usize)] = &[
    ("day", "p diff jumps", 498, 833),
    ("day", "p mat changes", 580, 868),
    ("day", "p mat versions", 2401, 4312),
    ("day", "p ver changes", 580, 868),
    ("day", "p ver versions", 2401, 4312),
    ("day", "p version 1 in v0", 49, 49),
    ("day", "po diff jumps", 149, 195),
    ("day", "po mat changes", 25, 38),
    ("day", "po mat versions", 912, 1144),
    ("day", "po ver changes", 25, 38),
    ("day", "po ver versions", 912, 1144),
    ("day", "po version 1 in v0", 13, 13),
    ("hour", "p diff jumps", 82, 196),
    ("hour", "po diff jumps", 41, 52),
    ("hour", "po mat changes", 15, 20),
    ("hour", "po mat versions", 138, 169),
    ("hour", "po ver changes", 15, 20),
    ("hour", "po ver versions", 138, 169),
    ("hour", "po version 1 in v0", 13, 13),
];

#[test]
fn bear_b_day() {
    bear_b(Granularity::Day);
}

#[test]
fn bear_b_hour() {
    bear_b(Granularity::Hour);
}

#[test]
fn bear_b_instant() {
    bear_b(Granularity::Instant);
}

/// Runs the suite on one granularity of the downloaded data, prints the report and fails if it
/// has problems.
fn bear_b(granularity: Granularity) {
    let dir = match bear_b_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping BEAR-B {}: {why}", granularity.name());
            return;
        }
    };
    if !granularity.archive(&dir).is_file() {
        println!(
            "skipping BEAR-B {}: {} is missing (`make bear-b-fetch` downloads it)",
            granularity.name(),
            granularity.archive(&dir).display()
        );
        return;
    }
    let pinned = if is_full() { &[][..] } else { OFFICIAL };
    let report = run_suite(&dir, granularity, &Plan::new(granularity), pinned)
        .unwrap_or_else(|e| panic!("BEAR-B {}: {e}", granularity.name()));
    println!("{report}");
    assert!(
        report.problems.is_empty(),
        "{} problem(s) in BEAR-B {}:\n{}",
        report.problems.len(),
        granularity.name(),
        report.problems.join("\n")
    );
}

// ---------------------------------------------------------------------------------------------
// Solutions
// ---------------------------------------------------------------------------------------------

/// A solution: its values, in the order of the sorted variable names, as N-Triples terms
/// (`UNDEF` where unbound).
type Row = Vec<String>;

/// Solutions as a sorted multiset, with their sorted variable names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Rows {
    variables: Vec<String>,
    rows: Vec<Row>,
}

const UNDEF: &str = "UNDEF";

impl Rows {
    fn new(variables: Vec<String>, mut rows: Vec<Row>) -> Self {
        rows.sort_unstable();
        Self { variables, rows }
    }

    fn from_results(results: SparqlResults) -> Result<Self, String> {
        let SparqlResults::Solutions { variables, rows } = results else {
            return Err("not solutions".to_owned());
        };
        let mut order: Vec<usize> = (0..variables.len()).collect();
        order.sort_by(|&a, &b| variables[a].as_str().cmp(variables[b].as_str()));
        let names = order
            .iter()
            .map(|&i| variables[i].as_str().to_owned())
            .collect();
        let rows = rows
            .into_iter()
            .map(|row| {
                order
                    .iter()
                    .map(|&i| {
                        row[i]
                            .as_ref()
                            .map_or_else(|| UNDEF.to_owned(), Term::to_string)
                    })
                    .collect()
            })
            .collect();
        Ok(Self::new(names, rows))
    }

    /// The rows of `self` that are not in `other`, as sets.
    fn minus(&self, other: &Self) -> Vec<Row> {
        let other: HashSet<&Row> = other.rows.iter().collect();
        let mut rows: Vec<Row> = self
            .rows
            .iter()
            .filter(|row| !other.contains(row))
            .cloned()
            .collect();
        rows.dedup();
        rows
    }

    /// The column of a variable.
    fn column(&self, variable: &str) -> Option<usize> {
        self.variables.iter().position(|v| v == variable)
    }
}

/// Compares solutions, with a short description of the difference.
fn compare(expected: &Rows, actual: &Rows) -> Result<(), String> {
    if expected.variables != actual.variables {
        return Err(format!(
            "expected variables {:?}, got {:?}",
            expected.variables, actual.variables
        ));
    }
    if expected.rows == actual.rows {
        return Ok(());
    }
    let only = |x: &[Row], y: &[Row]| {
        let mut y: HashMap<&Row, usize> = y.iter().fold(HashMap::new(), |mut m, r| {
            *m.entry(r).or_default() += 1;
            m
        });
        let rows: Vec<String> = x
            .iter()
            .filter(|row| match y.get_mut(row) {
                Some(n) if *n > 0 => {
                    *n -= 1;
                    false
                }
                _ => true,
            })
            .map(|row| row.join(" "))
            .collect();
        sample(&rows)
    };
    Err(format!(
        "solutions differ ({} expected, {} actual); only expected: {}; only actual: {}",
        expected.rows.len(),
        actual.rows.len(),
        only(&expected.rows, &actual.rows),
        only(&actual.rows, &expected.rows)
    ))
}

/// The first few items, for messages.
fn sample(items: &[String]) -> String {
    let shown: Vec<&str> = items.iter().take(4).map(String::as_str).collect();
    let shown = shown.join(" | ");
    let shown: String = shown.chars().take(600).collect();
    if items.len() > 4 {
        format!("[{shown} | ... {} more]", items.len() - 4)
    } else {
        format!("[{shown}]")
    }
}

/// Raphtory's solutions of a query on a view.
fn ours<G: RdfViewOps>(view: &G, query: &str) -> Result<Rows, String> {
    view.sparql(query)
        .map_err(|e| format!("Raphtory: {e}"))
        .and_then(Rows::from_results)
}

/// The oracle's solutions of a query on a dataset.
fn oracle(dataset: &Dataset, query: &str) -> Result<Rows, String> {
    let results = evaluator()
        .parse_query(query)
        .map_err(|e| format!("oracle: {e}"))?
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(|e| format!("oracle: {e}"))?;
    SparqlResults::from_query_results(results)
        .map_err(|e| format!("oracle: {e}"))
        .and_then(Rows::from_results)
}

/// The solutions of a lookup from native `(subject, object)` pairs, as `SELECT *` gives them.
fn lookup_rows(lookup: &Lookup, pairs: impl IntoIterator<Item = Pair>) -> Rows {
    let term = |name: &str| term_of(name).to_string();
    match lookup.object {
        // columns ?o ?s
        None => Rows::new(
            vec!["o".to_owned(), "s".to_owned()],
            pairs
                .into_iter()
                .map(|(s, o)| vec![term(&o), term(&s)])
                .collect(),
        ),
        Some(_) => Rows::new(
            vec!["s".to_owned()],
            pairs.into_iter().map(|(s, _)| vec![term(&s)]).collect(),
        ),
    }
}

/// An `xsd:integer` time, as the temporal functions return it.
fn time_literal(t: i64) -> String {
    Literal::from(t).to_string()
}

/// The form in which a temporal function receives a term: SPARQL hands functions a literal of
/// an xsd value type in canonical form.
fn canonical(term: &Term) -> Term {
    Term::from(ExpressionTerm::from(term.clone()))
}

/// Whether a term reaches functions as it is (IRIs, blank nodes, strings and literals of other
/// datatypes), so that it identifies its node.
fn passed_unchanged(term: &Term) -> bool {
    matches!(
        ExpressionTerm::from(term.clone()),
        ExpressionTerm::NamedNode(_)
            | ExpressionTerm::BlankNode(_)
            | ExpressionTerm::StringLiteral(_)
            | ExpressionTerm::LangStringLiteral { .. }
            | ExpressionTerm::OtherTypedLiteral { .. }
    )
}

// ---------------------------------------------------------------------------------------------
// The oracle: the versions on an oxrdf::Dataset
// ---------------------------------------------------------------------------------------------

/// A validity run: `[from, to)`, `to` `None` while open.
type Run = (i64, Option<i64>);

/// Replays the versions: the current version in the default graph of a dataset, and the
/// validity runs of every triple seen so far.
struct Replay {
    t: i64,
    dataset: Dataset,
    runs: HashMap<Triple, Vec<Run>>,
}

impl Replay {
    /// Version 1.
    fn new(base: &[Triple]) -> Self {
        let mut replay = Self {
            t: 1,
            dataset: Dataset::new(),
            runs: HashMap::new(),
        };
        for triple in base {
            replay.set(triple, true);
        }
        replay
    }

    fn set(&mut self, triple: &Triple, present: bool) {
        let quad = QuadRef::new(
            &triple.subject,
            &triple.predicate,
            &triple.object,
            GraphNameRef::DefaultGraph,
        );
        let runs = self.runs.entry(triple.clone()).or_default();
        if present {
            if self.dataset.insert(quad) {
                runs.push((self.t, None));
            }
        } else if self.dataset.remove(quad) {
            if let Some(run) = runs.last_mut() {
                run.1 = Some(self.t);
            }
        }
    }

    /// The next version: `(V \ deleted) ∪ added`.
    fn apply(&mut self, deleted: &[Triple], added: &[Triple]) {
        self.t += 1;
        let added_set: HashSet<&Triple> = added.iter().collect();
        for triple in deleted {
            if !added_set.contains(triple) {
                self.set(triple, false);
            }
        }
        for triple in added {
            self.set(triple, true);
        }
    }

    /// The triples of the current version.
    fn triples(&self) -> HashSet<Triple> {
        self.dataset
            .iter()
            .map(|quad| Triple::new(quad.subject, quad.predicate, quad.object))
            .collect()
    }

    /// The current version and version 1 as the time graphs `<raphtory:asof:t>` and
    /// `<raphtory:asof:1>`.
    fn diff_dataset(&self, base: &[Triple]) -> Dataset {
        let named = |t: i64| GraphName::from(NamedNode::new_unchecked(format!("{ASOF_NS}{t}")));
        let mut dataset = Dataset::new();
        let first = named(1);
        for triple in base {
            dataset.insert(&Quad::new(
                triple.subject.clone(),
                triple.predicate.clone(),
                triple.object.clone(),
                first.clone(),
            ));
        }
        let current = named(self.t);
        for quad in self.dataset.iter() {
            dataset.insert(QuadRef::new(
                quad.subject,
                quad.predicate,
                quad.object,
                match &current {
                    GraphName::NamedNode(n) => GraphNameRef::NamedNode(n.as_ref()),
                    _ => unreachable!(),
                },
            ));
        }
        dataset
    }

    /// The run of a triple that contains `t`.
    fn run_at(&self, triple: &Triple, t: i64) -> Option<Run> {
        self.runs
            .get(triple)?
            .iter()
            .copied()
            .find(|&(from, to)| from <= t && to.is_none_or(|to| t < to))
    }
}

// ---------------------------------------------------------------------------------------------
// The plan and the report
// ---------------------------------------------------------------------------------------------

/// The environment variable that turns on exhaustive checking (see [`Plan::full`]).
const FULL_ENV: &str = "RAPHTORY_BEAR_FULL";

/// Which versions are checked.
#[derive(Clone, Debug)]
struct Plan {
    /// Versions checked by the sanity gate on the edges of `pg.snapshot_at(k).valid()`.
    gate: BTreeSet<i64>,
    /// Versions checked by the sanity gate on the export of `pg.snapshot_at(k)` with `to_rdf`.
    export: BTreeSet<i64>,
    /// Versions at which Mat is checked (and which Ver lists).
    mat: BTreeSet<i64>,
    /// The versions `j` of the Diff jumps from version 1 (a subset of `mat`).
    diff: BTreeSet<i64>,
}

impl Plan {
    /// The number of versions of a granularity.
    fn versions(granularity: Granularity) -> i64 {
        match granularity {
            Granularity::Day => 89,
            Granularity::Hour => 1299,
            Granularity::Instant => 21046,
        }
    }

    /// The default plan: every version of day with a Diff jump to each; every 100th version of
    /// hour (jumps every 300) and every 5,000th of instant (jumps every 10,000).
    fn sample(granularity: Granularity) -> Self {
        let versions = Self::versions(granularity);
        match granularity {
            Granularity::Day => Self::every(versions, 1, 1, 1, 1),
            Granularity::Hour => Self::every(versions, 100, 100, 100, 300),
            Granularity::Instant => Self::every(versions, 5000, 5000, 5000, 10000),
        }
    }

    /// The exhaustive plan (`RAPHTORY_BEAR_FULL=1`): every version of day and hour (with BEAR's
    /// Diff jumps every 5 versions in hour); instant in every 10th (gate), 500th (export), 100th
    /// (Mat) and 500th (Diff) version.
    fn full(granularity: Granularity) -> Self {
        let versions = Self::versions(granularity);
        match granularity {
            Granularity::Day => Self::every(versions, 1, 1, 1, 1),
            Granularity::Hour => Self::every(versions, 1, 1, 1, 5),
            Granularity::Instant => Self::every(versions, 10, 500, 100, 500),
        }
    }

    /// The plan of a run: [`full`](Self::full) if `RAPHTORY_BEAR_FULL` is set (and not `0`),
    /// else [`sample`](Self::sample).
    fn new(granularity: Granularity) -> Self {
        if is_full() {
            Self::full(granularity)
        } else {
            Self::sample(granularity)
        }
    }

    /// The first, the last and every `step`-th version (`1, 1 + step, ..`) for each set; the
    /// Diff jumps (from version 1) include BEAR's jumps when `diff` divides 5. Mat is also
    /// checked at the Diff versions and the gate at the Mat versions.
    fn every(versions: i64, gate: i64, export: i64, mat: i64, diff: i64) -> Self {
        let every = |step: i64| -> BTreeSet<i64> {
            (1..=versions)
                .filter(|t| (t - 1) % step == 0)
                .chain([versions])
                .collect()
        };
        let mut diff: BTreeSet<i64> = every(diff);
        diff.remove(&1);
        let mut mat = every(mat);
        mat.extend(&diff);
        let mut gate = every(gate);
        gate.extend(&mat);
        Self {
            gate,
            export: every(export),
            mat,
            diff,
        }
    }

    /// The plan without versions past `last`.
    fn up_to(&self, last: i64) -> Self {
        let keep = |set: &BTreeSet<i64>| set.iter().copied().filter(|&t| t <= last).collect();
        Self {
            gate: keep(&self.gate),
            export: keep(&self.export),
            mat: keep(&self.mat),
            diff: keep(&self.diff),
        }
    }
}

/// Whether `RAPHTORY_BEAR_FULL` asks for exhaustive checking.
fn is_full() -> bool {
    std::env::var(FULL_ENV).is_ok_and(|v| !v.is_empty() && v != "0")
}

/// Pass and fail counts of one check.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Count {
    pass: usize,
    fail: usize,
}

/// What a run of the suite found.
#[derive(Debug, Default)]
struct Report {
    granularity: String,
    versions: usize,
    triples: usize,
    base: usize,
    queries: usize,
    /// The number of triples in the first, the middle and the last version, by version.
    sizes: BTreeMap<i64, usize>,
    /// Checks against the oracle, by name.
    checks: BTreeMap<String, Count>,
    /// Agreement with BEAR's official results: `(agreeing, compared)` by name.
    official: BTreeMap<String, (usize, usize)>,
    /// The time each phase took.
    timings: Vec<(&'static str, Duration)>,
    problems: Vec<String>,
}

impl Report {
    fn record(&mut self, check: &str, what: impl fmt::Display, outcome: Result<(), String>) {
        let count = self.checks.entry(check.to_owned()).or_default();
        match outcome {
            Ok(()) => count.pass += 1,
            Err(e) => {
                count.fail += 1;
                // a few per check are enough to debug
                if count.fail <= 5 {
                    self.problems.push(format!("{check} {what}: {e}"));
                } else if count.fail == 6 {
                    self.problems
                        .push(format!("{check}: more failures not shown"));
                }
            }
        }
    }

    fn official(&mut self, check: &str, agrees: bool) {
        let entry = self.official.entry(check.to_owned()).or_default();
        entry.0 += agrees as usize;
        entry.1 += 1;
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "BEAR-B {}: {} versions, {} triples written ({} in the reconstructed version 1), {} queries",
            self.granularity, self.versions, self.triples, self.base, self.queries
        )?;
        let sizes: Vec<String> = self
            .sizes
            .iter()
            .map(|(t, n)| format!("{n} in version {t}"))
            .collect();
        writeln!(f, "  triples: {}", sizes.join(", "))?;
        writeln!(
            f,
            "  {:<28} {:>8} {:>8}",
            "check (vs the oracle)", "pass", "fail"
        )?;
        for (check, count) in &self.checks {
            writeln!(f, "  {check:<28} {:>8} {:>8}", count.pass, count.fail)?;
        }
        let timings: Vec<String> = self
            .timings
            .iter()
            .map(|(phase, d)| format!("{phase} {:.1}s", d.as_secs_f64()))
            .collect();
        writeln!(f, "  time: {}", timings.join(", "))?;
        if !self.official.is_empty() {
            writeln!(f, "  {:<28} {:>8} {:>8}", "official results", "agree", "of")?;
            for (check, (agree, of)) in &self.official {
                writeln!(f, "  {check:<28} {agree:>8} {of:>8}")?;
            }
        }
        if !self.problems.is_empty() {
            writeln!(f, "  problems:")?;
            for problem in &self.problems {
                writeln!(f, "  - {problem}")?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Running the suite
// ---------------------------------------------------------------------------------------------

/// The outcomes of named checks.
type Checks = Vec<(&'static str, Result<(), String>)>;

/// The outcome of the Mat checks of one query at one version.
struct MatOutcome {
    expected: Result<Rows, String>,
    checks: Checks,
}

/// Runs the suite on the archive of one granularity in a BEAR-B directory.
fn run_suite(
    dir: &Path,
    granularity: Granularity,
    plan: &Plan,
    official: &[(&str, &str, usize, usize)],
) -> Result<Report, String> {
    let changes = Changes::read(&granularity.archive(dir))?;
    let queries = read_queries(dir)?;
    let mut report = Report {
        granularity: granularity.name().to_owned(),
        versions: changes.versions(),
        triples: changes.triples(),
        base: changes.base.len(),
        queries: queries.len(),
        ..Report::default()
    };
    let last = changes.versions() as i64;
    let plan = plan.up_to(last);

    let start = Instant::now();
    let pg = PersistentGraph::new();
    bear::load(&pg, &changes).map_err(|e| format!("cannot load the versions: {e}"))?;
    report.timings.push(("load", start.elapsed()));
    let (mut gate_time, mut export_time, mut mat_time, mut diff_time) = Default::default();
    // the versions to export, checked in parallel in batches of EXPORT_BATCH
    let mut exports: Vec<(i64, HashSet<Triple>)> = Vec::new();

    // the oracle's Mat solutions, by query and version
    let mut mats: HashMap<(usize, i64), Rows> = HashMap::new();
    let mut replay = Replay::new(&changes.base);
    for t in 1..=last {
        if t > 1 {
            let delta = &changes.deltas[(t - 2) as usize];
            let deleted = bear::parse(&delta.deleted)?;
            let added = bear::parse(&delta.added)?;
            replay.apply(&deleted, &added);
        }
        // the middle version is the one the benchmark measures
        if [1, (last + 1) / 2, last].contains(&t) {
            report.sizes.insert(t, replay.dataset.len());
        }
        if plan.gate.contains(&t) {
            let start = Instant::now();
            let outcome = gate(&pg, &replay, t);
            report.record("gate valid edges", format!("t={t}"), outcome);
            gate_time += start.elapsed();
        }
        if plan.export.contains(&t) {
            exports.push((t, replay.triples()));
            // a few at a time, so the versions waiting for their export stay few
            if exports.len() >= EXPORT_BATCH {
                let start = Instant::now();
                export_gates(&pg, &mut exports, &mut report);
                export_time += start.elapsed();
            }
        }
        if !plan.mat.contains(&t) {
            continue;
        }
        let start = Instant::now();
        let outcomes: Vec<MatOutcome> = queries
            .par_iter()
            .map(|q| mat_checks(&pg, &replay.dataset, q, t))
            .collect();
        for (i, (q, outcome)) in queries.iter().zip(outcomes).enumerate() {
            for (check, result) in outcome.checks {
                report.record(check, format_args!("{q} t={t}"), result);
            }
            match outcome.expected {
                Ok(rows) => {
                    mats.insert((i, t), rows);
                }
                Err(e) => report.problems.push(format!("oracle {q} t={t}: {e}")),
            }
        }
        mat_time += start.elapsed();
        if plan.diff.contains(&t) {
            let start = Instant::now();
            let dataset = replay.diff_dataset(&changes.base);
            let outcomes: Vec<Checks> = queries
                .par_iter()
                .enumerate()
                .map(|(i, q)| {
                    diff_checks(&pg, &dataset, q, t, mats.get(&(i, 1)), mats.get(&(i, t)))
                })
                .collect();
            for (q, checks) in queries.iter().zip(outcomes) {
                for (check, result) in checks {
                    report.record(check, format_args!("{q} 1->{t}"), result);
                }
            }
            diff_time += start.elapsed();
        }
    }
    let start = Instant::now();
    export_gates(&pg, &mut exports, &mut report);
    export_time += start.elapsed();
    report.timings.push(("gate", gate_time));
    report.timings.push(("export", export_time));
    report.timings.push(("mat", mat_time));
    report.timings.push(("diff", diff_time));

    // Ver, over the versions of Mat
    let start = Instant::now();
    let versions: Vec<i64> = plan.mat.iter().copied().collect();
    let outcomes: Vec<(Checks, Option<Rows>)> = queries
        .par_iter()
        .enumerate()
        .map(|(i, q)| ver_checks(&pg, &replay, q, i, &versions, &mats))
        .collect();
    let mut vers = HashMap::new();
    for (i, (q, (checks, ver))) in queries.iter().zip(outcomes).enumerate() {
        for (check, result) in checks {
            report.record(check, q, result);
        }
        if let Some(ver) = ver {
            vers.insert(i, ver);
        }
    }

    report.timings.push(("ver", start.elapsed()));
    let start = Instant::now();
    official_checks(
        dir,
        granularity,
        &queries,
        &plan,
        last,
        &mats,
        &vers,
        &mut report,
    )?;
    report.timings.push(("official", start.elapsed()));
    for &(g, check, agree, of) in official {
        if g != granularity.name() {
            continue;
        }
        match report.official.get(check) {
            Some(&found) if found == (agree, of) => {}
            found => report.problems.push(format!(
                "official {check}: {agree} of {of} agreed before, now {found:?}"
            )),
        }
    }
    Ok(report)
}

/// The sanity gate: the `(edge, layer)` pairs of `pg.snapshot_at(t).valid()`, read as triples
/// (subject and object from the node names, predicate from the layer), are exactly version `t`.
fn gate(pg: &PersistentGraph, replay: &Replay, t: i64) -> Result<(), String> {
    let ours: Vec<String> = pg
        .snapshot_at(t)
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
        .collect::<Result<_, String>>()?;
    let expected: HashSet<String> = replay.triples().iter().map(Triple::to_string).collect();
    let n = ours.len();
    let ours: HashSet<String> = ours.into_iter().collect();
    if n == ours.len() && ours == expected {
        return Ok(());
    }
    let missing: Vec<String> = expected.difference(&ours).cloned().collect();
    let extra: Vec<String> = ours.difference(&expected).cloned().collect();
    Err(format!(
        "{n} pairs ({} distinct), expected {} triples; missing {}; extra {}",
        ours.len(),
        expected.len(),
        sample(&missing),
        sample(&extra)
    ))
}

/// How many versions [`export_gates`] checks at once.
const EXPORT_BATCH: usize = 16;

/// Runs the export gate on the waiting versions, in parallel, and empties the list.
fn export_gates(
    pg: &PersistentGraph,
    exports: &mut Vec<(i64, HashSet<Triple>)>,
    report: &mut Report,
) {
    let outcomes: Vec<(i64, Result<(), String>)> = exports
        .par_iter()
        .map(|(t, expected)| (*t, export_gate(pg, expected, *t)))
        .collect();
    exports.clear();
    for (t, outcome) in outcomes {
        report.record("gate to_rdf", format!("t={t}"), outcome);
    }
}

/// The sanity gate on the export: `pg.snapshot_at(t)` exported with `to_rdf` is exactly version
/// `t` (`expected`).
fn export_gate(pg: &PersistentGraph, expected: &HashSet<Triple>, t: i64) -> Result<(), String> {
    let mut doc = Vec::new();
    let stats = pg
        .snapshot_at(t)
        .to_rdf(&mut doc, RdfFormat::NTriples)
        .map_err(|e| e.to_string())?;
    let ours: HashSet<Triple> = bear::parse(&doc)?.into_iter().collect();
    if stats.skipped == 0 && ours.len() == stats.triples && ours == *expected {
        return Ok(());
    }
    let missing: Vec<String> = expected.difference(&ours).map(Triple::to_string).collect();
    let extra: Vec<String> = ours.difference(expected).map(Triple::to_string).collect();
    Err(format!(
        "{} triples ({} skipped), expected {}; missing {}; extra {}",
        stats.triples,
        stats.skipped,
        expected.len(),
        sample(&missing),
        sample(&extra)
    ))
}

/// Mat at version `t`: the oracle, the snapshot view, the time graph and (for lookups) the
/// native form.
fn mat_checks(pg: &PersistentGraph, dataset: &Dataset, q: &BearQuery, t: i64) -> MatOutcome {
    let expected = oracle(dataset, &q.mat());
    let mut checks = Vec::new();
    let check = |actual: Result<Rows, String>| -> Result<(), String> {
        match &expected {
            Ok(expected) => compare(expected, &actual?),
            Err(e) => Err(e.clone()),
        }
    };
    checks.push(("mat snapshot_at", check(ours(&pg.snapshot_at(t), &q.mat()))));
    checks.push(("mat GRAPH asof", check(ours(pg, &q.mat_asof(t)))));
    if let Some(lookup) = &q.lookup {
        let native = lookup_rows(lookup, bear::native_mat(pg, lookup, t));
        checks.push(("mat native", check(Ok(native))));
    }
    MatOutcome { expected, checks }
}

/// Diff from version 1 to version `t`: the template on Raphtory against the template on the
/// oracle and against the set differences of the oracle's Mat solutions, and (for lookups) the
/// native form.
fn diff_checks(
    pg: &PersistentGraph,
    dataset: &Dataset,
    q: &BearQuery,
    t: i64,
    first: Option<&Rows>,
    last: Option<&Rows>,
) -> Checks {
    let query = q.diff(1, t);
    let actual = ours(pg, &query);
    let mut checks = Vec::new();
    let template = oracle(dataset, &query);
    checks.push((
        "diff template",
        template.and_then(|expected| compare(&expected, actual.as_ref().map_err(Clone::clone)?)),
    ));
    let (Some(first), Some(last)) = (first, last) else {
        checks.push((
            "diff set difference",
            Err("no oracle Mat solutions".to_owned()),
        ));
        return checks;
    };
    // BEAR's Diff: the solutions of j not in i (+) and of i not in j (-)
    let mut variables = first.variables.clone();
    variables.push("op".to_owned());
    variables.sort();
    let op = variables.iter().position(|v| v == "op").unwrap();
    let tag = |rows: Vec<Row>, sign: &str| -> Vec<Row> {
        rows.into_iter()
            .map(|mut row| {
                row.insert(op, Literal::new_simple_literal(sign).to_string());
                row
            })
            .collect()
    };
    let mut rows = tag(last.minus(first), "+");
    rows.extend(tag(first.minus(last), "-"));
    let expected = Rows::new(variables, rows);
    checks.push((
        "diff set difference",
        actual.and_then(|actual| compare(&expected, &actual)),
    ));
    if let Some(lookup) = &q.lookup {
        let (added, deleted) = bear::native_diff(pg, lookup, 1, t);
        let native = |pairs: Vec<Pair>, sign| tag(lookup_rows(lookup, pairs).rows, sign);
        let mut rows = native(added, "+");
        rows.extend(native(deleted, "-"));
        checks.push((
            "diff native",
            compare(&expected, &Rows::new(expected.variables.clone(), rows)),
        ));
    }
    checks
}

/// Ver over `versions`: the `VALUES` and `FROM NAMED` forms and, for lookups, the interval and
/// native forms. Also returns Raphtory's `VALUES` solutions, for the official results.
fn ver_checks(
    pg: &PersistentGraph,
    replay: &Replay,
    q: &BearQuery,
    i: usize,
    versions: &[i64],
    mats: &HashMap<(usize, i64), Rows>,
) -> (Checks, Option<Rows>) {
    let mut checks = Vec::new();
    // every Mat solution, tagged with its version
    let mut expected: Option<Rows> = None;
    for &t in versions {
        let Some(mat) = mats.get(&(i, t)) else {
            checks.push(("ver VALUES", Err(format!("no oracle Mat solutions at {t}"))));
            return (checks, None);
        };
        let expected = expected.get_or_insert_with(|| {
            let mut variables = mat.variables.clone();
            variables.push("g".to_owned());
            variables.sort();
            Rows::new(variables, Vec::new())
        });
        let g = expected.column("g").unwrap();
        for row in &mat.rows {
            let mut row = row.clone();
            row.insert(g, asof(t));
            expected.rows.push(row);
        }
    }
    let Some(mut expected) = expected else {
        return (checks, None);
    };
    expected.rows.sort_unstable();
    let values = ours(pg, &q.ver_values(versions));
    checks.push((
        "ver VALUES",
        values
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|actual| compare(&expected, actual)),
    ));
    checks.push((
        "ver FROM NAMED",
        ours(pg, &q.ver_from_named(versions)).and_then(|actual| compare(&expected, &actual)),
    ));
    if let (Some(lookup), Some(query)) = (&q.lookup, q.ver_intervals(versions)) {
        checks.push((
            "ver intervals",
            intervals(replay, lookup, i, versions, mats)
                .and_then(|expected| compare(&expected, &ours(pg, &query)?)),
        ));
        checks.push(("ver native", native_ver_check(pg, replay, lookup)));
    }
    (checks, values.ok())
}

/// The expected solutions of the interval form of Ver: for each triple in each listed version,
/// the run that contains the version, or unbound ends if another visible object of the subject
/// has the same canonical form (the functions cannot tell them apart).
fn intervals(
    replay: &Replay,
    lookup: &Lookup,
    i: usize,
    versions: &[i64],
    mats: &HashMap<(usize, i64), Rows>,
) -> Result<Rows, String> {
    let mut rows = BTreeSet::new();
    for &t in versions {
        let mat = mats
            .get(&(i, t))
            .ok_or_else(|| format!("no oracle Mat solutions at {t}"))?;
        let s_col = mat.column("s").ok_or("no ?s")?;
        let o_col = mat.column("o");
        let triples: Vec<(Term, Term)> = mat
            .rows
            .iter()
            .map(|row| {
                let s = Term::from_str(&row[s_col]).map_err(|e| e.to_string())?;
                let o = match (o_col, &lookup.object) {
                    (Some(c), _) => Term::from_str(&row[c]).map_err(|e| e.to_string())?,
                    (None, Some(o)) => o.clone(),
                    (None, None) => return Err("no ?o".to_owned()),
                };
                Ok((s, o))
            })
            .collect::<Result<_, String>>()?;
        // objects of a subject with the same canonical form, among those that are not passed
        // unchanged
        let mut same_form: HashMap<(&Term, Term), usize> = HashMap::new();
        for (s, o) in &triples {
            if !passed_unchanged(o) {
                *same_form.entry((s, canonical(o))).or_default() += 1;
            }
        }
        for (s, o) in &triples {
            let ambiguous = !passed_unchanged(o) && same_form[&(s, canonical(o))] > 1;
            let (from, to) = if ambiguous {
                (UNDEF.to_owned(), UNDEF.to_owned())
            } else {
                let triple = Triple::new(
                    match s {
                        Term::NamedNode(n) => n.clone(),
                        _ => return Err(format!("subject {s} is not an IRI")),
                    },
                    lookup.predicate.clone(),
                    o.clone(),
                );
                let (from, to) = replay
                    .run_at(&triple, t)
                    .ok_or_else(|| format!("{triple} has no run at {t}"))?;
                (
                    time_literal(from),
                    to.map_or_else(|| UNDEF.to_owned(), time_literal),
                )
            };
            // columns: ?from ?o ?s ?to (sorted), or ?from ?s ?to
            let mut row = vec![from];
            if lookup.object.is_none() {
                row.push(o.to_string());
            }
            row.push(s.to_string());
            row.push(to);
            rows.insert(row);
        }
    }
    let mut variables = vec!["from", "s", "to"];
    if lookup.object.is_none() {
        variables.insert(1, "o");
    }
    Ok(Rows::new(
        variables.into_iter().map(str::to_owned).collect(),
        rows.into_iter().collect(),
    ))
}

/// Ver natively: every run of every triple of the lookup, from the history of the edges, is
/// the oracle's run.
fn native_ver_check(pg: &PersistentGraph, replay: &Replay, lookup: &Lookup) -> Result<(), String> {
    let row = |s: &Term, o: &Term, (from, to): Run| -> Row {
        vec![
            s.to_string(),
            o.to_string(),
            from.to_string(),
            to.map_or_else(|| UNDEF.to_owned(), |t| t.to_string()),
        ]
    };
    let mut expected = Vec::new();
    for (triple, runs) in &replay.runs {
        if triple.predicate != lookup.predicate
            || lookup.object.as_ref().is_some_and(|o| *o != triple.object)
        {
            continue;
        }
        for &run in runs {
            expected.push(row(&triple.subject.clone().into(), &triple.object, run));
        }
    }
    let actual = bear::native_ver(pg, lookup)
        .into_iter()
        .map(|(s, o, from, to)| row(&term_of(&s), &term_of(&o), (from, to)))
        .collect();
    let variables: Vec<String> = ["s", "o", "from", "to"].map(str::to_owned).to_vec();
    compare(
        &Rows::new(variables.clone(), expected),
        &Rows::new(variables, actual),
    )
}

// ---------------------------------------------------------------------------------------------
// BEAR's official results
// ---------------------------------------------------------------------------------------------

/// A multiset of lines of an official result.
type Lines = HashMap<String, usize>;

/// The entries of an official result file: `[<header>]<value>` lines, where a value can go on
/// over the following lines (a literal with line breaks). Returns `(header, value)` pairs.
fn official_entries(text: &str, header_prefix: &[&str]) -> Vec<(String, String)> {
    let text = text.strip_suffix('\n').unwrap_or(text);
    let mut entries: Vec<(String, String)> = Vec::new();
    for line in text.split('\n') {
        let header = header_prefix
            .iter()
            .any(|p| line.starts_with(p))
            .then(|| line.find(']'))
            .flatten();
        match header {
            Some(end) => entries.push((line[1..end].to_owned(), line[end + 1..].to_owned())),
            None => match entries.last_mut() {
                Some((_, value)) => {
                    value.push('\n');
                    value.push_str(line);
                }
                None if line.is_empty() => {}
                None => entries.push((String::new(), line.to_owned())),
            },
        }
    }
    entries
}

/// An official Mat or Ver result: lines by version (numbered from 0).
fn official_versions(text: &str) -> Result<BTreeMap<usize, Lines>, String> {
    let mut versions: BTreeMap<usize, Lines> = BTreeMap::new();
    for (header, value) in official_entries(text, &["[Solution in version "]) {
        let v = header
            .strip_prefix("Solution in version ")
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| format!("unexpected line [{header}]{value}"))?;
        *versions.entry(v).or_default().entry(value).or_default() += 1;
    }
    Ok(versions)
}

/// The added and deleted lines of an official Diff result, by jump.
type Jumps = BTreeMap<usize, (BTreeSet<String>, BTreeSet<String>)>;

/// An official Diff result: the added and deleted lines by jump.
fn official_diffs(text: &str) -> Result<Jumps, String> {
    let mut jumps = Jumps::new();
    for (header, value) in official_entries(text, &["[ADD in jump ", "[DEL in jump "]) {
        let (op, jump) = header
            .split_once(" in jump ")
            .and_then(|(op, j)| Some((op, j.parse::<usize>().ok()?)))
            .ok_or_else(|| format!("unexpected line [{header}]{value}"))?;
        let entry = jumps.entry(jump).or_default();
        match op {
            "ADD" => entry.0.insert(value),
            "DEL" => entry.1.insert(value),
            _ => return Err(format!("unexpected line [{header}]{value}")),
        };
    }
    Ok(jumps)
}

/// The form of an official result line: the subject, and for `?P?` the object as its lexical
/// form with every non-ASCII character written as `?`.
fn official_line(kind: Kind, row: &Row, s_col: usize, o_col: Option<usize>) -> String {
    let s = &row[s_col];
    match (kind, o_col) {
        (Kind::P, Some(o_col)) => {
            let o = match Term::from_str(&row[o_col]) {
                Ok(Term::Literal(literal)) => literal
                    .value()
                    .chars()
                    .map(|c| if c.is_ascii() { c } else { '?' })
                    .collect(),
                _ => row[o_col].clone(),
            };
            format!("{s} {o}")
        }
        _ => s.clone(),
    }
}

/// Raphtory's solutions in the form of the official results, by version.
fn official_form(kind: Kind, rows: &Rows) -> Lines {
    let (s, o) = (rows.column("s").unwrap(), rows.column("o"));
    let mut lines = Lines::new();
    for row in &rows.rows {
        *lines.entry(official_line(kind, row, s, o)).or_default() += 1;
    }
    lines
}

fn union(a: &Lines, b: &Lines) -> Lines {
    let mut out = a.clone();
    for (line, n) in b {
        *out.entry(line.clone()).or_default() += n;
    }
    out
}

/// `a` without `b` (as multisets), or `None` if `b` is not in `a`.
fn minus(a: &Lines, b: &Lines) -> Option<Lines> {
    let mut out = a.clone();
    for (line, n) in b {
        let m = out.get_mut(line)?;
        if *m < *n {
            return None;
        }
        *m -= n;
        if *m == 0 {
            out.remove(line);
        }
    }
    Some(out)
}

/// `a ⊖ b`: the lines of `a` not matched by a line of `b`, as multisets.
fn sub(a: &Lines, b: &Lines) -> Lines {
    a.iter()
        .filter_map(|(line, &n)| {
            let n = n.saturating_sub(b.get(line).copied().unwrap_or(0));
            (n > 0).then(|| (line.clone(), n))
        })
        .collect()
}

/// The change from `before` to `after`: the lines that arrive and those that leave.
fn changes(before: &Lines, after: &Lines) -> (Lines, Lines) {
    (sub(after, before), sub(before, after))
}

/// Whether a change (from [`changes`]) has no line arriving or leaving.
fn unchanged((arrived, left): &(Lines, Lines)) -> bool {
    arrived.is_empty() && left.is_empty()
}

fn keys(lines: &Lines) -> BTreeSet<&String> {
    lines.keys().collect()
}

/// Compares Raphtory's results (with the static core of version 1 from the official version 0)
/// with BEAR's official results: Mat, Ver and Diff of the lookups, where downloaded.
#[allow(clippy::too_many_arguments)]
fn official_checks(
    dir: &Path,
    granularity: Granularity,
    queries: &[BearQuery],
    plan: &Plan,
    last: i64,
    mats: &HashMap<(usize, i64), Rows>,
    vers: &HashMap<usize, Rows>,
    report: &mut Report,
) -> Result<(), String> {
    let g = granularity.name();
    for kind in [Kind::P, Kind::Po] {
        let k = kind.name();
        let zip = |what: &str| dir.join(format!("results/{g}/{k}/{what}-{k}-queries.zip"));
        let read = |what: &str| -> Result<Option<HashMap<usize, String>>, String> {
            let path = zip(what);
            if !path.is_file() {
                return Ok(None);
            }
            let prefix = format!("{what}-{k}-queries-");
            let mut files = HashMap::new();
            for (name, text) in read_zip(&path)? {
                let file = name.rsplit('/').next().unwrap_or(&name);
                if let Some(n) = file
                    .strip_prefix(&prefix)
                    .and_then(|f| f.strip_suffix(".txt"))
                    .and_then(|n| n.parse::<usize>().ok())
                {
                    files.insert(n, text);
                }
            }
            Ok(Some(files))
        };
        let (mat, ver, diff) = (read("mat")?, read("ver")?, read("diff")?);
        // the static core of version 1, by query: the official version 0 without the
        // reconstructed version 1
        let mut cores: HashMap<usize, Lines> = HashMap::new();
        let no_versions = HashMap::new();
        let versions0 = mat.as_ref().or(ver.as_ref()).unwrap_or(&no_versions);
        for (i, q) in queries.iter().enumerate().filter(|(_, q)| q.kind == kind) {
            let Some(text) = versions0.get(&q.number) else {
                continue;
            };
            let official = official_versions(text)?;
            let ours = mats.get(&(i, 1)).ok_or("no Mat solutions of version 1")?;
            let v0 = official.get(&0).cloned().unwrap_or_default();
            let core = minus(&v0, &official_form(kind, ours));
            report.official(&format!("{k} version 1 in v0"), core.is_some());
            if let Some(core) = core {
                cores.insert(i, core);
            }
        }
        // ours in the form of the official results, with the core, by query and version
        let full = |i: usize, rows: &Rows| {
            cores
                .get(&i)
                .map(|core| union(&official_form(kind, rows), core))
        };
        let empty = Lines::new();
        for (what, files) in [("mat", &mat), ("ver", &ver)] {
            let Some(files) = files else { continue };
            let parsed: HashMap<usize, BTreeMap<usize, Lines>> = files
                .iter()
                .map(|(&n, text)| Ok((n, official_versions(text)?)))
                .collect::<Result<_, String>>()?;
            // the versions the results cover: within them, a version without lines in a file
            // is one where that query has no solutions
            let Some(covered) = parsed
                .values()
                .filter_map(|versions| versions.keys().next_back().copied())
                .max()
            else {
                continue;
            };
            for (i, q) in queries.iter().enumerate().filter(|(_, q)| q.kind == kind) {
                let Some(official) = parsed.get(&q.number) else {
                    continue;
                };
                // ours by version: from Mat, or from the Ver solutions grouped by ?g
                let ours: BTreeMap<i64, Lines> = if what == "mat" {
                    plan.mat
                        .iter()
                        .filter_map(|&t| Some((t, full(i, mats.get(&(i, t))?)?)))
                        .collect()
                } else {
                    let Some(ver) = vers.get(&i) else { continue };
                    let gc = ver.column("g").unwrap();
                    let mut by_version: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
                    for row in &ver.rows {
                        let t: i64 = row[gc]
                            .trim_start_matches(&format!("<{ASOF_NS}"))
                            .trim_end_matches('>')
                            .parse()
                            .map_err(|_| format!("unexpected graph {}", row[gc]))?;
                        let mut row = row.clone();
                        row.remove(gc);
                        by_version.entry(t).or_default().push(row);
                    }
                    let mut variables = ver.variables.clone();
                    variables.remove(gc);
                    plan.mat
                        .iter()
                        .filter_map(|&t| {
                            let rows = Rows::new(
                                variables.clone(),
                                by_version.remove(&t).unwrap_or_default(),
                            );
                            Some((t, full(i, &rows)?))
                        })
                        .collect()
                };
                let mut previous: Option<(&Lines, &Lines)> = None;
                for (&t, lines) in &ours {
                    let v = (t - 1) as usize;
                    if v > covered {
                        previous = None;
                        continue;
                    }
                    let expected = official.get(&v).unwrap_or(&empty);
                    // version 1 agrees by construction (its static core is taken from it)
                    if t > 1 {
                        report.official(&format!("{k} {what} versions"), lines == expected);
                    }
                    // the change from the previous checked version, where either side changed
                    if let Some((prev, expected_prev)) = previous {
                        let ours_changed = changes(prev, lines);
                        let theirs_changed = changes(expected_prev, expected);
                        if !(unchanged(&ours_changed) && unchanged(&theirs_changed)) {
                            report.official(
                                &format!("{k} {what} changes"),
                                ours_changed == theirs_changed,
                            );
                        }
                    }
                    previous = Some((lines, expected));
                }
            }
        }
        let Some(diff) = diff else { continue };
        // the jumps of the official results (a jump without changes has no lines)
        let mut jumps = BTreeSet::new();
        let mut parsed = HashMap::new();
        for (&n, text) in &diff {
            let d = official_diffs(text)?;
            jumps.extend(d.keys().copied());
            parsed.insert(n, d);
        }
        for (i, q) in queries.iter().enumerate().filter(|(_, q)| q.kind == kind) {
            let Some(official) = parsed.get(&q.number) else {
                continue;
            };
            // without a static core (hour ?P?) compare without it: it is in both versions
            let full =
                |i: usize, rows: &Rows| full(i, rows).unwrap_or_else(|| official_form(kind, rows));
            let Some(first) = mats.get(&(i, 1)).map(|rows| full(i, rows)) else {
                continue;
            };
            for &jump in &jumps {
                // BEAR's version `jump` is time `jump + 1`; jumps past the archive are skipped
                let t = jump as i64 + 1;
                if t > last || !plan.diff.contains(&t) {
                    continue;
                }
                let Some(lines) = mats.get(&(i, t)).map(|rows| full(i, rows)) else {
                    continue;
                };
                let added: BTreeSet<String> = keys(&lines)
                    .difference(&keys(&first))
                    .map(|s| (*s).clone())
                    .collect();
                let deleted: BTreeSet<String> = keys(&first)
                    .difference(&keys(&lines))
                    .map(|s| (*s).clone())
                    .collect();
                let (e_added, e_deleted) = official.get(&jump).cloned().unwrap_or_default();
                report.official(
                    &format!("{k} diff jumps"),
                    added == e_added && deleted == e_deleted,
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Self-tests: the suite on a small archive written to a temporary directory
// ---------------------------------------------------------------------------------------------

/// A tar archive (ustar, with a GNU long name for names over 100 bytes) of `files`.
fn tar(files: &[(&str, &[u8])]) -> Vec<u8> {
    fn entry(out: &mut Vec<u8>, name: &str, data: &[u8], kind: u8) {
        let mut header = [0u8; 512];
        let short = &name.as_bytes()[..name.len().min(100)];
        header[..short.len()].copy_from_slice(short);
        header[100..108].copy_from_slice(b"0000644\0");
        header[124..136].copy_from_slice(format!("{:011o}\0", data.len()).as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0");
        header[156] = kind;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[148..156].copy_from_slice(b"        ");
        let sum: u32 = header.iter().map(|&b| b as u32).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(data);
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    let mut out = Vec::new();
    for (name, data) in files {
        if name.len() > 100 {
            let mut long = name.as_bytes().to_vec();
            long.push(0);
            entry(&mut out, "././@LongLink", &long, b'L');
        }
        entry(&mut out, name, data, b'0');
    }
    out.resize(out.len() + 1024, 0);
    out
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn write_zip(path: &Path, files: &[(&str, &str)]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut zip = zip::ZipWriter::new(fs::File::create(path).unwrap());
    for (name, text) in files {
        zip.start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(text.as_bytes()).unwrap();
    }
    zip.finish().unwrap();
}

const EX: &str = "http://ex/";

/// The deltas of the small archive, `(deleted, added)` from version 1 to 6. Version 1 (not in
/// the archive) has `<s1> <p> <o1>`, `<s1> <p> "héllo"@en`, `<s4> <type> <T>` and `<core> <p>
/// "kept"` (its static core, never in a delta).
const DELTAS: &[(&str, &str)] = &[
    // 1 -> 2: <s1> <p> <o1> goes; a literal with a line break arrives; two literals with one
    // canonical form (042 and 42) arrive for <s2> <num>
    (
        "<http://ex/s1> <http://ex/p> <http://ex/o1> .\n",
        "<http://ex/s2> <http://ex/p> \"a\\nb\" .\n\
         <http://ex/s2> <http://ex/num> \"042\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
         <http://ex/s2> <http://ex/num> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .\n\
         <http://ex/s3> <http://ex/num> \"7\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
         <http://ex/s1> <http://ex/type> <http://ex/T> .\n\
         <http://ex/o1> <http://ex/label> \"o one\" .\n",
    ),
    // 2 -> 3: <s1> <p> <o1> is back; "héllo" goes; one of the two numbers goes
    (
        "<http://ex/s1> <http://ex/p> \"h\u{e9}llo\"@en .\n\
         <http://ex/s2> <http://ex/num> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .\n",
        "<http://ex/s1> <http://ex/p> <http://ex/o1> .\n\
         <http://ex/s2> <http://ex/type> <http://ex/T> .\n",
    ),
    // 3 -> 4: a triple deleted and added by the same delta stays, also one that is not in the
    // versions so far (deleting it means it was in version 1)
    (
        "<http://ex/s2> <http://ex/p> \"a\\nb\" .\n\
         <http://ex/s4> <http://ex/type> <http://ex/T> .\n",
        "<http://ex/s2> <http://ex/p> \"a\\nb\" .\n\
         <http://ex/s4> <http://ex/type> <http://ex/T> .\n\
         <http://ex/s2> <http://ex/num> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .\n",
    ),
    // 4 -> 5
    (
        "<http://ex/s1> <http://ex/type> <http://ex/T> .\n",
        "<http://ex/s3> <http://ex/p> \"x\" .\n<http://ex/s3> <http://ex/label> \"three\" .\n",
    ),
    // 5 -> 6
    (
        "<http://ex/s3> <http://ex/p> \"x\" .\n<http://ex/o1> <http://ex/label> \"o one\" .\n",
        "",
    ),
];

/// BEAR's official `Mat` results of `?s <http://ex/p> ?o` in the small archive, with the static
/// core `<core> <p> "kept"`: a literal is its lexical form with non-ASCII characters as `?`.
const OFFICIAL_MAT: &str = "\
[Solution in version 0]<http://ex/core> kept
[Solution in version 0]<http://ex/s1> <http://ex/o1>
[Solution in version 0]<http://ex/s1> h?llo
[Solution in version 1]<http://ex/core> kept
[Solution in version 1]<http://ex/s1> h?llo
[Solution in version 1]<http://ex/s2> a
b
[Solution in version 2]<http://ex/core> kept
[Solution in version 2]<http://ex/s1> <http://ex/o1>
[Solution in version 2]<http://ex/s2> a
b
[Solution in version 3]<http://ex/core> kept
[Solution in version 3]<http://ex/s1> <http://ex/o1>
[Solution in version 3]<http://ex/s2> a
b
[Solution in version 4]<http://ex/s3> x
[Solution in version 4]<http://ex/core> kept
[Solution in version 4]<http://ex/s1> <http://ex/o1>
[Solution in version 4]<http://ex/s2> a
b
[Solution in version 5]<http://ex/core> kept
[Solution in version 5]<http://ex/s1> <http://ex/o1>
[Solution in version 5]<http://ex/s2> a
b
";

/// The official `Mat` results of `?s <http://ex/label> ?o`, which has no solutions in version
/// 0, and none in version 5 either, where the archive has `<s3> <label> "three"` (the official
/// results change without a delta): version 0 agrees, version 5 does not.
const OFFICIAL_MAT_LABEL: &str = "\
[Solution in version 1]<http://ex/o1> o one
[Solution in version 2]<http://ex/o1> o one
[Solution in version 3]<http://ex/o1> o one
[Solution in version 4]<http://ex/o1> o one
[Solution in version 4]<http://ex/s3> three
";

/// The official `Diff` results of `?s <http://ex/p> ?o`: jumps from version 0 to 2 and 5. Jump 9
/// is to a version the archive does not have, so it is not compared.
const OFFICIAL_DIFF: &str = "\
[ADD in jump 5]<http://ex/s2> a
b
[DEL in jump 5]<http://ex/s1> h?llo
[ADD in jump 2]<http://ex/s2> a
b
[DEL in jump 2]<http://ex/s1> h?llo
[ADD in jump 9]<http://ex/s9> not in the archive
[DEL in jump 9]<http://ex/s1> h?llo
";

/// Writes the small archive, its queries and official results as a BEAR-B directory.
fn small_bear_b(dir: &Path) {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for (i, (deleted, added)) in DELTAS.iter().enumerate() {
        let i = i + 1;
        files.push((
            format!("data-added_{i}-{}.nt.gz", i + 1),
            gzip(added.as_bytes()),
        ));
        // one long path, as GNU tar writes it
        let deleted_path = if i == 2 {
            format!("{}/data-deleted_{i}-{}.nt.gz", "d".repeat(120), i + 1)
        } else {
            format!("data-deleted_{i}-{}.nt.gz", i + 1)
        };
        files.push((deleted_path, gzip(deleted.as_bytes())));
    }
    files.push(("README".to_owned(), b"not a delta".to_vec()));
    let entries: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, data)| (name.as_str(), data.as_slice()))
        .collect();
    let archive = Granularity::Day.archive(dir);
    fs::create_dir_all(archive.parent().unwrap()).unwrap();
    fs::write(&archive, gzip(&tar(&entries))).unwrap();
    fs::create_dir_all(dir.join("Queries/p")).unwrap();
    fs::create_dir_all(dir.join("Queries/po")).unwrap();
    fs::write(
        dir.join("Queries/p/p.txt"),
        "?s <http://ex/p> ?o .\n?s <http://ex/num> ?o .\n\n?s <http://ex/label> ?o .\n",
    )
    .unwrap();
    fs::write(
        dir.join("Queries/po/po.txt"),
        "?s <http://ex/type> <http://ex/T> .\n?s <http://ex/type> <http://ex/Missing> .\n",
    )
    .unwrap();
    write_zip(
        &dir.join("Queries/joins.zip"),
        &[
            (
                "join1.txt",
                "{ \n?s <http://ex/p> ?o .\n?o <http://ex/label> ?y .\n}\n",
            ),
            (
                "join2.txt",
                "{ \n?s <http://ex/type> <http://ex/T> .\n?s <http://ex/p> ?o .\n}\n",
            ),
        ],
    );
    write_zip(
        &dir.join("results/day/p/mat-p-queries.zip"),
        &[
            ("mat-p-queries/mat-p-queries-1.txt", OFFICIAL_MAT),
            ("mat-p-queries/mat-p-queries-3.txt", OFFICIAL_MAT_LABEL),
        ],
    );
    write_zip(
        &dir.join("results/day/p/ver-p-queries.zip"),
        &[
            ("ver-p-queries/ver-p-queries-1.txt", OFFICIAL_MAT),
            ("ver-p-queries/ver-p-queries-3.txt", OFFICIAL_MAT_LABEL),
        ],
    );
    write_zip(
        &dir.join("results/day/p/diff-p-queries.zip"),
        &[("diff-p-queries/diff-p-queries-1.txt", OFFICIAL_DIFF)],
    );
}

/// The whole suite on the small archive: no problems, every check runs, and the official
/// results agree.
#[test]
fn suite_runs_on_a_small_archive() {
    let dir = tempfile::tempdir().unwrap();
    small_bear_b(dir.path());
    let plan = Plan::every(6, 1, 1, 1, 1);
    let pinned = [("day", "p mat versions", 9, 10)];
    let report = run_suite(dir.path(), Granularity::Day, &plan, &pinned).unwrap();
    println!("{report}");
    assert!(report.problems.is_empty(), "{report}");
    assert_eq!((report.versions, report.base, report.queries), (6, 3, 7));
    let count = |check: &str| report.checks.get(check).copied().unwrap_or_default();
    // 6 versions; 7 queries (3 ?P?, 2 ?PO, 2 joins), 5 of them lookups; 5 jumps
    assert_eq!(count("gate valid edges"), Count { pass: 6, fail: 0 });
    assert_eq!(count("gate to_rdf"), Count { pass: 6, fail: 0 });
    for (check, n) in [
        ("mat snapshot_at", 42),
        ("mat GRAPH asof", 42),
        ("mat native", 30),
        ("diff template", 35),
        ("diff set difference", 35),
        ("diff native", 25),
        ("ver VALUES", 7),
        ("ver FROM NAMED", 7),
        ("ver intervals", 5),
        ("ver native", 5),
    ] {
        assert_eq!(count(check), Count { pass: n, fail: 0 }, "{check}");
    }
    let official: Vec<(&str, (usize, usize))> = report
        .official
        .iter()
        .map(|(check, &counts)| (check.as_str(), counts))
        .collect();
    // two queries with official Mat and Ver results (`?s <p> ?o` and `?s <label> ?o`), one with
    // Diff results
    assert_eq!(
        official,
        [
            // jumps 2 and 5; jump 9 is past the last version
            ("p diff jumps", (2, 2)),
            // the pairs of consecutive versions where either side changed: 4 of `<p>`, and 3
            // of `<label>` (version 0 to 1 and 3 to 4 agree, 4 to 5 does not)
            ("p mat changes", (6, 7)),
            // versions 1 to 5: those of `<p>`, and those of `<label>` but version 5, where
            // the official result has no lines (and so no solutions)
            ("p mat versions", (9, 10)),
            ("p ver changes", (6, 7)),
            ("p ver versions", (9, 10)),
            ("p version 1 in v0", (2, 2)),
        ]
    );

    // a pinned count that changed is a problem
    let pinned = [("day", "p mat versions", 10, 10)];
    let report = run_suite(dir.path(), Granularity::Day, &plan, &pinned).unwrap();
    assert_eq!(report.problems.len(), 1, "{report}");
}

/// The default plans: every version of day with a Diff jump to each, and samples of hour and
/// instant.
#[test]
fn default_plans() {
    let day = Plan::sample(Granularity::Day);
    assert_eq!(day.mat, (1..=89).collect());
    assert_eq!(day.diff, (2..=89).collect());
    assert_eq!(day.export, day.mat);
    let hour = Plan::sample(Granularity::Hour);
    assert_eq!(hour.mat.len(), 14);
    assert_eq!(hour.diff, BTreeSet::from([301, 601, 901, 1201, 1299]));
    let instant = Plan::sample(Granularity::Instant);
    assert_eq!(instant.mat.len(), 6);
    assert_eq!(instant.diff, BTreeSet::from([10001, 20001, 21046]));
}

/// The archive is read and version 1 reconstructed: the triples deleted before being added.
#[test]
fn version_1_is_reconstructed() {
    let dir = tempfile::tempdir().unwrap();
    small_bear_b(dir.path());
    let changes = Changes::read(&Granularity::Day.archive(dir.path())).unwrap();
    assert_eq!(changes.versions(), 6);
    assert!(!changes.blank_nodes);
    let base: BTreeSet<String> = changes.base.iter().map(Triple::to_string).collect();
    assert_eq!(
        base,
        BTreeSet::from([
            "<http://ex/s1> <http://ex/p> <http://ex/o1>".to_owned(),
            "<http://ex/s1> <http://ex/p> \"h\u{e9}llo\"@en".to_owned(),
            "<http://ex/s4> <http://ex/type> <http://ex/T>".to_owned(),
        ])
    );
    assert_eq!(
        changes.delta_triples,
        DELTAS
            .iter()
            .map(|(d, a)| d.lines().count() + a.lines().count())
            .sum::<usize>()
    );

    // a missing delta is an error
    let tar_without = tar(&[(
        "data-added_1-2.nt",
        b"<http://ex/a> <http://ex/b> <http://ex/c> .\n",
    )]);
    assert!(bear::deltas_of_tar(&tar_without)
        .unwrap_err()
        .contains("from version 1 to 2"));
}

/// With blank nodes, `load` keeps their labels, so a later deletion matches.
#[test]
fn blank_nodes_keep_their_labels() {
    let deltas = vec![
        bear::Delta {
            deleted: Vec::new(),
            added: b"_:b1 <http://ex/p> <http://ex/o> .\n".to_vec(),
        },
        bear::Delta {
            deleted: b"_:b1 <http://ex/p> <http://ex/o> .\n".to_vec(),
            added: Vec::new(),
        },
    ];
    let changes = Changes::from_deltas(deltas).unwrap();
    assert!(changes.blank_nodes);
    let pg = PersistentGraph::new();
    bear::load(&pg, &changes).unwrap();
    let count = |t: i64| {
        let mut doc = Vec::new();
        pg.snapshot_at(t)
            .to_rdf(&mut doc, RdfFormat::NTriples)
            .unwrap()
            .triples
    };
    assert_eq!((count(1), count(2), count(3)), (0, 1, 0));
}

/// The checks fail when Raphtory and the oracle differ.
#[test]
fn checks_detect_differences() {
    let dir = tempfile::tempdir().unwrap();
    small_bear_b(dir.path());
    let changes = Changes::read(&Granularity::Day.archive(dir.path())).unwrap();
    let queries = read_queries(dir.path()).unwrap();
    let pg = PersistentGraph::new();
    bear::load(&pg, &changes).unwrap();
    // a triple the oracle does not have, asserted at 2 and retracted at 3
    let extra = Triple::new(
        NamedNode::new_unchecked(format!("{EX}s9")),
        NamedNode::new_unchecked(format!("{EX}p")),
        NamedNode::new_unchecked(format!("{EX}o9")),
    );
    use raphtory::rdf::RdfMutationOps;
    pg.add_triple(2, &extra).unwrap();
    pg.delete_triple(3, &extra).unwrap();
    let mut replay = Replay::new(&changes.base);
    let delta = &changes.deltas[0];
    replay.apply(
        &bear::parse(&delta.deleted).unwrap(),
        &bear::parse(&delta.added).unwrap(),
    );
    assert!(gate(&pg, &replay, 2).unwrap_err().contains("s9"));
    assert!(export_gate(&pg, &replay.triples(), 2)
        .unwrap_err()
        .contains("s9"));
    let p = &queries[0];
    let outcome = mat_checks(&pg, &replay.dataset, p, 2);
    assert!(outcome.expected.is_ok());
    for (check, result) in outcome.checks {
        assert!(result.unwrap_err().contains("s9"), "{check}");
    }
    let native = native_ver_check(&pg, &replay, p.lookup.as_ref().unwrap());
    assert!(native.unwrap_err().contains("s9"));
}

/// Validity runs: a retraction and an assertion at one time keep a run going, a retraction
/// with an identical event time wins, and a triple that was only retracted never holds.
#[test]
fn runs_follow_the_validity_rules() {
    use raphtory::api::core::storage::timeindex::EventTime;
    let e = |t: i64, id: usize, asserted: bool| (EventTime(t, id), asserted);
    assert_eq!(
        bear::runs(vec![
            e(1, 0, true),
            e(3, 1, false),
            e(3, 2, true),
            e(5, 3, false)
        ]),
        [(1, Some(5))]
    );
    assert_eq!(
        bear::runs(vec![
            e(1, 0, true),
            e(2, 1, true),
            e(4, 2, false),
            e(6, 3, true)
        ]),
        [(1, Some(4)), (6, None)]
    );
    assert_eq!(bear::runs(vec![e(2, 7, true), e(2, 7, false)]), []);
    assert_eq!(bear::runs(vec![e(2, 0, false)]), []);
    assert!(bear::holds_at(&[(1, Some(4)), (6, None)], 3));
    assert!(!bear::holds_at(&[(1, Some(4)), (6, None)], 4));
    assert!(bear::holds_at(&[(1, Some(4)), (6, None)], 100));
}

#[test]
fn lookups_are_parsed() {
    let p = Lookup::parse("?s <http://ex/p> ?o .").unwrap();
    assert_eq!((p.layer(), p.object.is_none()), ("http://ex/p", true));
    let po = Lookup::parse("?s <http://ex/p> <http://ex/o> .").unwrap();
    assert_eq!(po.object_name().as_deref(), Some("http://ex/o"));
    let literal = Lookup::parse("?s <http://ex/p> \"x y\"@en .").unwrap();
    assert_eq!(literal.object_name().as_deref(), Some("\"x y\"@en"));
    assert!(Lookup::parse("?x <http://ex/p> ?o .").is_err());
}

#[test]
fn official_results_are_parsed() {
    let versions = official_versions(OFFICIAL_MAT).unwrap();
    assert_eq!(versions.len(), 6);
    assert_eq!(versions[&1]["<http://ex/s2> a\nb"], 1);
    // a version without solutions has no lines
    let versions = official_versions(OFFICIAL_MAT_LABEL).unwrap();
    assert_eq!(versions.keys().copied().collect::<Vec<_>>(), [1, 2, 3, 4]);
    let diffs = official_diffs(OFFICIAL_DIFF).unwrap();
    assert_eq!(diffs.keys().copied().collect::<Vec<_>>(), [2, 5, 9]);
    assert_eq!(
        diffs[&5].1,
        BTreeSet::from(["<http://ex/s1> h?llo".to_owned()])
    );
    // the form of a solution: lexical forms, non-ASCII characters as `?`
    let row = vec![
        "\"2015\u{2013}16 Chelsea\"@en".to_owned(),
        "<http://ex/s>".to_owned(),
    ];
    assert_eq!(
        official_line(Kind::P, &row, 1, Some(0)),
        "<http://ex/s> 2015?16 Chelsea"
    );
}
