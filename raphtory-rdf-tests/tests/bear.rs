//! BEAR-B as a temporal correctness suite for Raphtory's RDF support: the versions of DBpedia
//! Live resources loaded into one `RdfGraph`, and BEAR's three kinds of versioned queries
//! (Fernández et al., "Evaluating query and storage strategies for RDF archives", SWJ 2019):
//!
//! - **Mat** (version materialisation, VM): the results of a query in version `k`;
//! - **Diff** (delta materialisation, DM): the results that differ between versions `i` and `j`;
//! - **Ver** (version queries, V): the results of a query in every version, with their version.
//!
//! # Running
//!
//! `make bear-b-fetch` downloads the data into `~/.cache/raphtory-rdf/bear-b` and
//! `make rust-test-rdf-bear` runs this file with `RAPHTORY_RDF_DATA` set, printing a report for
//! the day granularity. Without `RAPHTORY_RDF_DATA` the BEAR-B test prints a message and passes;
//! the self-test at the end, which runs the suite on a small in-memory archive, always runs.
//!
//! # The model
//!
//! Version `k` is time `k` (see the [`bear`] module, shared with
//! `rdf-bench/benches/temporal.rs`): each triple is an edge in the layer of its
//! predicate, a delta retracts its deleted triples and then asserts its added ones at the time of
//! its version, and "version `k`" is `rg.snapshot_at(k)` or the time graph `<raphtory:asof:k>`.
//! The archives do not hold version 1, so it is reconstructed from the deltas (see [`bear`]).
//!
//! # What is checked
//!
//! An *oracle* replays the same versions on an `oxrdf::Dataset` and evaluates queries with
//! spareval. Every check compares Raphtory with the oracle as multisets of solutions; any
//! difference is a bug in `raphtory::rdf`. Every version is checked.
//!
//! - **Sanity gate:** `rg.snapshot_at(k)` exported with `to_rdf` is version `k`.
//! - **Mat:** for each query `Q` (the `?s <p> ?o` and `?s <p> <o>` lookups and the two-pattern
//!   joins), `rg.snapshot_at(k).sparql(Q)` and `SELECT * { GRAPH <raphtory:asof:k> { Q } }` on
//!   `rg`.
//! - **Diff:** jumps from version 1 to later versions, with the template `{ GRAPH <asof:j> {Q}
//!   FILTER NOT EXISTS { GRAPH <asof:i> {Q} } BIND("+" AS ?op) } UNION { GRAPH <asof:i> {Q}
//!   FILTER NOT EXISTS { GRAPH <asof:j> {Q} } BIND("-" AS ?op) }`, which must give the set
//!   differences of the two Mat results.
//! - **Ver:** `SELECT * { VALUES ?g { <raphtory:asof:1> ... } GRAPH ?g { Q } }` must be every Mat
//!   result tagged with its version.
#[path = "common/bear.rs"]
mod bear;
#[path = "common/data.rs"]
mod data;
#[path = "common/oracle.rs"]
mod oracle;

use bear::{asof, bear_b_dir, read_queries, BearQuery, Changes, Delta, Granularity, Kind, Lookup};
use data::parse;
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::{
        model::{Dataset, GraphNameRef, Literal, QuadRef, Term, Triple},
        RdfFormat, SparqlResults,
    },
};
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};

/// Runs the suite on the day granularity of the downloaded data, prints the report and fails if
/// it has problems.
#[test]
fn bear_b_day() {
    let granularity = Granularity::Day;
    let dir = match bear_b_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping BEAR-B {}: {why}", granularity.name());
            return;
        }
    };
    let archive = granularity.archive(&dir);
    if !archive.is_file() {
        println!(
            "skipping BEAR-B {}: {} is missing (`make bear-b-fetch` downloads it)",
            granularity.name(),
            archive.display()
        );
        return;
    }
    let report = Changes::read(&archive)
        .and_then(|changes| run_suite(granularity.name(), &changes, &read_queries(&dir)?))
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
fn ours<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
) -> Result<Rows, String> {
    view.sparql(query)
        .map_err(|e| format!("Raphtory: {e}"))
        .and_then(Rows::from_results)
}

/// The oracle's solutions of a query on a dataset.
fn oracle(dataset: &Dataset, query: &str) -> Result<Rows, String> {
    oracle::evaluate(dataset, query)
        .map_err(|e| format!("oracle: {e}"))
        .and_then(Rows::from_results)
}

// ---------------------------------------------------------------------------------------------
// The oracle: the versions on an oxrdf::Dataset
// ---------------------------------------------------------------------------------------------

/// Replays the versions: the current version in the default graph of a dataset.
struct Replay {
    dataset: Dataset,
}

impl Replay {
    /// Version 1.
    fn new(base: &[Triple]) -> Self {
        let mut replay = Self {
            dataset: Dataset::new(),
        };
        for triple in base {
            replay.dataset.insert(Self::quad(triple));
        }
        replay
    }

    fn quad(triple: &Triple) -> QuadRef<'_> {
        QuadRef::new(
            &triple.subject,
            &triple.predicate,
            &triple.object,
            GraphNameRef::DefaultGraph,
        )
    }

    /// The next version: `(V \ deleted) ∪ added`.
    fn apply(&mut self, deleted: &[Triple], added: &[Triple]) {
        let added_set: HashSet<&Triple> = added.iter().collect();
        for triple in deleted {
            if !added_set.contains(triple) {
                self.dataset.remove(Self::quad(triple));
            }
        }
        for triple in added {
            self.dataset.insert(Self::quad(triple));
        }
    }

    /// The triples of the current version.
    fn triples(&self) -> HashSet<Triple> {
        self.dataset
            .iter()
            .map(|quad| Triple::new(quad.subject, quad.predicate, quad.object))
            .collect()
    }
}

// ---------------------------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------------------------

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
    /// Checks against the oracle, by name.
    checks: BTreeMap<String, Count>,
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
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "BEAR-B {}: {} versions, {} triples written ({} in the reconstructed version 1), {} queries",
            self.granularity, self.versions, self.triples, self.base, self.queries
        )?;
        writeln!(
            f,
            "  {:<28} {:>8} {:>8}",
            "check (vs the oracle)", "pass", "fail"
        )?;
        for (check, count) in &self.checks {
            writeln!(f, "  {check:<28} {:>8} {:>8}", count.pass, count.fail)?;
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

/// Runs the suite on the versions of an archive, at every version.
fn run_suite(name: &str, changes: &Changes, queries: &[BearQuery]) -> Result<Report, String> {
    let mut report = Report {
        granularity: name.to_owned(),
        versions: changes.versions(),
        triples: changes.triples(),
        base: changes.base.len(),
        queries: queries.len(),
        ..Report::default()
    };
    let last = changes.versions() as i64;

    let rg = RdfGraph::new();
    bear::load(&rg, changes).map_err(|e| format!("cannot load the versions: {e}"))?;
    // the versions to export, checked in parallel in batches of EXPORT_BATCH
    let mut exports: Vec<(i64, HashSet<Triple>)> = Vec::new();

    // the oracle's Mat solutions, by query and version
    let mut mats: HashMap<(usize, i64), Rows> = HashMap::new();
    let mut replay = Replay::new(&changes.base);
    for t in 1..=last {
        if t > 1 {
            let delta = &changes.deltas[(t - 2) as usize];
            let deleted = parse(&delta.deleted, RdfFormat::NTriples)?;
            let added = parse(&delta.added, RdfFormat::NTriples)?;
            replay.apply(&deleted, &added);
        }
        exports.push((t, replay.triples()));
        // a few at a time, so the versions waiting for their export stay few
        if exports.len() >= EXPORT_BATCH {
            export_gates(&rg, &mut exports, &mut report);
        }
        let outcomes: Vec<MatOutcome> = queries
            .par_iter()
            .map(|q| mat_checks(&rg, &replay.dataset, q, t))
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
        if t > 1 {
            let outcomes: Vec<Checks> = queries
                .par_iter()
                .enumerate()
                .map(|(i, q)| diff_checks(&rg, q, t, mats.get(&(i, 1)), mats.get(&(i, t))))
                .collect();
            for (q, checks) in queries.iter().zip(outcomes) {
                for (check, result) in checks {
                    report.record(check, format_args!("{q} 1->{t}"), result);
                }
            }
        }
    }
    export_gates(&rg, &mut exports, &mut report);

    let versions: Vec<i64> = (1..=last).collect();
    let outcomes: Vec<Checks> = queries
        .par_iter()
        .enumerate()
        .map(|(i, q)| ver_checks(&rg, q, i, &versions, &mats))
        .collect();
    for (q, checks) in queries.iter().zip(outcomes) {
        for (check, result) in checks {
            report.record(check, q, result);
        }
    }
    Ok(report)
}

/// How many versions [`export_gates`] checks at once.
const EXPORT_BATCH: usize = 16;

/// Runs the export gate on the waiting versions, in parallel, and empties the list.
fn export_gates(rg: &RdfGraph, exports: &mut Vec<(i64, HashSet<Triple>)>, report: &mut Report) {
    let outcomes: Vec<(i64, Result<(), String>)> = exports
        .par_iter()
        .map(|(t, expected)| (*t, export_gate(rg, expected, *t)))
        .collect();
    exports.clear();
    for (t, outcome) in outcomes {
        report.record("gate to_rdf", format!("t={t}"), outcome);
    }
}

/// The sanity gate on the export: `rg.snapshot_at(t)` exported with `to_rdf` is exactly version
/// `t` (`expected`).
fn export_gate(rg: &RdfGraph, expected: &HashSet<Triple>, t: i64) -> Result<(), String> {
    let mut doc = Vec::new();
    let stats = rg
        .snapshot_at(t)
        .to_rdf(&mut doc, RdfFormat::NTriples, &[])
        .map_err(|e| e.to_string())?;
    let ours: HashSet<Triple> = parse(&doc, RdfFormat::NTriples)?.into_iter().collect();
    if stats.skipped == 0 && ours.len() == stats.written && ours == *expected {
        return Ok(());
    }
    let missing: Vec<String> = expected.difference(&ours).map(Triple::to_string).collect();
    let extra: Vec<String> = ours.difference(expected).map(Triple::to_string).collect();
    Err(format!(
        "{} triples ({} skipped), expected {}; missing {}; extra {}",
        stats.written,
        stats.skipped,
        expected.len(),
        sample(&missing),
        sample(&extra)
    ))
}

/// Mat at version `t`: the oracle, the snapshot view and the time graph.
fn mat_checks(rg: &RdfGraph, dataset: &Dataset, q: &BearQuery, t: i64) -> MatOutcome {
    let expected = oracle(dataset, &q.mat());
    let check = |actual: Result<Rows, String>| -> Result<(), String> {
        match &expected {
            Ok(expected) => compare(expected, &actual?),
            Err(e) => Err(e.clone()),
        }
    };
    let checks = vec![
        ("mat snapshot_at", check(ours(&rg.snapshot_at(t), &q.mat()))),
        ("mat GRAPH asof", check(ours(rg, &q.mat_asof(t)))),
    ];
    MatOutcome { expected, checks }
}

/// Diff from version 1 to version `t`: the template on Raphtory against the set differences of
/// the oracle's Mat solutions.
fn diff_checks(
    rg: &RdfGraph,
    q: &BearQuery,
    t: i64,
    first: Option<&Rows>,
    last: Option<&Rows>,
) -> Checks {
    let (Some(first), Some(last)) = (first, last) else {
        return vec![(
            "diff set difference",
            Err("no oracle Mat solutions".to_owned()),
        )];
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
    vec![(
        "diff set difference",
        ours(rg, &q.diff(1, t)).and_then(|actual| compare(&expected, &actual)),
    )]
}

/// Ver over `versions` in the `VALUES` form.
fn ver_checks(
    rg: &RdfGraph,
    q: &BearQuery,
    i: usize,
    versions: &[i64],
    mats: &HashMap<(usize, i64), Rows>,
) -> Checks {
    // every Mat solution, tagged with its version
    let mut expected: Option<Rows> = None;
    for &t in versions {
        let Some(mat) = mats.get(&(i, t)) else {
            return vec![("ver VALUES", Err(format!("no oracle Mat solutions at {t}")))];
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
        return Vec::new();
    };
    expected.rows.sort_unstable();
    vec![(
        "ver VALUES",
        ours(rg, &q.ver_values(versions)).and_then(|actual| compare(&expected, &actual)),
    )]
}

// ---------------------------------------------------------------------------------------------
// Self-test: the suite on a small in-memory archive
// ---------------------------------------------------------------------------------------------

/// The deltas of the small archive, `(deleted, added)` from version 1 to 6. The reconstructed
/// version 1 has `<s1> <p> <o1>`, `<s1> <p> "héllo"@en` and `<s4> <type> <T>`.
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

/// The queries of the small archive: three `?P?` and two `?PO` lookups, and two joins.
fn small_queries() -> Vec<BearQuery> {
    let mut queries = Vec::new();
    for (kind, lines) in [
        (
            Kind::P,
            &[
                "?s <http://ex/p> ?o .",
                "?s <http://ex/num> ?o .",
                "?s <http://ex/label> ?o .",
            ][..],
        ),
        (
            Kind::Po,
            &[
                "?s <http://ex/type> <http://ex/T> .",
                "?s <http://ex/type> <http://ex/Missing> .",
            ][..],
        ),
    ] {
        for (i, line) in lines.iter().enumerate() {
            let lookup = Lookup::parse(line).unwrap();
            queries.push(BearQuery {
                kind,
                number: i + 1,
                pattern: lookup.pattern.clone(),
                lookup: Some(lookup),
            });
        }
    }
    for (i, pattern) in [
        "?s <http://ex/p> ?o .\n?o <http://ex/label> ?y .",
        "?s <http://ex/type> <http://ex/T> .\n?s <http://ex/p> ?o .",
    ]
    .into_iter()
    .enumerate()
    {
        queries.push(BearQuery {
            kind: Kind::Join,
            number: i + 1,
            pattern: pattern.to_owned(),
            lookup: None,
        });
    }
    queries
}

/// The whole suite on the small archive: no problems, and every check runs.
#[test]
fn suite_runs_on_a_small_archive() {
    let deltas = DELTAS
        .iter()
        .map(|(deleted, added)| Delta {
            deleted: deleted.as_bytes().to_vec(),
            added: added.as_bytes().to_vec(),
        })
        .collect();
    let changes = Changes::from_deltas(deltas).unwrap();
    let report = run_suite("small", &changes, &small_queries()).unwrap();
    println!("{report}");
    assert!(report.problems.is_empty(), "{report}");
    assert_eq!((report.versions, report.base, report.queries), (6, 3, 7));
    let count = |check: &str| report.checks.get(check).copied().unwrap_or_default();
    // 6 versions; 7 queries (3 ?P?, 2 ?PO, 2 joins); 5 jumps
    for (check, n) in [
        ("gate to_rdf", 6),
        ("mat snapshot_at", 42),
        ("mat GRAPH asof", 42),
        ("diff set difference", 35),
        ("ver VALUES", 7),
    ] {
        assert_eq!(count(check), Count { pass: n, fail: 0 }, "{check}");
    }
}
