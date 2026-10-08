//! The Berlin SPARQL Benchmark (BSBM) and the Sparqloscope feature queries as a correctness
//! suite for Raphtory's SPARQL: every query is run on a `PersistentGraph` loaded with `load_rdf`
//! and on an `oxrdf::Dataset` holding the same triples, evaluated by spareval (the evaluator
//! Raphtory uses), and the two results must agree. Any difference is a bug in `raphtory::rdf`.
//!
//! # Running
//!
//! The data is not part of the repository (see the [`bsbm`] module). `make rust-test-rdf-bsbm`
//! runs this file with `RAPHTORY_RDF_DATA` set and prints a report per scale. Without
//! `RAPHTORY_RDF_DATA` the BSBM tests print a message and pass; the self-tests at the end, which
//! run the suite on a small generated BSBM, always run.
//!
//! # What is checked
//!
//! **1,000 products** ([`bsbm_1000`]): `dataset-1000` written at time 1, then update `k` of
//! `exploreAndUpdate-1000` at time `k + 2` (`INSERT DATA` as `load_rdf`, `DELETE WHERE
//! { <offer> ?p ?o }` as retractions).
//!
//! - **Temporal workload:** the explore-and-update mix is replayed in order; the oracle applies
//!   each update as it comes, and each query is checked on `pg.snapshot_at(1 + u)`, where `u` is
//!   the number of updates before it.
//! - **Final state:** every distinct query of the explore and business intelligence mixes and
//!   every Sparqloscope query, on `pg` after all updates (only 3 queries of the slow template
//!   [`SLOW_BI`]).
//!
//! **5,000 products** ([`bsbm_5000`]): `dataset-5000` loaded at time 1, checked with
//! [`Plan::sample`]; `RAPHTORY_BSBM_FULL=1` checks every distinct query (slow).
//!
//! # How results are compared
//!
//! `SELECT` results are compared as multisets of rows (blank nodes shown as `_:`, columns sorted
//! by name); `ASK` as booleans; `CONSTRUCT` and `DESCRIBE` as sets of triples. An error must be
//! an error on both sides. Under a top-level `ORDER BY` the rows must also come in the same
//! sequence, up to ties, and a `LIMIT`/`OFFSET` may cut through a group of ties. When rows differ
//! in either way, the query is rewritten ([`keyed`]) to also project its `ORDER BY` keys and
//! [`compare_keyed`] checks Raphtory's keys follow the oracle's sequence, allowing any oracle row
//! within a cut tie group; [`follows_keys`] checks `sparql()`'s own rows follow those keys. The
//! rewritten query is evaluated from its algebra, on Raphtory through `RaphtoryDataset`.
//!
//! The oracle's row order varies between runs (`oxrdf::Dataset` hashes with a per-process
//! `RandomState`), so the "same up to ties" and "rounding" counts vary slightly.
#[path = "common/bsbm.rs"]
mod bsbm;

use bsbm::{
    apply_dataset, bsbm_dir, distinct_queries, load_dataset, load_raphtory, read_sparqloscope,
    short, templates, update_time, Bsbm, Entry, Op, BASE_TIME,
};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::{
        evaluator,
        model::{Dataset, Term, Variable},
        RaphtoryDataset, RdfViewOps, SparqlResults,
    },
};
use rayon::prelude::*;
use spargebra::{
    algebra::{GraphPattern, OrderExpression},
    Query, SparqlParser,
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::Path,
    time::Instant,
};

/// Checks every distinct query of the mixes on the 5,000-product data (slow) when set to
/// anything but `0`.
const FULL_ENV: &str = "RAPHTORY_BSBM_FULL";

/// The prefix of the variables that [`keyed`] adds for the `ORDER BY` keys.
const KEY_PREFIX: &str = "bsbm_order_key_";

/// The stack of the threads that run queries.
const STACK: usize = 64 << 20;

#[test]
fn bsbm_1000() {
    bsbm_scale(1000);
}

#[test]
fn bsbm_5000() {
    bsbm_scale(5000);
}

/// Runs the suite on one scale of the downloaded data, prints the report and fails if any
/// query differs.
fn bsbm_scale(products: usize) {
    let dir = match bsbm_dir() {
        Ok(dir) => dir,
        Err(why) => {
            println!("skipping BSBM {products}: {why}");
            return;
        }
    };
    if !Bsbm::file(&dir, "dataset", products, "nt.bz2").is_file() {
        println!("skipping BSBM {products}: dataset-{products}.nt.bz2 is missing");
        return;
    }
    let plan = if products <= 1000 || is_full() {
        Plan::full()
    } else {
        Plan::sample()
    };
    let reports =
        run_scale(&dir, products, &plan).unwrap_or_else(|e| panic!("BSBM {products}: {e}"));
    let mut problems = Vec::new();
    for report in &reports {
        println!("{report}");
        problems.extend(
            report
                .differ
                .iter()
                .map(|d| format!("{}: {d}", report.name)),
        );
    }
    assert!(
        problems.is_empty(),
        "{} difference(s) in BSBM {products}:\n{}",
        problems.len(),
        problems.join("\n")
    );
}

fn is_full() -> bool {
    std::env::var(FULL_ENV).is_ok_and(|v| !v.is_empty() && v != "0")
}

/// Template 7 of the business intelligence mix (`FILTER NOT EXISTS` over a sub-`SELECT` with
/// `LIMIT 1000`), which is slow on every engine.
const SLOW_BI: u32 = 7;

/// A Sparqloscope query that is very slow on the oracle at 5,000 products.
const SLOW_SPARQLOSCOPE: &str =
    "EXISTS JOIN chain of three large predicates with the largest sum of join sizes 2";

/// How many distinct queries of each template to check on the final state.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Plan {
    /// Of each explore template (`None`: all).
    explore: Option<usize>,
    /// Of each business intelligence template (`None`: all), except those of `bi_caps`.
    bi: Option<usize>,
    /// `(template, at most)` for some business intelligence templates.
    bi_caps: Vec<(u32, usize)>,
    /// The descriptions of the Sparqloscope queries not to check.
    sparqloscope_skip: Vec<&'static str>,
}

impl Plan {
    /// Every distinct query, but only 3 of [`SLOW_BI`].
    fn full() -> Self {
        Self {
            explore: None,
            bi: None,
            bi_caps: vec![(SLOW_BI, 3)],
            sparqloscope_skip: vec![],
        }
    }

    /// Every distinct query of the explore mix, the first 5 of each business intelligence
    /// template, without [`SLOW_BI`], and every Sparqloscope query but [`SLOW_SPARQLOSCOPE`].
    fn sample() -> Self {
        Self {
            explore: None,
            bi: Some(5),
            bi_caps: vec![(SLOW_BI, 0)],
            sparqloscope_skip: vec![SLOW_SPARQLOSCOPE],
        }
    }

    /// The first distinct queries of every template of the explore mix.
    fn explore<'a>(&self, entries: &'a [Entry]) -> Vec<(u32, &'a str)> {
        pick(entries, self.explore, &[])
    }

    /// The first distinct queries of every template of the business intelligence mix.
    fn bi<'a>(&self, entries: &'a [Entry]) -> Vec<(u32, &'a str)> {
        pick(entries, self.bi, &self.bi_caps)
    }

    /// The business intelligence templates this plan does not check.
    fn skipped_bi(&self) -> Vec<u32> {
        self.bi_caps
            .iter()
            .filter(|(_, n)| *n == 0)
            .map(|(t, _)| *t)
            .collect()
    }
}

/// The first `limit` distinct queries of every template (all with `None`), or fewer for the
/// templates of `caps`.
fn pick<'a>(
    entries: &'a [Entry],
    limit: Option<usize>,
    caps: &[(u32, usize)],
) -> Vec<(u32, &'a str)> {
    let mut taken: HashMap<u32, usize> = HashMap::new();
    distinct_queries(entries)
        .into_iter()
        .filter(|(t, _)| {
            let cap = caps
                .iter()
                .find(|(c, _)| c == t)
                .map(|(_, n)| *n)
                .or(limit)
                .unwrap_or(usize::MAX);
            let k = taken.entry(*t).or_default();
            *k += 1;
            *k <= cap
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------------------------

/// A row: its values as N-Triples terms (`UNDEF` where unbound, `_:` for a blank node).
type Row = Vec<String>;

/// Query results in a form that can be compared.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Normal {
    /// Solutions with their variable names sorted, rows in result order with values in the
    /// order of the names.
    Rows {
        variables: Vec<String>,
        rows: Vec<Row>,
    },
    Boolean(bool),
    /// Triples, sorted.
    Graph(Vec<String>),
}

fn value(term: &Option<Term>) -> String {
    match term {
        None => "UNDEF".to_owned(),
        Some(Term::BlankNode(_)) => "_:".to_owned(),
        Some(term) => term.to_string(),
    }
}

impl Normal {
    fn of(results: SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => {
                let mut order: Vec<usize> = (0..variables.len()).collect();
                order.sort_by(|&a, &b| variables[a].as_str().cmp(variables[b].as_str()));
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
                let mut triples: Vec<String> = triples
                    .iter()
                    .map(|t| {
                        let s = if t.subject.is_blank_node() {
                            "_:".to_owned()
                        } else {
                            t.subject.to_string()
                        };
                        format!("{s} {} {}", t.predicate, value(&Some(t.object.clone())))
                    })
                    .collect();
                triples.sort_unstable();
                Normal::Graph(triples)
            }
        }
    }
}

fn sorted(rows: &[Row]) -> Vec<Row> {
    let mut rows = rows.to_vec();
    rows.sort_unstable();
    rows
}

/// The rows of `x` that are not in `y`, as multisets, for messages.
fn only(x: &[Row], y: &[Row]) -> String {
    let mut left: HashMap<&Row, usize> = HashMap::new();
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
    let shown: String = rows
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ")
        .chars()
        .take(500)
        .collect();
    if rows.len() > 3 {
        format!("[{shown} | ... {} more]", rows.len() - 3)
    } else {
        format!("[{shown}]")
    }
}

// ---------------------------------------------------------------------------------------------
// Running queries
// ---------------------------------------------------------------------------------------------

/// Raphtory's results of a query text, through `sparql()`.
fn ours<G: RdfViewOps>(view: &G, query: &str) -> Result<Normal, String> {
    view.sparql(query)
        .map(Normal::of)
        .map_err(|e| e.to_string())
}

/// Raphtory's results of a query algebra, through `RaphtoryDataset` (what `sparql()` queries).
fn ours_algebra<G: StaticGraphViewOps + IntoDynamic>(
    view: &G,
    query: &Query,
) -> Result<Normal, String> {
    let results = evaluator()
        .for_query(query.clone())
        .on_queryable_dataset(RaphtoryDataset::new(view.clone()))
        .execute()
        .map_err(|e| e.to_string())?;
    SparqlResults::from_query_results(results)
        .map(Normal::of)
        .map_err(|e| e.to_string())
}

/// The oracle's results of a query text.
fn oracle(dataset: &Dataset, query: &str) -> Result<Normal, String> {
    let query = evaluator()
        .parse_query(query)
        .map_err(|e| format!("syntax: {e}"))?;
    let results = query
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(|e| e.to_string())?;
    SparqlResults::from_query_results(results)
        .map(Normal::of)
        .map_err(|e| e.to_string())
}

/// The oracle's results of a query algebra.
fn oracle_algebra(dataset: &Dataset, query: &Query) -> Result<Normal, String> {
    let results = evaluator()
        .for_query(query.clone())
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(|e| e.to_string())?;
    SparqlResults::from_query_results(results)
        .map(Normal::of)
        .map_err(|e| e.to_string())
}

/// The outcome of one query.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Verdict {
    /// The same results (in the same sequence for an `ORDER BY`).
    Same,
    /// The same, except that `xsd:float` or `xsd:double` values differ in their last digits
    /// (see [`cells_match`]).
    Rounding,
    /// The same up to the order of ties, or up to which tied rows a `LIMIT` or `OFFSET` keeps.
    Ties,
    /// An error on both sides (the message is Raphtory's).
    BothFail(String),
    /// Different results.
    Differ(String),
}

/// Runs a query on Raphtory and on the oracle and compares the results.
fn check<G: RdfViewOps>(view: &G, dataset: &Dataset, query: &str) -> Verdict {
    let sampled = sampled_variables(query);
    let ours = ours(view, query).map(|r| mask(r, &sampled));
    let theirs = oracle(dataset, query).map(|r| mask(r, &sampled));
    match (ours, theirs) {
        (Err(a), Err(_)) => Verdict::BothFail(a),
        (Err(a), Ok(_)) => Verdict::Differ(format!("only Raphtory fails: {a}")),
        (Ok(_), Err(b)) => Verdict::Differ(format!("only the oracle fails: {b}")),
        (Ok(a), Ok(b)) => compare(view, dataset, query, &sampled, &a, &b),
    }
}

/// The variables a query binds to `SAMPLE(..)`, whose value is any value of the group, so they
/// are not compared (`(SAMPLE(?x) AS ?v)`, read from the text).
fn sampled_variables(query: &str) -> Vec<String> {
    let lower = query.to_ascii_lowercase();
    let mut variables = Vec::new();
    let mut from = 0;
    while let Some(at) = lower[from..].find("sample") {
        let rest = &lower[from + at + "sample".len()..];
        from += at + "sample".len();
        if !rest.trim_start().starts_with('(') {
            continue;
        }
        let Some(close) = rest.find(')') else { break };
        let after = rest[close + 1..].trim_start();
        if let Some(name) = after.strip_prefix("as").map(str::trim_start) {
            if let Some(name) = name.strip_prefix('?').or_else(|| name.strip_prefix('$')) {
                let start = query.len() - name.len();
                let len = name
                    .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .unwrap_or(name.len());
                variables.push(query[start..start + len].to_owned());
            }
        }
    }
    variables
}

/// Replaces the values of the given variables with `SAMPLE`.
fn mask(results: Normal, sampled: &[String]) -> Normal {
    match results {
        Normal::Rows { variables, rows } if !sampled.is_empty() => {
            let masked: Vec<bool> = variables.iter().map(|v| sampled.contains(v)).collect();
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .zip(&masked)
                        .map(|(value, &m)| if m { "SAMPLE".to_owned() } else { value })
                        .collect()
                })
                .collect();
            Normal::Rows { variables, rows }
        }
        other => other,
    }
}

/// The value of an `xsd:float` or `xsd:double` literal, with its datatype.
fn floating(cell: &str) -> Option<(&str, f64)> {
    let (lexical, datatype) = cell.strip_prefix('"')?.rsplit_once("\"^^")?;
    if datatype == "<http://www.w3.org/2001/XMLSchema#float>"
        || datatype == "<http://www.w3.org/2001/XMLSchema#double>"
    {
        Some((datatype, lexical.parse().ok()?))
    } else {
        None
    }
}

/// Whether two values match: they are equal, or both `xsd:float` (or both `xsd:double`) within a
/// relative 1e-5. Aggregates such as `AVG` and `SUM` add floating-point values in the order the
/// solutions come, and that order depends on the storage, so their last digits can differ.
fn cells_match(a: &str, b: &str) -> bool {
    a == b
        || match (floating(a), floating(b)) {
            (Some((ta, x)), Some((tb, y))) => {
                ta == tb && (x == y || (x - y).abs() <= 1e-5 * x.abs().max(y.abs()))
            }
            _ => false,
        }
}

fn rows_match(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cells_match(x, y))
}

/// A row with its floating-point values replaced by their datatype, for grouping.
fn masked(row: &[String]) -> Row {
    row.iter()
        .map(|cell| match floating(cell) {
            Some((datatype, _)) => format!("~{datatype}"),
            None => cell.clone(),
        })
        .collect()
}

/// Whether every row of `a` matches a different row of `b` ([`rows_match`]); with `exact`, also
/// the other way around (the same multiset). Greedy within the rows that are equal but for their
/// floating-point values.
fn rows_within(a: &[Row], b: &[Row], exact: bool) -> bool {
    if exact && a.len() != b.len() {
        return false;
    }
    let mut groups: HashMap<Row, Vec<(&Row, bool)>> = HashMap::new();
    for row in b {
        groups.entry(masked(row)).or_default().push((row, false));
    }
    a.iter().all(|row| {
        groups.get_mut(&masked(row)).is_some_and(|candidates| {
            candidates
                .iter_mut()
                .find(|(c, used)| !used && rows_match(row, c))
                .map(|(_, used)| *used = true)
                .is_some()
        })
    })
}

fn sequences_match(a: &[Row], b: &[Row]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| rows_match(x, y))
}

fn compare<G: RdfViewOps>(
    view: &G,
    dataset: &Dataset,
    query: &str,
    sampled: &[String],
    ours: &Normal,
    theirs: &Normal,
) -> Verdict {
    match (ours, theirs) {
        (
            Normal::Rows {
                variables: va,
                rows: a,
            },
            Normal::Rows {
                variables: vb,
                rows: b,
            },
        ) => {
            if va != vb {
                return Verdict::Differ(format!("variables {va:?} != oracle {vb:?}"));
            }
            let Ok(parsed) = SparqlParser::new().parse_query(query) else {
                return Verdict::Differ("the query does not parse with spargebra".to_owned());
            };
            let shape = Shape::of(&parsed);
            if sorted(a) == sorted(b) && (!shape.ordered || a == b) {
                return Verdict::Same;
            }
            let same_rows = rows_within(a, b, true);
            if same_rows && (!shape.ordered || sequences_match(a, b)) {
                return Verdict::Rounding;
            }
            if !same_rows && !shape.sliced {
                return Verdict::Differ(format!(
                    "{} rows, oracle {}; only Raphtory: {}; only oracle: {}",
                    a.len(),
                    b.len(),
                    only(a, b),
                    only(b, a)
                ));
            }
            // the order differs, or a slice may cut through ties: compare with the keys
            let Some(keyed) = keyed(&parsed) else {
                return Verdict::Differ("rows differ and the query cannot be keyed".to_owned());
            };
            let result = ours_algebra(view, &keyed.sliced)
                .and_then(|o| oracle_algebra(dataset, &keyed.full).map(|f| (o, f)))
                .and_then(|(o, f)| {
                    let o = mask(o, sampled);
                    let f = mask(f, sampled);
                    let (ok, of) = (split_keys(&o)?, split_keys(&f)?);
                    // the keyed query must give Raphtory's own rows
                    let projected: Vec<Row> = ok.iter().map(|(_, v)| v.clone()).collect();
                    if !rows_within(&projected, a, true) {
                        return Err(
                            "the keyed query gives Raphtory other rows than the query".to_owned()
                        );
                    }
                    // `sparql()`'s rows must come in the sequence of the checked keys
                    follows_keys(a, &ok)?;
                    compare_keyed(&ok, &of, keyed.start, keyed.length)
                });
            match result {
                Ok(()) => Verdict::Ties,
                Err(e) => Verdict::Differ(e),
            }
        }
        (a, b) if a == b => Verdict::Same,
        (Normal::Graph(a), Normal::Graph(b)) => {
            let a: Vec<Row> = a.iter().map(|t| vec![t.clone()]).collect();
            let b: Vec<Row> = b.iter().map(|t| vec![t.clone()]).collect();
            Verdict::Differ(format!(
                "{} triples, oracle {}; only Raphtory: {}; only oracle: {}",
                a.len(),
                b.len(),
                only(&a, &b),
                only(&b, &a)
            ))
        }
        (a, b) => Verdict::Differ(format!("{a:?} != oracle {b:?}")),
    }
}

// ---------------------------------------------------------------------------------------------
// ORDER BY and slices
// ---------------------------------------------------------------------------------------------

/// What the top of a `SELECT` query does to the order of its rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Shape {
    /// It has a top-level `ORDER BY`.
    ordered: bool,
    /// It has a top-level `LIMIT` or `OFFSET`.
    sliced: bool,
}

impl Shape {
    fn of(query: &Query) -> Self {
        let Query::Select { pattern, .. } = query else {
            return Self::default();
        };
        let mut shape = Self::default();
        let mut pattern = pattern;
        let mut projected = false;
        loop {
            match pattern {
                GraphPattern::Slice { inner, .. } => {
                    shape.sliced = true;
                    pattern = inner;
                }
                GraphPattern::Distinct { inner } | GraphPattern::Reduced { inner } => {
                    pattern = inner
                }
                // a second projection is a sub-SELECT, whose order is not the result's
                GraphPattern::Project { inner, .. } if !projected => {
                    projected = true;
                    pattern = inner;
                }
                GraphPattern::OrderBy { .. } => {
                    shape.ordered = true;
                    return shape;
                }
                _ => return shape,
            }
        }
    }
}

/// A `SELECT` query rewritten to also project its `ORDER BY` keys.
#[derive(Clone, Debug)]
struct Keyed {
    /// With the slice of the query.
    sliced: Query,
    /// Without the slice.
    full: Query,
    start: usize,
    length: Option<usize>,
}

/// Rewrites `SELECT .. ORDER BY e1 e2 .. LIMIT l OFFSET o` into the same query that also
/// projects `?bsbm_order_key_i` bound to `ei` (an `Extend` below the `OrderBy`, which keeps the
/// order), with and without its slice. A query without a top-level `ORDER BY` gets no keys, so
/// every row has the same (empty) key. `None` if the query is not a `SELECT` with a projection.
fn keyed(query: &Query) -> Option<Keyed> {
    let Query::Select {
        dataset,
        pattern,
        base_iri,
    } = query
    else {
        return None;
    };
    let (start, length, pattern) = match pattern {
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => (*start, *length, &**inner),
        p => (0, None, p),
    };
    let (distinct, reduced, pattern) = match pattern {
        GraphPattern::Distinct { inner } => (true, false, &**inner),
        GraphPattern::Reduced { inner } => (false, true, &**inner),
        p => (false, false, p),
    };
    let GraphPattern::Project { inner, variables } = pattern else {
        return None;
    };
    let mut projected = variables.clone();
    let body = match &**inner {
        GraphPattern::OrderBy { inner, expression } => {
            let mut body = (**inner).clone();
            for (i, order) in expression.iter().enumerate() {
                let (OrderExpression::Asc(e) | OrderExpression::Desc(e)) = order;
                let variable = Variable::new(format!("{KEY_PREFIX}{i}")).ok()?;
                body = GraphPattern::Extend {
                    inner: Box::new(body),
                    variable: variable.clone(),
                    expression: e.clone(),
                };
                projected.push(variable);
            }
            GraphPattern::OrderBy {
                inner: Box::new(body),
                expression: expression.clone(),
            }
        }
        other => other.clone(),
    };
    let mut full = GraphPattern::Project {
        inner: Box::new(body),
        variables: projected,
    };
    if distinct {
        full = GraphPattern::Distinct {
            inner: Box::new(full),
        };
    }
    if reduced {
        full = GraphPattern::Reduced {
            inner: Box::new(full),
        };
    }
    let sliced = if start > 0 || length.is_some() {
        GraphPattern::Slice {
            inner: Box::new(full.clone()),
            start,
            length,
        }
    } else {
        full.clone()
    };
    let select = |pattern| Query::Select {
        dataset: dataset.clone(),
        pattern,
        base_iri: base_iri.clone(),
    };
    Some(Keyed {
        sliced: select(sliced),
        full: select(full),
        start,
        length,
    })
}

/// A row split into its `ORDER BY` keys and its other values.
type KeyedRow = (Row, Row);

/// Splits keyed results into keys and values (both in the order of the sorted names).
fn split_keys(results: &Normal) -> Result<Vec<KeyedRow>, String> {
    let Normal::Rows { variables, rows } = results else {
        return Err("the keyed query gives no solutions".to_owned());
    };
    let is_key: Vec<bool> = variables
        .iter()
        .map(|v| v.starts_with(KEY_PREFIX))
        .collect();
    Ok(rows
        .iter()
        .map(|row| {
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for (value, &k) in row.iter().zip(&is_key) {
                if k {
                    keys.push(value.clone());
                } else {
                    values.push(value.clone());
                }
            }
            (keys, values)
        })
        .collect())
}

/// Checks that `rows` (the rows of a query) come in the sequence of `keyed` (the rows of the same
/// query that also projects its `ORDER BY` keys): for every maximal run of rows with equal keys in
/// `keyed`, the rows at the same positions of `rows` are the values of that run, in any order.
fn follows_keys(rows: &[Row], keyed: &[KeyedRow]) -> Result<(), String> {
    if rows.len() != keyed.len() {
        return Err(format!(
            "{} rows, the keyed query gives {}",
            rows.len(),
            keyed.len()
        ));
    }
    let mut begin = 0;
    while begin < keyed.len() {
        let end = keyed[begin..]
            .iter()
            .position(|(key, _)| *key != keyed[begin].0)
            .map_or(keyed.len(), |n| begin + n);
        let values: Vec<Row> = keyed[begin..end].iter().map(|(_, v)| v.clone()).collect();
        if !rows_within(&rows[begin..end], &values, true) {
            return Err(format!(
                "the rows are not in ORDER BY sequence: rows {begin} to {} should have the key \
                 {:?}; only in the rows: {}; only with the key: {}",
                end - 1,
                keyed[begin].0,
                only(&rows[begin..end], &values),
                only(&values, &rows[begin..end])
            ));
        }
        begin = end;
    }
    Ok(())
}

/// Checks Raphtory's keyed rows (`ours`, sliced) against the oracle's keyed rows without the
/// slice (`full`, in order): Raphtory's keys must be the keys of the oracle's slice, in the same
/// sequence, and its rows the rows of that slice, except in a group of ties that the slice cuts
/// (the key of its first row when it starts after row 0, the key of its last row when it ends
/// before the end), where Raphtory may keep any of the oracle's rows with that key. Values are
/// compared with [`cells_match`].
fn compare_keyed(
    ours: &[KeyedRow],
    full: &[KeyedRow],
    start: usize,
    length: Option<usize>,
) -> Result<(), String> {
    let begin = start.min(full.len());
    let end = length.map_or(full.len(), |l| (begin + l).min(full.len()));
    let expected = &full[begin..end];
    if ours.len() != expected.len() {
        return Err(format!(
            "{} rows, the oracle's slice has {}",
            ours.len(),
            expected.len()
        ));
    }
    if let Some(at) = ours
        .iter()
        .zip(expected)
        .position(|((a, _), (b, _))| !rows_match(a, b))
    {
        return Err(format!(
            "ORDER BY keys differ at row {at}: {:?}, oracle {:?}",
            ours[at].0, expected[at].0
        ));
    }
    // the keys of the groups of ties that the slice cuts
    let mut cut: Vec<&Row> = Vec::new();
    if let (true, Some((k, _))) = (begin > 0, expected.first()) {
        cut.push(k);
    }
    if let (true, Some((k, _))) = (end < full.len(), expected.last()) {
        cut.push(k);
    }
    // the keys match position by position, so the oracle's keys tell which rows are cut
    let whole = |row: &KeyedRow| [row.0.clone(), row.1.clone()].concat();
    let (mut a, mut b) = (Vec::new(), Vec::new());
    let mut tied: HashMap<&Row, Vec<Row>> = HashMap::new();
    for (o, e) in ours.iter().zip(expected) {
        match cut.iter().find(|k| ***k == e.0) {
            Some(k) => tied.entry(k).or_default().push(whole(o)),
            None => {
                a.push(whole(o));
                b.push(whole(e));
            }
        }
    }
    if !rows_within(&a, &b, true) {
        return Err(format!(
            "rows differ outside the ties at the slice; only Raphtory: {}; only oracle: {}",
            only(&a, &b),
            only(&b, &a)
        ));
    }
    for (key, rows) in tied {
        let candidates: Vec<Row> = full
            .iter()
            .filter(|(k, _)| rows_match(k, key))
            .map(whole)
            .collect();
        if !rows_within(&rows, &candidates, false) {
            return Err(format!(
                "rows tied on {key:?} that the oracle does not have: {}",
                only(&rows, &candidates)
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------------------------

/// The outcome of one part of the suite.
#[derive(Debug, Default)]
struct Report {
    name: String,
    checked: usize,
    same: usize,
    rounding: usize,
    ties: usize,
    /// Queries that fail on both sides.
    both_fail: Vec<String>,
    /// Queries whose results differ.
    differ: Vec<String>,
    seconds: f64,
    /// The slowest queries: (seconds for both sides, what).
    slowest: Vec<(f64, String)>,
}

impl Report {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    fn record(&mut self, what: &str, verdict: Verdict) {
        self.checked += 1;
        match verdict {
            Verdict::Same => self.same += 1,
            Verdict::Rounding => self.rounding += 1,
            Verdict::Ties => self.ties += 1,
            Verdict::BothFail(e) => self.both_fail.push(format!("{what}: {e}")),
            Verdict::Differ(e) => self.differ.push(format!("{what}: {e}")),
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}: {} queries in {:.1} s: {} same, {} same up to float rounding, {} same up to ties, \
             {} fail on both sides, {} differ",
            self.name,
            self.checked,
            self.seconds,
            self.same,
            self.rounding,
            self.ties,
            self.both_fail.len(),
            self.differ.len()
        )?;
        for (seconds, what) in &self.slowest {
            writeln!(f, "  slow: {seconds:.1} s {what}")?;
        }
        for e in self.both_fail.iter().take(5) {
            writeln!(f, "  both fail: {e}")?;
        }
        for e in self.differ.iter().take(20) {
            writeln!(f, "  DIFFERS: {e}")?;
        }
        Ok(())
    }
}

/// Checks queries in parallel, on threads with a deep stack.
fn check_all<G: RdfViewOps + Sync>(
    report: &mut Report,
    view: &G,
    dataset: &Dataset,
    queries: &[(String, &str)],
) {
    let verdicts: Vec<(Verdict, f64)> = queries
        .par_iter()
        .map(|(_, q)| {
            let start = Instant::now();
            (check(view, dataset, q), start.elapsed().as_secs_f64())
        })
        .collect();
    for ((what, q), (verdict, seconds)) in queries.iter().zip(verdicts) {
        let what = format!("{what} `{}`", short(q));
        if seconds >= 1.0 {
            report.slowest.push((seconds, what.clone()));
            report.slowest.sort_by(|a, b| b.0.total_cmp(&a.0));
            report.slowest.truncate(3);
        }
        report.record(&what, verdict);
    }
}

/// Runs the suite on one scale.
fn run_scale(dir: &Path, products: usize, plan: &Plan) -> Result<Vec<Report>, String> {
    let pool = rayon::ThreadPoolBuilder::new()
        .stack_size(STACK)
        .build()
        .map_err(|e| e.to_string())?;
    pool.install(|| run_scale_on_pool(dir, products, plan))
}

fn run_scale_on_pool(dir: &Path, products: usize, plan: &Plan) -> Result<Vec<Report>, String> {
    let start = Instant::now();
    let bsbm = Bsbm::read(dir, products)?;
    let updates = bsbm.updates();
    println!(
        "BSBM {products}: {} triples in the dataset, {} updates inserting {} triples (read in {:.1} s)",
        bsbm.dataset_triples,
        updates.len(),
        bsbm.inserted_triples(),
        start.elapsed().as_secs_f64()
    );

    let start = Instant::now();
    let pg = PersistentGraph::new();
    load_raphtory(&pg, &bsbm)?;
    println!(
        "BSBM {products}: loaded into Raphtory in {:.1} s",
        start.elapsed().as_secs_f64()
    );

    let mut reports = Vec::new();
    let mut dataset = load_dataset(&bsbm.dataset)?;

    // the explore-and-update mix, replayed in order on the history
    if !bsbm.explore_update.is_empty() {
        let start = Instant::now();
        let mut report = Report::new(format!("{products} explore-and-update (as of each update)"));
        let mut applied = 0;
        let mut block: Vec<(String, &str)> = Vec::new();
        let flush = |report: &mut Report,
                     block: &mut Vec<(String, &str)>,
                     dataset: &Dataset,
                     applied: usize| {
            if !block.is_empty() {
                let view = pg.snapshot_at(BASE_TIME + applied as i64);
                check_all(report, &view, dataset, block);
                block.clear();
            }
        };
        for (i, entry) in bsbm.explore_update.iter().enumerate() {
            match &entry.op {
                Op::Query(q) => block.push((
                    format!("op {i} (Q{}, after {applied} updates)", entry.template),
                    q.as_str(),
                )),
                op => {
                    flush(&mut report, &mut block, &dataset, applied);
                    apply_dataset(&mut dataset, op);
                    applied += 1;
                }
            }
        }
        flush(&mut report, &mut block, &dataset, applied);
        report.seconds = start.elapsed().as_secs_f64();
        reports.push(report);
        assert_eq!(applied, updates.len());
        assert_eq!(update_time(applied - 1), BASE_TIME + applied as i64);
    }

    // the final state
    let triples = pg.valid().edges().explode_layers().iter().count();
    if triples != dataset.len() {
        return Err(format!(
            "the final state has {triples} triples in Raphtory, {} in the oracle",
            dataset.len()
        ));
    }
    println!("BSBM {products}: {triples} triples in the final state");
    for (name, picked) in [
        ("explore", plan.explore(&bsbm.explore)),
        ("business intelligence", plan.bi(&bsbm.bi)),
    ] {
        let start = Instant::now();
        let mut report = Report::new(format!("{products} {name}"));
        let queries: Vec<(String, &str)> = picked
            .into_iter()
            .map(|(t, q)| (format!("Q{t}"), q))
            .collect();
        check_all(&mut report, &pg, &dataset, &queries);
        report.seconds = start.elapsed().as_secs_f64();
        reports.push(report);
    }
    let sparqloscope = dir.join("sparqloscope-bsbm-5000.csv");
    if sparqloscope.is_file() {
        let start = Instant::now();
        let mut report = Report::new(format!("{products} Sparqloscope"));
        let queries = read_sparqloscope(&sparqloscope)?;
        let queries: Vec<(String, &str)> = queries
            .iter()
            .filter(|(d, _)| !plan.sparqloscope_skip.contains(&d.as_str()))
            .map(|(d, q)| (d.clone(), q.as_str()))
            .collect();
        if !plan.sparqloscope_skip.is_empty() {
            println!(
                "BSBM {products}: Sparqloscope queries {:?} not checked (slow; {FULL_ENV}=1 checks them)",
                plan.sparqloscope_skip
            );
        }
        check_all(&mut report, &pg, &dataset, &queries);
        report.seconds = start.elapsed().as_secs_f64();
        reports.push(report);
    }
    // every template must have been checked, except the ones the plan skips
    for (name, picked, entries, skipped) in [
        (
            "explore",
            plan.explore(&bsbm.explore),
            &bsbm.explore,
            vec![],
        ),
        (
            "business intelligence",
            plan.bi(&bsbm.bi),
            &bsbm.bi,
            plan.skipped_bi(),
        ),
    ] {
        let checked: HashSet<u32> = picked.iter().map(|(t, _)| *t).collect();
        let expected: HashSet<u32> = templates(entries)
            .into_iter()
            .filter(|t| !skipped.contains(t))
            .collect();
        if checked != expected {
            return Err(format!("not every {name} template was checked"));
        }
        if !skipped.is_empty() {
            println!("BSBM {products}: {name} templates {skipped:?} not checked (slow; {FULL_ENV}=1 checks them)");
        }
    }
    Ok(reports)
}

// ---------------------------------------------------------------------------------------------
// Self-tests: they run without the data
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod self_tests {
    use super::*;
    use bsbm::{parse_update, read_mix, Op};
    use bzip2::{write::BzEncoder, Compression};
    use raphtory::rdf::model::NamedNode;
    use std::{fs, io::Write};

    const EX: &str = "http://example.org/";

    fn bz2(data: &str) -> Vec<u8> {
        let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    /// A CSV field, quoted.
    fn field(s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }

    fn mix(rows: &[(u32, &str, &str)]) -> String {
        let mut csv = "id,kind,content\n".to_owned();
        for (id, kind, content) in rows {
            csv.push_str(&format!("{id},{kind},{}\n", field(content)));
        }
        csv
    }

    /// A tiny BSBM: products with prices (with ties), and an update stream that inserts a
    /// product and deletes an offer.
    fn small_bsbm(dir: &Path) {
        let mut data = String::new();
        for i in 0..6 {
            data.push_str(&format!(
                "<{EX}p{i}> <{EX}label> \"product {i}\" .\n<{EX}p{i}> <{EX}price> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n<{EX}o{i}> <{EX}product> <{EX}p{i}> .\n",
                i % 2
            ));
        }
        fs::write(dir.join("dataset-10.nt.bz2"), bz2(&data)).unwrap();
        let queries = [
            // ties at the LIMIT: three products have price 0
            (
                1,
                format!("SELECT ?p WHERE {{ ?p <{EX}price> ?v }} ORDER BY ?v LIMIT 2"),
            ),
            (
                1,
                format!("SELECT ?p WHERE {{ ?p <{EX}price> ?v }} ORDER BY DESC(?v) ?p"),
            ),
            (2, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }".to_owned()),
            (
                2,
                format!("SELECT ?p WHERE {{ ?p <{EX}price> ?v }} LIMIT 1"),
            ),
            (3, format!("DESCRIBE <{EX}p1>")),
            (
                3,
                format!("CONSTRUCT {{ ?p <{EX}cheap> true }} WHERE {{ ?p <{EX}price> 0 }}"),
            ),
            (4, format!("ASK {{ <{EX}o3> ?p ?o }}")),
            (4, "SELECT * WHERE { ?s ?p }".to_owned()),
        ];
        let rows: Vec<(u32, &str, &str)> = queries
            .iter()
            .map(|(t, q)| (*t, "query", q.as_str()))
            .collect();
        fs::write(dir.join("explore-10.csv.bz2"), bz2(&mix(&rows))).unwrap();
        // "# " is removed from the business intelligence queries
        let bi = format!(
            "SELECT ?p (COUNT(?o) AS ?n) {{ ?o <{EX}product> ?p }} # GROUP BY ?p ORDER BY DESC(?n) ?p LIMIT 3"
        );
        fs::write(
            dir.join("businessIntelligence-10.csv.bz2"),
            bz2(&mix(&[(1, "query", &bi)])),
        )
        .unwrap();
        let insert = format!(
            "INSERT DATA {{ <{EX}p9> <{EX}label> \"product 9\" . <{EX}p9> <{EX}price> 0 . <{EX}o9> <{EX}product> <{EX}p9> . }}"
        );
        let delete = format!("DELETE WHERE {{ <{EX}o3> ?p ?o }}");
        let update_mix = mix(&[
            (3, "query", queries[0].1.as_str()),
            (9, "query", queries[6].1.as_str()),
            (1, "update", insert.as_str()),
            (3, "query", queries[0].1.as_str()),
            (2, "update", delete.as_str()),
            (9, "query", queries[6].1.as_str()),
            (4, "query", queries[2].1.as_str()),
        ]);
        fs::write(dir.join("exploreAndUpdate-10.csv.bz2"), bz2(&update_mix)).unwrap();
        let sparqloscope = format!(
            "description,query\ncount,{}\nregex,{}\n",
            field("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"),
            field(&format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ ?s <{EX}label> ?o FILTER REGEX(?o, \"^pro\") }}"
            ))
        );
        fs::write(dir.join("sparqloscope-bsbm-5000.csv"), sparqloscope).unwrap();
    }

    #[test]
    fn suite_runs_on_a_small_bsbm() {
        let dir = tempfile::tempdir().unwrap();
        small_bsbm(dir.path());
        let reports = run_scale(dir.path(), 10, &Plan::full()).unwrap();
        for r in &reports {
            println!("{r}");
        }
        let names: Vec<&str> = reports.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "10 explore-and-update (as of each update)",
                "10 explore",
                "10 business intelligence",
                "10 Sparqloscope"
            ]
        );
        assert!(reports.iter().all(|r| r.differ.is_empty()));
        // the explore-and-update mix runs 5 queries; the explore mix 8 distinct queries, one of
        // which is invalid
        assert_eq!(reports[0].checked, 5);
        assert_eq!(reports[1].checked, 8);
        assert_eq!(reports[1].both_fail.len(), 1);
        assert_eq!(reports[2].checked, 1);
        assert_eq!(reports[2].both_fail.len(), 0);
        assert_eq!(reports[3].checked, 2);
    }

    #[test]
    fn the_history_is_replayed() {
        let dir = tempfile::tempdir().unwrap();
        small_bsbm(dir.path());
        let bsbm = Bsbm::read(dir.path(), 10).unwrap();
        assert_eq!(bsbm.dataset_triples, 18);
        assert_eq!(bsbm.updates().len(), 2);
        assert_eq!(bsbm.inserted_triples(), 3);
        let pg = PersistentGraph::new();
        load_raphtory(&pg, &bsbm).unwrap();
        let count = |view: &dyn Fn(&str) -> SparqlResults| match view(
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
        ) {
            SparqlResults::Solutions { rows, .. } => rows[0][0].as_ref().unwrap().to_string(),
            r => panic!("{r:?}"),
        };
        let n = |t: i64| count(&|q| pg.snapshot_at(t).sparql(q).unwrap());
        let int = |n: i32| format!("\"{n}\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        // the dataset, then the insert, then the delete of o3's one triple
        assert_eq!(n(BASE_TIME), int(18));
        assert_eq!(n(update_time(0)), int(21));
        assert_eq!(n(update_time(1)), int(20));
        assert_eq!(count(&|q| pg.sparql(q).unwrap()), int(20));
        let last = bsbm::final_dataset(&bsbm).unwrap();
        assert_eq!(last.len(), 20);
        // the benchmark's way of applying the updates to a dataset ends in the same state
        let mut timed = load_dataset(&bsbm.dataset).unwrap();
        let written: Vec<usize> = bsbm
            .updates()
            .into_iter()
            .map(|op| bsbm::apply_dataset_like_raphtory(&mut timed, op).unwrap())
            .collect();
        assert_eq!(written, [3, 1]);
        assert_eq!(timed, last);
    }

    #[test]
    fn updates_are_parsed() {
        let insert = parse_update(&format!(
            "INSERT DATA {{ <{EX}a> <{EX}p> \"x\" . <{EX}a> <{EX}q> <{EX}b> . }} "
        ))
        .unwrap();
        let Op::Insert { triples, doc, .. } = &insert else {
            panic!("{insert:?}")
        };
        assert_eq!(triples.len(), 2);
        assert_eq!(
            String::from_utf8(doc.clone()).unwrap(),
            format!("<{EX}a> <{EX}p> \"x\" .\n<{EX}a> <{EX}q> <{EX}b> .\n")
        );
        let delete = parse_update(&format!(" DELETE WHERE {{ <{EX}o1> ?p ?o }} ")).unwrap();
        assert_eq!(
            delete,
            Op::Delete {
                text: format!(" DELETE WHERE {{ <{EX}o1> ?p ?o }} "),
                subject: NamedNode::new(format!("{EX}o1")).unwrap()
            }
        );
        assert!(parse_update(&format!("DELETE WHERE {{ ?s <{EX}p> ?o }}")).is_err());
        assert!(parse_update("CLEAR ALL").is_err());
        assert!(parse_update("INSERT DATA { <a> }").is_err());
    }

    #[test]
    fn mixes_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mix.csv.bz2");
        fs::write(
            &path,
            bz2(&mix(&[
                (1, "query", "SELECT # x\n* {}"),
                (2, "query", "ASK {}"),
                (1, "query", "SELECT # x\n* {}"),
            ])),
        )
        .unwrap();
        let entries = read_mix(&path, true).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].op, Op::Query("SELECT x\n* {}".to_owned()));
        assert_eq!(
            read_mix(&path, false).unwrap()[0].op,
            Op::Query("SELECT # x\n* {}".to_owned())
        );
        assert_eq!(
            distinct_queries(&entries),
            [(1, "SELECT x\n* {}"), (2, "ASK {}")]
        );
        assert_eq!(templates(&entries), [1, 2]);
        assert_eq!(pick(&entries, Some(1), &[]).len(), 2);
        assert_eq!(pick(&entries, None, &[(1, 0)]), [(2, "ASK {}")]);
        assert_eq!(Plan::sample().skipped_bi(), [SLOW_BI]);
        assert!(Plan::full().skipped_bi().is_empty());
        fs::write(&path, bz2(&mix(&[(1, "remove", "x")]))).unwrap();
        assert!(read_mix(&path, false).is_err());
    }

    fn rows(rows: &[(&str, &str)]) -> Vec<KeyedRow> {
        rows.iter()
            .map(|(k, v)| (vec![k.to_string()], vec![v.to_string()]))
            .collect()
    }

    #[test]
    fn ties_cut_by_a_slice_may_differ() {
        let full = rows(&[("1", "a"), ("2", "b"), ("2", "c"), ("2", "d"), ("3", "e")]);
        // LIMIT 3: the tie on 2 is cut, any two of b, c and d will do
        assert!(compare_keyed(
            &rows(&[("1", "a"), ("2", "d"), ("2", "b")]),
            &full,
            0,
            Some(3)
        )
        .is_ok());
        // but they must be rows of the oracle, with the right keys, in the right sequence
        assert!(compare_keyed(
            &rows(&[("1", "a"), ("2", "x"), ("2", "b")]),
            &full,
            0,
            Some(3)
        )
        .is_err());
        assert!(compare_keyed(
            &rows(&[("2", "b"), ("1", "a"), ("2", "c")]),
            &full,
            0,
            Some(3)
        )
        .is_err());
        assert!(compare_keyed(&rows(&[("1", "a"), ("2", "b")]), &full, 0, Some(3)).is_err());
        // a row outside the cut group must be the oracle's
        assert!(compare_keyed(
            &rows(&[("1", "z"), ("2", "b"), ("2", "c")]),
            &full,
            0,
            Some(3)
        )
        .is_err());
        // OFFSET 2 LIMIT 2 cuts the group at both ends
        assert!(compare_keyed(&rows(&[("2", "b"), ("2", "c")]), &full, 2, Some(2)).is_ok());
        // without a cut, ties must hold the same rows in any order
        assert!(compare_keyed(
            &rows(&[("1", "a"), ("2", "d"), ("2", "c"), ("2", "b"), ("3", "e")]),
            &full,
            0,
            None
        )
        .is_ok());
        assert!(compare_keyed(
            &rows(&[("1", "a"), ("2", "d"), ("2", "d"), ("2", "b"), ("3", "e")]),
            &full,
            0,
            None
        )
        .is_err());
        // no ORDER BY: every row has the empty key, and a LIMIT picks any rows
        let unordered: Vec<KeyedRow> = ["a", "b", "c"]
            .iter()
            .map(|v| (vec![], vec![v.to_string()]))
            .collect();
        assert!(compare_keyed(&unordered[2..], &unordered, 0, Some(1)).is_ok());
    }

    #[test]
    fn rows_must_follow_their_keys() {
        let keyed = rows(&[("1", "a"), ("2", "b"), ("2", "c"), ("3", "d")]);
        let values = |v: &[&str]| -> Vec<Row> { v.iter().map(|v| vec![v.to_string()]).collect() };
        assert!(follows_keys(&values(&["a", "b", "c", "d"]), &keyed).is_ok());
        // ties in any order
        assert!(follows_keys(&values(&["a", "c", "b", "d"]), &keyed).is_ok());
        // but not across keys
        assert!(follows_keys(&values(&["b", "a", "c", "d"]), &keyed).is_err());
        assert!(follows_keys(&values(&["d", "c", "b", "a"]), &keyed).is_err());
        assert!(follows_keys(&values(&["a", "b", "c"]), &keyed).is_err());
        // no ORDER BY: one run of empty keys
        let unordered: Vec<KeyedRow> = ["a", "b"]
            .iter()
            .map(|v| (vec![], vec![v.to_string()]))
            .collect();
        assert!(follows_keys(&values(&["b", "a"]), &unordered).is_ok());
    }

    /// Rows of `sparql()` in the wrong `ORDER BY` sequence are a difference, even though the
    /// rewritten query (on another code path) gives them in the right one.
    #[test]
    fn a_wrong_sequence_is_a_difference() {
        let mut data = String::new();
        for i in 0..4 {
            data.push_str(&format!(
                "<{EX}p{i}> <{EX}price> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
                i % 2
            ));
        }
        let pg = PersistentGraph::new();
        pg.load_rdf(1, data.as_bytes(), raphtory::rdf::RdfFormat::NTriples, None)
            .unwrap();
        let dataset = load_dataset(data.as_bytes()).unwrap();
        let with_rows = |order: &[usize]| Normal::Rows {
            variables: vec!["p".to_owned()],
            rows: order.iter().map(|i| vec![format!("<{EX}p{i}>")]).collect(),
        };
        // the prices are 0, 1, 0, 1
        let query = format!("SELECT ?p WHERE {{ ?p <{EX}price> ?v }} ORDER BY DESC(?v)");
        let theirs = oracle(&dataset, &query).unwrap();
        assert!(matches!(
            check(&pg, &dataset, &query),
            Verdict::Same | Verdict::Ties
        ));
        let compare_with =
            |order: &[usize]| compare(&pg, &dataset, &query, &[], &with_rows(order), &theirs);
        assert!(matches!(
            compare_with(&[3, 1, 2, 0]),
            Verdict::Same | Verdict::Ties
        ));
        assert!(matches!(
            compare_with(&[1, 3, 0, 2]),
            Verdict::Same | Verdict::Ties
        ));
        for wrong in [[0, 2, 1, 3], [1, 0, 3, 2], [0, 1, 2, 3]] {
            let verdict = compare_with(&wrong);
            assert!(
                matches!(&verdict, Verdict::Differ(e) if e.contains("ORDER BY sequence")),
                "{wrong:?}: {verdict:?}"
            );
        }
        // the same with a LIMIT through the ties: Raphtory's rows are two with price 1, then one
        // of the two with price 0
        let limited = format!("{query} LIMIT 3");
        let theirs = oracle(&dataset, &limited).unwrap();
        let Ok(Normal::Rows { variables, rows }) = ours(&pg, &limited) else {
            panic!("no rows")
        };
        let reordered = |order: [usize; 3]| Normal::Rows {
            variables: variables.clone(),
            rows: order.iter().map(|&i| rows[i].clone()).collect(),
        };
        assert!(matches!(
            compare(&pg, &dataset, &limited, &[], &reordered([1, 0, 2]), &theirs),
            Verdict::Same | Verdict::Ties
        ));
        let verdict = compare(&pg, &dataset, &limited, &[], &reordered([2, 0, 1]), &theirs);
        assert!(
            matches!(&verdict, Verdict::Differ(e) if e.contains("ORDER BY sequence")),
            "{verdict:?}"
        );
    }

    #[test]
    fn floats_match_up_to_rounding() {
        let f = |v: &str| format!("\"{v}\"^^<http://www.w3.org/2001/XMLSchema#float>");
        let d = |v: &str| format!("\"{v}\"^^<http://www.w3.org/2001/XMLSchema#double>");
        assert!(cells_match(&f("1.1697693"), &f("1.1697694")));
        assert!(cells_match(&d("5475.0356"), &d("5475.0357")));
        assert!(!cells_match(&f("1.1697693"), &f("1.17")));
        assert!(!cells_match(&f("1.0"), &d("1.0")));
        assert!(!cells_match("\"1\"", "\"1.0\""));
        let row = |v: &str, x: &str| vec![f(v), x.to_owned()];
        let a = vec![row("1.0000001", "a"), row("2", "b"), row("2.0000001", "c")];
        let b = vec![row("2", "c"), row("1", "a"), row("2", "b")];
        assert!(rows_within(&a, &b, true));
        assert!(!rows_within(&a, &b[..2], true));
        assert!(rows_within(&a[..2], &b, false));
        assert!(!rows_within(&[row("3", "a")], &b, false));
        assert!(!sequences_match(&a, &b));
    }

    #[test]
    fn sampled_values_are_not_compared() {
        assert_eq!(
            sampled_variables(
                "SELECT ?o1 (MIN(?s) AS ?min) (SAMPLE(?s) AS ?sample) (sample(?x) as $Other) { }"
            ),
            ["sample", "Other"]
        );
        assert!(sampled_variables("SELECT ?sample { ?sample ?p ?o }").is_empty());
        let rows = Normal::Rows {
            variables: vec!["a".to_owned(), "sample".to_owned()],
            rows: vec![vec!["1".to_owned(), "2".to_owned()]],
        };
        assert_eq!(
            mask(rows, &["sample".to_owned()]),
            Normal::Rows {
                variables: vec!["a".to_owned(), "sample".to_owned()],
                rows: vec![vec!["1".to_owned(), "SAMPLE".to_owned()]],
            }
        );
    }

    #[test]
    fn queries_are_keyed() {
        let query = SparqlParser::new()
            .parse_query(&format!(
                "SELECT DISTINCT ?p WHERE {{ ?p <{EX}price> ?v }} ORDER BY DESC(?v + 1) ?p LIMIT 2 OFFSET 1"
            ))
            .unwrap();
        assert_eq!(
            Shape::of(&query),
            Shape {
                ordered: true,
                sliced: true
            }
        );
        let keyed = keyed(&query).unwrap();
        assert_eq!((keyed.start, keyed.length), (1, Some(2)));
        let mut data = String::new();
        for i in 0..4 {
            data.push_str(&format!(
                "<{EX}p{i}> <{EX}price> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
                i % 2
            ));
        }
        let dataset = load_dataset(data.as_bytes()).unwrap();
        let full = split_keys(&oracle_algebra(&dataset, &keyed.full).unwrap()).unwrap();
        let key = |v: i32| format!("\"{v}\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        let p = |i: i32| format!("<{EX}p{i}>");
        assert_eq!(
            full,
            [
                (vec![key(2), p(1)], vec![p(1)]),
                (vec![key(2), p(3)], vec![p(3)]),
                (vec![key(1), p(0)], vec![p(0)]),
                (vec![key(1), p(2)], vec![p(2)]),
            ]
        );
        let sliced = split_keys(&oracle_algebra(&dataset, &keyed.sliced).unwrap()).unwrap();
        assert_eq!(sliced, full[1..3]);
        // queries that are not SELECTs have no keys
        assert!(keyed_of("ASK {}").is_none());
        assert_eq!(
            Shape::of(&parse("SELECT * {} LIMIT 1")),
            Shape {
                ordered: false,
                sliced: true
            }
        );
        // the order of a sub-SELECT is not the result's
        assert_eq!(
            Shape::of(&parse("SELECT * { { SELECT ?x {} ORDER BY ?x } }")),
            Shape::default()
        );
    }

    fn parse(q: &str) -> Query {
        SparqlParser::new().parse_query(q).unwrap()
    }

    fn keyed_of(q: &str) -> Option<Keyed> {
        keyed(&parse(q))
    }

    #[test]
    fn differences_are_found() {
        let data = format!("<{EX}a> <{EX}p> <{EX}b> .\n<{EX}a> <{EX}p> <{EX}c> .\n");
        let pg = PersistentGraph::new();
        pg.load_rdf(1, data.as_bytes(), raphtory::rdf::RdfFormat::NTriples, None)
            .unwrap();
        let dataset = load_dataset(data.as_bytes()).unwrap();
        let mut other = dataset.clone();
        apply_dataset(
            &mut other,
            &parse_update(&format!("INSERT DATA {{ <{EX}a> <{EX}p> <{EX}d> . }}")).unwrap(),
        );
        let select = format!("SELECT ?o WHERE {{ <{EX}a> <{EX}p> ?o }}");
        assert_eq!(check(&pg, &dataset, &select), Verdict::Same);
        assert!(matches!(check(&pg, &other, &select), Verdict::Differ(_)));
        // ORDER BY: the sequence counts
        let ordered = format!("{select} ORDER BY ?o");
        assert_eq!(check(&pg, &dataset, &ordered), Verdict::Same);
        let describe = format!("DESCRIBE <{EX}a>");
        assert_eq!(check(&pg, &dataset, &describe), Verdict::Same);
        assert!(matches!(check(&pg, &other, &describe), Verdict::Differ(_)));
        assert!(matches!(
            check(&pg, &other, &format!("ASK {{ <{EX}a> <{EX}p> <{EX}d> }}")),
            Verdict::Differ(_)
        ));
        assert!(matches!(
            check(&pg, &dataset, "SELECT"),
            Verdict::BothFail(_)
        ));
        // a LIMIT through ties: either row will do
        let limited = format!("SELECT ?o WHERE {{ <{EX}a> <{EX}p> ?o }} LIMIT 1");
        assert!(matches!(
            check(&pg, &dataset, &limited),
            Verdict::Same | Verdict::Ties
        ));
    }
}
