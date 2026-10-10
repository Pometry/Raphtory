//! The W3C conformance tests for Raphtory's RDF support: the SPARQL 1.0 and 1.1 query evaluation
//! tests and the Turtle evaluation tests of <https://github.com/w3c/rdf-tests>.
//!
//! # Running
//!
//! The suites are read from the git submodule `raphtory-rdf-tests/test-suites/rdf-tests` (pinned
//! to the commit oxigraph pins; bump it with oxigraph), or from `RAPHTORY_RDF_TESTS` if set and
//! not empty. `make w3c-tests-init` checks the submodule out and `make rust-test-rdf-w3c` runs
//! this file.
//! Without a checkout the suites print a message and pass.
//!
//! # What is checked
//!
//! - `mf:QueryEvaluationTest`: the `qt:data` files are loaded with `load_rdf` (the base IRI is
//!   the URL of the file) into an `RdfGraph` with decoy history around them (see
//!   [`add_decoys`]), and the query, prefixed with `BASE <query URL>`, runs through `sparql()` on
//!   `at(T)`. It must give the expected result: the same solutions as a multiset up to
//!   blank-node renaming, the same boolean, or an isomorphic graph. Tests with
//!   `mf:resultCardinality mf:LaxCardinality`, named graphs (`qt:graphData`, or a query with
//!   `FROM` or `FROM NAMED`) or `SERVICE` are skipped: Raphtory stores only the default graph.
//! - `rdft:TestTurtleEval`: the document is loaded with `load_rdf` and exported as N-Triples with
//!   `to_rdf`, which must be isomorphic to the expected N-Triples.
//!
//! # Triage
//!
//! A test that fails is compared with an oracle: spareval (the evaluator Raphtory uses) over an
//! `oxrdf::Dataset` of the same data, which keeps terms exactly as written, as Raphtory does.
//! If Raphtory's result differs from the oracle's, the failure is a bug in `raphtory::rdf`. If
//! they agree, it comes from spareval or from the test suite, and the test must be listed in
//! [`KNOWN_FAILURES`] with the reason. A listed test that passes fails the run, so the list is
//! pruned when, for example, an oxigraph upgrade fixes one.
#[path = "common/oracle.rs"]
mod oracle;

use oxigraph::{
    io::RdfParser,
    sparql::results::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput},
};
use raphtory::{
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{
            dataset::CanonicalizationAlgorithm, vocab::rdf, BlankNode, Dataset, Graph as OxGraph,
            GraphName, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Quad, Term, TermRef, Triple,
        },
        parse_rdf_format, RdfFormat, SparqlResults,
    },
};
use spargebra::{algebra::GraphPattern, Query, SparqlParser};
use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::Duration,
};

/// The environment variable that overrides the directory of the checkout of w3c/rdf-tests.
const TESTS_ENV: &str = "RAPHTORY_RDF_TESTS";

/// The git submodule of w3c/rdf-tests, relative to the root of the workspace.
const SUBMODULE: &str = "raphtory-rdf-tests/test-suites/rdf-tests";

/// The command that checks the submodule out at its pinned commit (`--checkout` does so whatever
/// `update` strategy the submodule has).
const INIT: &str =
    "git submodule update --init --checkout raphtory-rdf-tests/test-suites/rdf-tests";

/// The URL of the root of the checkout. Test files are named by their URL under it, as
/// oxigraph's test runner names them, so relative IRIs resolve as the expected results assume.
const TESTS_URL: &str = "https://w3c.github.io/rdf-tests/";

const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const QT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#";
const RS: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#";
const RDFT: &str = "http://www.w3.org/ns/rdftest#";

/// The time the data is asserted at.
const T: i64 = 10;

/// The IRI prefix of the decoy triples of [`add_decoys`].
const DECOY: &str = "urn:decoy:";

/// How long one test may run.
const TIMEOUT: Duration = Duration::from_secs(120);

/// The stack of the thread a test runs on.
const STACK: usize = 64 << 20;

/// The IRI of a SPARQL 1.0 test.
macro_rules! r2 {
    ($test:literal) => {
        concat!("http://www.w3.org/2001/sw/DataAccess/tests/data-r2/", $test)
    };
}

/// The IRI of a SPARQL 1.1 test.
macro_rules! r11 {
    ($test:literal) => {
        concat!(
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/",
            $test
        )
    };
}

/// The tests that fail in the same way with spareval over an `oxrdf::Dataset`, with the reason:
/// they come from spareval/spargebra or from the test suite, not from `raphtory::rdf`. Terms are
/// compared exactly, as Raphtory keeps them as written.
const KNOWN_FAILURES: &[(&str, &str)] = &[
    // SPARQL 1.0
    (
        r2!("open-world/manifest#date-2"),
        "spareval compares an xsd:date without a timezone with one with a timezone (XSD 1.1 \
         rules) where SPARQL 1.0 expects a type error, so `!=` keeps two more dates",
    ),
    (
        r2!("optional-filter/manifest#dawg-optional-filter-005-not-simplified"),
        "spareval simplifies the nested group of `OPTIONAL { { .. } FILTER(..) }` before it \
         scopes the filter",
    ),
    (r2!("expr-builtin/manifest#dawg-str-1"), STR_FORMS),
    (r2!("expr-builtin/manifest#dawg-str-2"), STR_FORMS),
    // SPARQL 1.1
    (r11!("aggregates/manifest#agg-groupconcat-04"), GROUP_CONCAT),
    (r11!("aggregates/manifest#agg-groupconcat-06"), GROUP_CONCAT),
    (r11!("aggregates/manifest#agg-sum-02"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-avg-02"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-min-01"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-min-02"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-max-01"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-max-02"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-err-02"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-max-distinct"), NUMBER_FORMS),
    (r11!("aggregates/manifest#agg-min-distinct"), NUMBER_FORMS),
    (r11!("cast/manifest#cast-float"), NUMBER_FORMS),
    (r11!("cast/manifest#cast-double"), NUMBER_FORMS),
    (r11!("cast/manifest#cast-decimal"), NUMBER_FORMS),
    (r11!("functions/manifest#plus-1-corrected"), NUMBER_FORMS),
    (r11!("functions/manifest#coalesce01"), NUMBER_FORMS),
    (
        r11!("functions/manifest#bnode01"),
        "spareval's BNODE(str) gives the same blank node for the same string in different \
         solutions; SPARQL scopes it to one solution",
    ),
    (
        r11!("property-path/manifest#zero_or_more_set_start"),
        ZERO_LENGTH,
    ),
    (
        r11!("property-path/manifest#zero_or_more_set_end"),
        ZERO_LENGTH,
    ),
    (
        r11!("property-path/manifest#zero_or_one_set_start"),
        ZERO_LENGTH,
    ),
    (
        r11!("property-path/manifest#zero_or_one_set_end"),
        ZERO_LENGTH,
    ),
];

const NUMBER_FORMS: &str = "spareval writes the numbers that aggregates, casts and arithmetic \
    return in its own lexical form (\"32100\"^^xsd:double, \"2\"^^xsd:decimal), the expected \
    results in the canonical XSD form (\"3.21E4\", \"2.0\"): the values are equal";
const STR_FORMS: &str = "spareval's STR() of a number gives its canonical lexical form \
    (\"01\"^^xsd:integer and \"1.0e0\"^^xsd:double give \"1\"), not the form in the data";
const GROUP_CONCAT: &str = "spareval's GROUP_CONCAT keeps a language tag that all its values \
    share (\"1 2\"@en); the test expects a simple literal";
const ZERO_LENGTH: &str = "spareval's zero-length path does not match a constant that is not \
    in the data; the test (from an erratum) expects the constant to match itself";

// ---------------------------------------------------------------------------------------------
// The suites
// ---------------------------------------------------------------------------------------------

#[test]
fn sparql10_evaluation() {
    conformance("sparql/sparql10/manifest-evaluation.ttl");
}

/// The SPARQL 1.1 query tests (the syntax tests of `syntax-query` are skipped).
#[test]
fn sparql11_query() {
    conformance("sparql/sparql11/manifest-sparql11-query.ttl");
}

#[test]
fn turtle() {
    conformance("rdf/rdf11/rdf-turtle/manifest.ttl");
}

/// The directory of the checkout of w3c/rdf-tests: `RAPHTORY_RDF_TESTS` if it is set and not
/// empty, or else the submodule of the workspace.
fn tests_dir() -> PathBuf {
    match std::env::var_os(TESTS_ENV) {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("raphtory-rdf-tests is in a workspace")
            .join(SUBMODULE),
    }
}

/// The checkout of w3c/rdf-tests, or `None` (with a message) if [`tests_dir`] is not a checkout.
fn tests_root() -> Option<PathBuf> {
    checkout_root(&tests_dir())
        .inspect_err(|message| println!("{message}"))
        .ok()
}

/// `dir` as a canonical absolute path if it is a checkout of w3c/rdf-tests (it has a `sparql/`
/// directory), or else the skip message. A relative `dir` is resolved against the
/// `raphtory-rdf-tests` crate directory, where cargo runs the tests.
fn checkout_root(dir: &Path) -> Result<PathBuf, String> {
    let absolute = std::path::absolute(dir).unwrap_or_else(|_| dir.to_owned());
    if absolute.join("sparql").is_dir() {
        Ok(absolute.canonicalize().unwrap_or(absolute))
    } else {
        Err(format!(
            "skipping the W3C conformance tests: {} (resolved to {}) is not a checkout of \
             https://github.com/w3c/rdf-tests (it has no sparql/ directory). Run `{INIT}` (or \
             `make w3c-tests-init`) in the Raphtory repository, or set {TESTS_ENV} to a checkout \
             (a relative path is resolved against the raphtory-rdf-tests crate directory, where \
             cargo runs the tests)",
            dir.display(),
            absolute.display()
        ))
    }
}

/// Runs the suite of `manifest`, prints its report and fails if it has problems.
fn conformance(manifest: &str) {
    let Some(root) = tests_root() else {
        return;
    };
    if !root.join(manifest).is_file() {
        println!(
            "skipping {manifest}: it is not in {} (the submodule {SUBMODULE} at its pinned \
             commit has it: `{INIT}`)",
            root.display()
        );
        return;
    }
    let report = run_suite(&root, manifest, KNOWN_FAILURES);
    println!("{report}");
    assert!(
        report.problems.is_empty(),
        "{} problem(s) in {manifest}:\n{}",
        report.problems.len(),
        report.problems.join("\n")
    );
}

// ---------------------------------------------------------------------------------------------
// Manifests
// ---------------------------------------------------------------------------------------------

/// The files of the checkout, named by their URL under [`TESTS_URL`].
#[derive(Clone)]
struct Files {
    root: PathBuf,
}

impl Files {
    fn path(&self, url: &str) -> Result<PathBuf, String> {
        let relative = url
            .strip_prefix(TESTS_URL)
            .ok_or_else(|| format!("{url} is not under {TESTS_URL}"))?;
        Ok(self.root.join(relative))
    }

    fn read(&self, url: &str) -> Result<Vec<u8>, String> {
        fs::read(self.path(url)?).map_err(|e| format!("cannot read {url}: {e}"))
    }

    fn read_string(&self, url: &str) -> Result<String, String> {
        String::from_utf8(self.read(url)?).map_err(|e| format!("{url} is not UTF-8: {e}"))
    }
}

/// One test of a manifest.
#[derive(Clone, Debug, Default)]
struct TestCase {
    /// The IRI of the test.
    id: String,
    /// Its `rdf:type`s.
    kinds: Vec<String>,
    /// The query (`qt:query`), or the file of a syntax or Turtle test (`mf:action`).
    action: Option<String>,
    /// `qt:data`.
    data: Vec<String>,
    /// Whether it has `qt:graphData`.
    graph_data: bool,
    /// Whether it has `qt:serviceData`.
    service_data: bool,
    /// `mf:result`.
    result: Option<String>,
    /// Whether its `mf:resultCardinality` is `mf:LaxCardinality`.
    lax: bool,
}

impl TestCase {
    fn is(&self, namespace: &str, local: &str) -> bool {
        self.kinds
            .iter()
            .any(|kind| kind.strip_prefix(namespace) == Some(local))
    }
}

fn iri(namespace: &str, local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{namespace}{local}"))
}

/// The RDF format of a file, from its extension.
fn format_of(url: &str) -> Result<RdfFormat, String> {
    let extension = url.rsplit_once('.').map_or("", |(_, extension)| extension);
    parse_rdf_format(extension).map_err(|e| format!("{url}: {e}"))
}

/// Parses an RDF file, keeping blank-node labels.
fn read_triples(files: &Files, url: &str) -> Result<Vec<Triple>, String> {
    parse_triples(&files.read(url)?, format_of(url)?, url)
}

fn parse_triples(data: &[u8], format: RdfFormat, base: &str) -> Result<Vec<Triple>, String> {
    RdfParser::from_format(format)
        .with_base_iri(base)
        .map_err(|e| e.to_string())?
        .for_slice(data)
        .map(|quad| quad.map(Triple::from).map_err(|e| format!("{base}: {e}")))
        .collect()
}

fn read_graph(files: &Files, url: &str) -> Result<OxGraph, String> {
    let mut graph = OxGraph::new();
    for triple in read_triples(files, url)? {
        graph.insert(&triple);
    }
    Ok(graph)
}

fn subject_of(term: TermRef<'_>) -> Option<NamedOrBlankNodeRef<'_>> {
    match term {
        TermRef::NamedNode(node) => Some(node.into()),
        TermRef::BlankNode(node) => Some(node.into()),
        _ => None,
    }
}

fn iri_of(term: TermRef<'_>) -> Option<String> {
    match term {
        TermRef::NamedNode(node) => Some(node.as_str().to_owned()),
        _ => None,
    }
}

/// The items of the RDF list that is the object of `subject predicate`.
fn list<'a>(
    graph: &'a OxGraph,
    subject: NamedOrBlankNodeRef<'_>,
    predicate: NamedNodeRef<'_>,
) -> Vec<TermRef<'a>> {
    let mut items = Vec::new();
    let mut cell = graph.object_for_subject_predicate(subject, predicate);
    while let Some(node) = cell.and_then(subject_of) {
        if node == rdf::NIL.into() || items.len() > 100_000 {
            break;
        }
        items.extend(graph.object_for_subject_predicate(node, rdf::FIRST));
        cell = graph.object_for_subject_predicate(node, rdf::REST);
    }
    items
}

/// Reads the tests of the manifest at `url`, after those of the manifests it includes.
fn read_manifest(
    files: &Files,
    url: &str,
    tests: &mut Vec<TestCase>,
    depth: usize,
) -> Result<(), String> {
    if depth > 8 {
        return Err(format!("{url}: manifests include each other too deep"));
    }
    let graph = read_graph(files, url)?;
    let manifest = iri(MF, "Manifest");
    let manifests: Vec<_> = graph
        .subjects_for_predicate_object(rdf::TYPE, &manifest)
        .collect();
    if manifests.is_empty() {
        return Err(format!("{url} has no mf:Manifest"));
    }
    for subject in manifests {
        for include in list(&graph, subject, iri(MF, "include").as_ref()) {
            let include = iri_of(include).ok_or_else(|| format!("{url}: bad mf:include"))?;
            read_manifest(files, &include, tests, depth + 1)?;
        }
        for entry in list(&graph, subject, iri(MF, "entries").as_ref()) {
            tests.push(read_test(&graph, entry).ok_or_else(|| format!("{url}: bad entry"))?);
        }
    }
    Ok(())
}

/// The objects of `subject predicate`, where `predicate` is `namespace` + `local`.
fn objects<'a>(
    graph: &'a OxGraph,
    subject: NamedOrBlankNodeRef<'_>,
    namespace: &str,
    local: &str,
) -> Vec<TermRef<'a>> {
    graph
        .objects_for_subject_predicate(subject, &iri(namespace, local))
        .collect()
}

fn read_test(graph: &OxGraph, entry: TermRef<'_>) -> Option<TestCase> {
    let subject = subject_of(entry)?;
    let mut test = TestCase {
        id: iri_of(entry).unwrap_or_else(|| entry.to_string()),
        kinds: graph
            .objects_for_subject_predicate(subject, rdf::TYPE)
            .filter_map(iri_of)
            .collect(),
        result: objects(graph, subject, MF, "result")
            .into_iter()
            .find_map(iri_of),
        lax: objects(graph, subject, MF, "resultCardinality")
            .into_iter()
            .filter_map(iri_of)
            .any(|cardinality| cardinality == format!("{MF}LaxCardinality")),
        ..TestCase::default()
    };
    match objects(graph, subject, MF, "action").first()? {
        TermRef::NamedNode(action) => test.action = Some(action.as_str().to_owned()),
        TermRef::BlankNode(action) => {
            let qt = |local| objects(graph, (*action).into(), QT, local);
            test.action = qt("query").into_iter().find_map(iri_of);
            test.data = qt("data").into_iter().filter_map(iri_of).collect();
            test.graph_data = !qt("graphData").is_empty();
            test.service_data = !qt("serviceData").is_empty();
        }
        _ => return None,
    }
    Some(test)
}

// ---------------------------------------------------------------------------------------------
// Results and their comparison
// ---------------------------------------------------------------------------------------------

type Row = Vec<Option<Term>>;

/// Query results in a form that can be compared.
#[derive(Debug, Clone)]
enum Results {
    Solutions {
        variables: Vec<String>,
        rows: Vec<Row>,
    },
    Boolean(bool),
    Graph(Vec<Triple>),
}

impl Results {
    fn kind(&self) -> &'static str {
        match self {
            Self::Solutions { .. } => "solutions",
            Self::Boolean(_) => "a boolean",
            Self::Graph(_) => "a graph",
        }
    }
}

impl From<SparqlResults> for Results {
    fn from(results: SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => Self::Solutions {
                variables: variables.iter().map(|v| v.as_str().to_owned()).collect(),
                rows,
            },
            SparqlResults::Boolean(value) => Self::Boolean(value),
            SparqlResults::Graph(triples) => Self::Graph(triples),
        }
    }
}

/// A value with blank nodes replaced by `_:`, as compared where labels do not matter.
fn shape(value: &Option<Term>) -> String {
    match value {
        None => "UNDEF".to_owned(),
        Some(Term::BlankNode(_)) => "_:".to_owned(),
        Some(term) => term.to_string(),
    }
}

/// Compares results: solutions as multisets up to a renaming of blank nodes, graphs up to
/// isomorphism.
fn compare(expected: &Results, actual: &Results) -> Result<(), String> {
    match (expected, actual) {
        (Results::Boolean(e), Results::Boolean(a)) if e == a => Ok(()),
        (Results::Boolean(e), Results::Boolean(a)) => Err(format!("expected {e}, got {a}")),
        (Results::Graph(e), Results::Graph(a)) => compare_graphs(e, a),
        (
            Results::Solutions {
                variables: ev,
                rows: er,
            },
            Results::Solutions {
                variables: av,
                rows: ar,
            },
        ) => compare_solutions(ev, er, av, ar),
        (e, a) => Err(format!("expected {}, got {}", e.kind(), a.kind())),
    }
}

/// A graph with its blank nodes relabelled canonically, so that isomorphic graphs are equal.
fn canonical(triples: &[Triple]) -> OxGraph {
    let mut graph = OxGraph::new();
    for triple in triples {
        graph.insert(triple);
    }
    graph.canonicalize(CanonicalizationAlgorithm::Unstable);
    graph
}

fn compare_graphs(expected: &[Triple], actual: &[Triple]) -> Result<(), String> {
    let (e, a) = (canonical(expected), canonical(actual));
    if e == a {
        return Ok(());
    }
    let only = |x: &OxGraph, y: &OxGraph| {
        sample(x.iter().filter(|t| !y.contains(*t)).map(|t| t.to_string()))
    };
    Err(format!(
        "graphs differ ({} expected, {} actual triples); only expected: {}; only actual: {}",
        e.len(),
        a.len(),
        only(&e, &a),
        only(&a, &e)
    ))
}

/// Rows as a graph with a blank node per row, so that two multisets of rows are equal up to a
/// renaming of blank nodes when their graphs are isomorphic. The rows are not linked to a common
/// node: canonicalization would then permute all the rows that look alike.
fn solutions_graph(variables: &[String], rows: &[Row]) -> Result<Vec<Triple>, String> {
    let predicates = variables
        .iter()
        .map(|v| NamedNode::new(format!("{RS}value/{v}")).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let solution = iri(RS, "Solution");
    let mut triples = Vec::new();
    for row in rows {
        let node = BlankNode::default();
        triples.push(Triple::new(node.clone(), rdf::TYPE, solution.clone()));
        for (predicate, value) in predicates.iter().zip(row) {
            if let Some(value) = value {
                triples.push(Triple::new(node.clone(), predicate.clone(), value.clone()));
            }
        }
    }
    Ok(triples)
}

/// The first few items, for messages.
fn sample(items: impl Iterator<Item = String>) -> String {
    let items: Vec<_> = items.collect();
    let shown = items
        .iter()
        .take(6)
        .cloned()
        .collect::<Vec<_>>()
        .join(" | ");
    if items.len() > 6 {
        format!("[{shown} | ... {} more]", items.len() - 6)
    } else {
        format!("[{shown}]")
    }
}

fn compare_solutions(ev: &[String], er: &[Row], av: &[String], ar: &[Row]) -> Result<(), String> {
    let (mut es, mut avs) = (ev.to_vec(), av.to_vec());
    es.sort();
    avs.sort();
    if es != avs || ev.len() != av.len() {
        return Err(format!("expected variables {ev:?}, got {av:?}"));
    }
    // the actual rows with their columns in the expected order
    let columns: Vec<usize> = ev
        .iter()
        .map(|v| av.iter().position(|a| a == v).unwrap())
        .collect();
    let ar: Vec<Row> = ar
        .iter()
        .map(|row| columns.iter().map(|&i| row[i].clone()).collect())
        .collect();
    if canonical(&solutions_graph(ev, er)?) != canonical(&solutions_graph(ev, &ar)?) {
        let shapes = |rows: &[Row]| {
            let mut shapes: Vec<String> = rows
                .iter()
                .map(|row| row.iter().map(shape).collect::<Vec<_>>().join(" "))
                .collect();
            shapes.sort();
            shapes
        };
        let (es, as_) = (shapes(er), shapes(&ar));
        let only = |x: &[String], y: &[String]| {
            let mut y = y.to_vec();
            sample(
                x.iter()
                    .filter_map(|row| match y.iter().position(|r| r == row) {
                        Some(i) => {
                            y.swap_remove(i);
                            None
                        }
                        None => Some(row.clone()),
                    }),
            )
        };
        return Err(format!(
            "solutions differ ({} expected, {} actual) for {ev:?}; only expected: {}; only \
             actual: {}",
            er.len(),
            ar.len(),
            only(&es, &as_),
            only(&as_, &es)
        ));
    }
    Ok(())
}

fn has_service(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Service { .. } => true,
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => has_service(left) || has_service(right),
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => has_service(inner),
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
    }
}

// ---------------------------------------------------------------------------------------------
// Expected results
// ---------------------------------------------------------------------------------------------

fn extension(url: &str) -> &str {
    url.rsplit_once('.').map_or("", |(_, extension)| extension)
}

/// Reads the expected results of a query of the given form (`None` if the query does not
/// parse).
fn read_expected(files: &Files, url: &str, query: Option<&Query>) -> Result<Results, String> {
    let graph_form = matches!(
        query,
        Some(Query::Construct { .. } | Query::Describe { .. })
    );
    if let Some(format) = QueryResultsFormat::from_extension(extension(url)) {
        return parse_results(&files.read(url)?, format).map_err(|e| format!("{url}: {e}"));
    }
    let triples = read_triples(files, url)?;
    let mut graph = OxGraph::new();
    for triple in &triples {
        graph.insert(triple);
    }
    let result_set = graph
        .subject_for_predicate_object(rdf::TYPE, &iri(RS, "ResultSet"))
        .is_some();
    if graph_form || !result_set {
        Ok(Results::Graph(triples))
    } else {
        decode_result_set(&graph).map_err(|e| format!("{url}: {e}"))
    }
}

fn parse_results(data: &[u8], format: QueryResultsFormat) -> Result<Results, String> {
    match QueryResultsParser::from_format(format)
        .for_reader(data)
        .map_err(|e| e.to_string())?
    {
        ReaderQueryResultsParserOutput::Boolean(value) => Ok(Results::Boolean(value)),
        ReaderQueryResultsParserOutput::Solutions(solutions) => {
            let variables: Vec<String> = solutions
                .variables()
                .iter()
                .map(|v| v.as_str().to_owned())
                .collect();
            let rows = solutions
                .map(|solution| {
                    solution
                        .map(|s| s.values().to_vec())
                        .map_err(|e| e.to_string())
                })
                .collect::<Result<_, _>>()?;
            Ok(Results::Solutions { variables, rows })
        }
    }
}

/// Decodes results written in the `rs:` vocabulary.
fn decode_result_set(graph: &OxGraph) -> Result<Results, String> {
    let rs = |local| iri(RS, local);
    let set = graph
        .subject_for_predicate_object(rdf::TYPE, &rs("ResultSet"))
        .ok_or("no rs:ResultSet")?;
    let literal = |term: Option<TermRef<'_>>| match term {
        Some(TermRef::Literal(literal)) => Ok(literal.value().to_owned()),
        other => Err(format!("expected a literal, got {other:?}")),
    };
    if let Some(value) = graph.object_for_subject_predicate(set, &rs("boolean")) {
        return Ok(Results::Boolean(literal(Some(value))? == "true"));
    }
    let mut variables = graph
        .objects_for_subject_predicate(set, &rs("resultVariable"))
        .map(|v| literal(Some(v)))
        .collect::<Result<Vec<_>, _>>()?;
    variables.sort();
    let mut rows = Vec::new();
    for solution in graph.objects_for_subject_predicate(set, &rs("solution")) {
        let solution = subject_of(solution).ok_or("bad rs:solution")?;
        let mut row = vec![None; variables.len()];
        for binding in graph.objects_for_subject_predicate(solution, &rs("binding")) {
            let binding = subject_of(binding).ok_or("bad rs:binding")?;
            let variable = literal(graph.object_for_subject_predicate(binding, &rs("variable")))?;
            let value = graph
                .object_for_subject_predicate(binding, &rs("value"))
                .ok_or("rs:binding without rs:value")?;
            let column = variables
                .iter()
                .position(|v| *v == variable)
                .ok_or_else(|| format!("binding of ?{variable}, which is not a result variable"))?;
            row[column] = Some(value.into_owned());
        }
        rows.push(row);
    }
    Ok(Results::Solutions { variables, rows })
}

// ---------------------------------------------------------------------------------------------
// Running tests
// ---------------------------------------------------------------------------------------------

/// The outcome of a test.
#[derive(Debug)]
enum Outcome {
    Pass,
    /// The result is not the expected one, and the oracle gives the same result: the failure
    /// comes from spareval or the test suite.
    Upstream(String),
    /// The result is not the expected one, and the oracle gives another: a bug in Raphtory.
    Bug(String),
}

/// What happened to a test.
#[derive(Debug)]
enum TestRun {
    Skipped(String),
    Ran(Outcome),
    /// The test could not be run: a file is missing or malformed, or it panicked or timed out.
    Error(String),
}

/// Runs a test on its own thread, with a large stack and a timeout.
fn run_isolated(files: &Files, test: &TestCase) -> TestRun {
    let (tx, rx) = mpsc::channel();
    let (files, test) = (files.clone(), test.clone());
    let spawned = thread::Builder::new()
        .name(test.id.clone())
        .stack_size(STACK)
        .spawn(move || {
            let _ = tx.send(run_test(&files, &test));
        });
    if let Err(e) = spawned {
        return TestRun::Error(format!("cannot start a thread: {e}"));
    }
    match rx.recv_timeout(TIMEOUT) {
        Ok(run) => run,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            TestRun::Error(format!("timed out after {} s", TIMEOUT.as_secs()))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => TestRun::Error("panicked".to_owned()),
    }
}

fn run_test(files: &Files, test: &TestCase) -> TestRun {
    let result = if test.is(MF, "QueryEvaluationTest") {
        Eval::new(files, test).map(|eval| match eval {
            Ok(eval) => TestRun::Ran(eval.outcome()),
            Err(skip) => TestRun::Skipped(skip),
        })
    } else if test.is(RDFT, "TestTurtleEval") {
        turtle_eval_test(files, test)
    } else {
        let kind = test.kinds.first().map_or("none", String::as_str);
        let kind = kind.rsplit(['#', '/']).next().unwrap_or(kind);
        Ok(TestRun::Skipped(format!("test type {kind}")))
    };
    result.unwrap_or_else(TestRun::Error)
}

/// A query evaluation test: its query, data and expected results.
struct Eval {
    /// The query with `BASE <query URL>` in front.
    query: String,
    /// The data files: URL, content and format.
    data: Vec<(String, Vec<u8>, RdfFormat)>,
    expected: Results,
}

impl Eval {
    /// The test, or `Ok(Err(reason))` if it is skipped.
    fn new(files: &Files, test: &TestCase) -> Result<Result<Self, String>, String> {
        if test.graph_data {
            return Ok(Err("named graphs (qt:graphData)".to_owned()));
        }
        if test.service_data {
            return Ok(Err("SERVICE (qt:serviceData)".to_owned()));
        }
        if test.lax {
            return Ok(Err("lax cardinality (mf:LaxCardinality)".to_owned()));
        }
        let query_url = test.action.as_deref().ok_or("no qt:query")?;
        let query = format!("BASE <{query_url}>\n{}", files.read_string(query_url)?);
        let parsed = SparqlParser::new().parse_query(&query).ok();
        if let Some(parsed) = &parsed {
            if parsed.dataset().is_some() {
                return Ok(Err("FROM / FROM NAMED".to_owned()));
            }
            let pattern = match parsed {
                Query::Select { pattern, .. }
                | Query::Construct { pattern, .. }
                | Query::Describe { pattern, .. }
                | Query::Ask { pattern, .. } => pattern,
            };
            if has_service(pattern) {
                return Ok(Err("SERVICE".to_owned()));
            }
        }
        let result_url = test.result.as_deref().ok_or("no mf:result")?;
        let expected = read_expected(files, result_url, parsed.as_ref())?;
        let data = test
            .data
            .iter()
            .map(|url| Ok((url.clone(), files.read(url)?, format_of(url)?)))
            .collect::<Result<_, String>>()?;
        Ok(Ok(Self {
            query,
            data,
            expected,
        }))
    }

    /// The data asserted at `T` in an `RdfGraph`, with decoy history around it.
    fn graph(&self) -> Result<RdfGraph, String> {
        let g = RdfGraph::new();
        for (url, data, format) in &self.data {
            g.load_rdf(T, data.as_slice(), *format, Some(url.as_str()))
                .map_err(|e| format!("cannot load {url}: {e}"))?;
        }
        add_decoys(&g).map_err(|e| format!("cannot add decoys: {e}"))?;
        Ok(g)
    }

    /// Raphtory's results, on the graph as of `T`.
    fn ours(&self) -> Result<Results, String> {
        let g = self.graph()?;
        g.at(T)
            .sparql(&self.query)
            .map(Results::from)
            .map_err(|e| e.to_string())
    }

    /// The oracle's results: spareval over an `oxrdf::Dataset` of the data.
    fn oracle(&self) -> Result<Results, String> {
        let mut dataset = Dataset::new();
        for (url, data, format) in &self.data {
            let parser = RdfParser::from_format(*format)
                .with_base_iri(url)
                .map_err(|e| e.to_string())?
                .rename_blank_nodes();
            for quad in parser.for_slice(data) {
                let quad = quad.map_err(|e| format!("{url}: {e}"))?;
                dataset.insert(&Quad::new(
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    GraphName::DefaultGraph,
                ));
            }
        }
        oracle::evaluate(&dataset, &self.query).map(Results::from)
    }

    fn outcome(&self) -> Outcome {
        let actual = self.ours();
        let check = actual
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|actual| compare(&self.expected, actual));
        match check {
            Ok(()) => Outcome::Pass,
            Err(diff) => self.triage(actual, diff),
        }
    }

    /// Tells a bug of Raphtory (its result differs from the oracle's) from a failure that
    /// comes from spareval or the test suite (the same result as the oracle).
    fn triage(&self, ours: Result<Results, String>, diff: String) -> Outcome {
        let oracle = self.oracle();
        match (&ours, &oracle) {
            (Ok(ours), Ok(oracle)) if compare(oracle, ours).is_ok() => Outcome::Upstream(diff),
            (Err(_), Err(_)) => Outcome::Upstream(diff),
            (_, oracle) => Outcome::Bug(format!(
                "{diff}; the oracle gives {}",
                match oracle {
                    Ok(Results::Solutions { rows, .. }) => format!("{} solutions", rows.len()),
                    Ok(Results::Boolean(value)) => value.to_string(),
                    Ok(Results::Graph(triples)) => format!("{} triples", triples.len()),
                    Err(e) => format!("an error: {e}"),
                }
            )),
        }
    }
}

/// Adds history around the data of `rg` (asserted at `T`) that is not visible as of `T`: for
/// every triple `s p o`, `s p <decoy>` and `s <decoy> o` asserted at 5 and retracted at 7,
/// `<decoy> p o` asserted at 20, and the triple itself asserted at 3, retracted at 5 and
/// retracted again at 20; and a triple of decoy terms asserted at 5 and retracted at 7.
fn add_decoys(rg: &RdfGraph) -> Result<(), GraphError> {
    let mut doc = Vec::new();
    rg.to_rdf(&mut doc, RdfFormat::NTriples, &[])?;
    let triples = parse_triples(&doc, RdfFormat::NTriples, TESTS_URL)
        .expect("to_rdf writes N-Triples that parse");
    let decoy = |local: String| NamedNode::new_unchecked(format!("{DECOY}{local}"));
    let gone = Triple::new(decoy("s".into()), decoy("p".into()), decoy("o".into()));
    rg.add_triple(5, &gone)?;
    rg.retract_triple(7, &gone)?;
    for (i, t) in triples.iter().enumerate() {
        let other_object = Triple::new(
            t.subject.clone(),
            t.predicate.clone(),
            decoy(format!("o{i}")),
        );
        let other_layer = Triple::new(t.subject.clone(), decoy("p".into()), t.object.clone());
        for gone in [&other_object, &other_layer] {
            rg.add_triple(5, gone)?;
            rg.retract_triple(7, gone)?;
        }
        let later = Triple::new(
            decoy(format!("s{i}")),
            t.predicate.clone(),
            t.object.clone(),
        );
        rg.add_triple(20, &later)?;
        rg.add_triple(3, t)?;
        rg.retract_triple(5, t)?;
        rg.retract_triple(20, t)?;
    }
    Ok(())
}

/// A Turtle evaluation test: loaded with `load_rdf` and written with `to_rdf`, the document
/// must give the expected N-Triples. The oracle is the Turtle parser.
fn turtle_eval_test(files: &Files, test: &TestCase) -> Result<TestRun, String> {
    let url = test.action.as_deref().ok_or("no mf:action")?;
    let doc = files.read(url)?;
    let expected = read_triples(files, test.result.as_deref().ok_or("no mf:result")?)?;
    let oracle = parse_triples(&doc, RdfFormat::Turtle, url);
    let export = |g: &RdfGraph| -> Result<Vec<Triple>, String> {
        let mut out = Vec::new();
        let stats = g
            .at(T)
            .to_rdf(&mut out, RdfFormat::NTriples, &[])
            .map_err(|e| e.to_string())?;
        if stats.skipped > 0 {
            return Err(format!("to_rdf skipped {} triples", stats.skipped));
        }
        parse_triples(&out, RdfFormat::NTriples, TESTS_URL)
    };
    let g = RdfGraph::new();
    let ours = g
        .load_rdf(T, doc.as_slice(), RdfFormat::Turtle, Some(url))
        .map_err(|e: GraphError| format!("cannot load {url}: {e}"))
        .and_then(|_| export(&g));
    let outcome = match ours
        .clone()
        .and_then(|ours| compare_graphs(&expected, &ours))
    {
        Ok(()) => Outcome::Pass,
        Err(diff) => match (&ours, &oracle) {
            (Ok(ours), Ok(oracle)) if compare_graphs(oracle, ours).is_ok() => {
                Outcome::Upstream(diff)
            }
            (Err(_), Err(_)) => Outcome::Upstream(diff),
            _ => Outcome::Bug(diff),
        },
    };
    Ok(TestRun::Ran(outcome))
}

// ---------------------------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------------------------

/// What a suite gave.
#[derive(Default)]
struct Report {
    suite: String,
    tests: usize,
    /// Tests skipped, by reason.
    skipped: BTreeMap<String, usize>,
    pass: usize,
    fail: usize,
    /// The known failures that failed as expected, with the reason.
    known: Vec<(String, String)>,
    /// Failures, errors and known failures that pass.
    problems: Vec<String>,
}

impl Report {
    fn add(&mut self, test: &TestCase, run: TestRun, known: &[(&str, &str)]) {
        self.tests += 1;
        let listed = known
            .iter()
            .find(|(id, _)| *id == test.id)
            .map(|(_, reason)| *reason);
        let id = &test.id;
        match run {
            TestRun::Skipped(reason) => {
                if listed.is_some() {
                    self.problems.push(format!(
                        "{id} is listed in KNOWN_FAILURES but is skipped ({reason})"
                    ));
                }
                *self.skipped.entry(reason).or_default() += 1;
            }
            TestRun::Error(e) => self.problems.push(format!("{id}: {e}")),
            TestRun::Ran(Outcome::Pass) => {
                self.pass += 1;
                if let Some(reason) = listed {
                    self.problems.push(format!(
                        "{id} is listed in KNOWN_FAILURES ({reason}) but passes: remove it from \
                         the list"
                    ));
                }
            }
            TestRun::Ran(Outcome::Upstream(diff)) => match listed {
                Some(reason) => self.known.push((id.clone(), reason.to_owned())),
                None => {
                    self.fail += 1;
                    self.problems.push(format!(
                        "{id}: {}; the oracle (spareval over an oxrdf Dataset) gives the same \
                         result, so list it in KNOWN_FAILURES with the reason",
                        truncate(&diff)
                    ));
                }
            },
            TestRun::Ran(Outcome::Bug(diff)) => {
                self.fail += 1;
                self.problems.push(format!(
                    "{id}: BUG, Raphtory differs from the oracle: {}",
                    truncate(&diff)
                ));
            }
        }
    }
}

fn truncate(text: &str) -> String {
    const MAX: usize = 1200;
    match text.char_indices().nth(MAX) {
        Some((at, _)) => format!("{} ...", &text[..at]),
        None => text.to_owned(),
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let skipped: usize = self.skipped.values().sum();
        writeln!(
            f,
            "== W3C suite {}: {} tests, {} pass, {} known, {} fail, {} skipped",
            self.suite,
            self.tests,
            self.pass,
            self.known.len(),
            self.fail,
            skipped
        )?;
        for (reason, n) in &self.skipped {
            writeln!(f, "   skipped: {n} x {reason}")?;
        }
        if !self.known.is_empty() {
            writeln!(f, "   known failures ({}):", self.known.len())?;
            for (id, reason) in &self.known {
                writeln!(f, "      {id}: {reason}")?;
            }
        }
        if !self.problems.is_empty() {
            writeln!(f, "   PROBLEMS ({}):", self.problems.len())?;
            for problem in &self.problems {
                writeln!(f, "      {problem}")?;
            }
        }
        Ok(())
    }
}

/// Runs every test of the manifest at `manifest` (relative to `root`).
fn run_suite(root: &Path, manifest: &str, known: &[(&str, &str)]) -> Report {
    let files = Files {
        root: root.to_owned(),
    };
    let mut report = Report {
        suite: manifest.to_owned(),
        ..Report::default()
    };
    let mut tests = Vec::new();
    if let Err(e) = read_manifest(&files, &format!("{TESTS_URL}{manifest}"), &mut tests, 0) {
        report.problems.push(e);
        return report;
    }
    for test in &tests {
        let run = run_isolated(&files, test);
        report.add(test, run, known);
    }
    report
}
