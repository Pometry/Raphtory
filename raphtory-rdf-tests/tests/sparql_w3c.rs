//! The W3C conformance tests for Raphtory's RDF support: the SPARQL 1.0 and 1.1 query tests, the
//! SPARQL 1.1 results-format tests and the Turtle tests of <https://github.com/w3c/rdf-tests>.
//!
//! # Running
//!
//! The suites are read from the git submodule `raphtory-rdf-tests/test-suites/rdf-tests` (pinned
//! to the commit oxigraph pins; bump it with oxigraph), or from `RAPHTORY_RDF_TESTS` if set and
//! not empty. `make w3c-tests-init` checks the submodule out and `make rust-test-rdf-w3c` runs
//! this file.
//! Without a checkout the suites print a message and pass; the self-tests at the end always run.
//!
//! # What is checked
//!
//! - `mf:QueryEvaluationTest` and `mf:CSVResultFormatTest`: the `qt:data` files are loaded with
//!   `load_rdf` (the base IRI is the URL of the file), and the query, prefixed with
//!   `BASE <query URL>`, runs through `sparql()` in every [`Mode`]. Each mode must give the
//!   expected result: the same solutions as a multiset up to blank-node renaming, the same
//!   boolean, or an isomorphic graph. Under a top-level `ORDER BY`, when the expected solutions
//!   are in order, they must also come in the same order: that of the `ORDER BY` keys if they
//!   are all projected variables, or else that of the whole rows (see [`Order`]). A test with
//!   `mf:resultCardinality mf:LaxCardinality` passes with fewer duplicates (see [`Rules::lax`]).
//!   Tests with named graphs (`qt:graphData`, or a query with `FROM` or `FROM NAMED`) or
//!   `SERVICE` are skipped: Raphtory stores only the default graph.
//! - `mf:PositiveSyntaxTest(11)` and `mf:NegativeSyntaxTest(11)`: `sparql()` must accept the
//!   query, or reject it with a syntax error.
//! - `rdft:TestTurtleEval`: the document is loaded with `load_rdf` and exported as N-Triples with
//!   `to_rdf`, which must be isomorphic to the expected N-Triples. The Turtle syntax tests must
//!   load, or fail to load.
//!
//! # Triage
//!
//! A mode that fails is compared with an oracle: spareval (the evaluator Raphtory uses) over an
//! `oxrdf::Dataset` of the same data, which keeps terms exactly as written, as Raphtory does.
//! If Raphtory's result differs from the oracle's, the failure is a bug in `raphtory::rdf`. If
//! they agree, it comes from spareval or from the test suite, and the test must be listed in
//! [`KNOWN_FAILURES`] with the reason. A listed test that passes in every mode fails the run, so
//! the list is pruned when, for example, an oxigraph upgrade fixes one.
use oxigraph::sparql::results::{
    QueryResultsFormat, QueryResultsParser, QueryResultsSerializer, ReaderQueryResultsParserOutput,
};
use raphtory::{
    errors::GraphError,
    prelude::*,
    rdf::{
        evaluator,
        model::{
            dataset::CanonicalizationAlgorithm, vocab::rdf, BlankNode, Dataset, Graph as RdfGraph,
            GraphName, Literal, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Quad, Term, TermRef,
            Triple,
        },
        parse_rdf_format, RdfError, RdfFormat, RdfMutationOps, RdfParser, RdfViewOps,
        SparqlResults, ASOF_NS,
    },
};
use spargebra::{
    algebra::{Expression, GraphPattern, OrderExpression, QueryDataset},
    Query, SparqlParser,
};
use std::{
    cell::OnceCell,
    collections::{BTreeMap, HashMap},
    fmt, fs,
    path::{Path, PathBuf},
    str::FromStr,
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

/// The IRI prefix of the decoy triples of [`Mode::Decoys`].
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
    (
        r2!("syntax-sparql3/manifest#syn-bad-26"),
        "spargebra reads `?x<?a&&?b>?y` as comparisons; by the longest-token rule `<?a&&?b>` \
         is an IRI, so the query is invalid",
    ),
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
    // SPARQL 1.1 results formats
    (
        r11!("csv-tsv-res/manifest#tsv03"),
        "the expected TSV writes the data's \"1.0E6\"^^xsd:double as 1.0e6, another lexical \
         form of the same value",
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

/// The manifests of the suites, relative to the root of the checkout.
const SUITES: &[&str] = &[
    "sparql/sparql10/manifest-evaluation.ttl",
    "sparql/sparql10/manifest-syntax.ttl",
    "sparql/sparql11/manifest-sparql11-query.ttl",
    "sparql/sparql11/manifest-sparql11-results.ttl",
    "rdf/rdf11/rdf-turtle/manifest.ttl",
];

#[test]
fn sparql10_evaluation() {
    conformance(SUITES[0]);
}

#[test]
fn sparql10_syntax() {
    conformance(SUITES[1]);
}

/// The SPARQL 1.1 query tests, including the syntax tests of `syntax-query`.
#[test]
fn sparql11_query() {
    conformance(SUITES[2]);
}

/// The JSON, CSV and TSV results-format tests.
#[test]
fn sparql11_results_formats() {
    conformance(SUITES[3]);
}

#[test]
fn turtle() {
    conformance(SUITES[4]);
}

/// Every test listed in [`KNOWN_FAILURES`] is a test of one of the suites.
#[test]
fn known_failures_are_tests_of_the_suites() {
    let Some(root) = tests_root() else {
        return;
    };
    let files = Files { root };
    let mut tests = Vec::new();
    for suite in SUITES {
        read_manifest(&files, &format!("{TESTS_URL}{suite}"), &mut tests, 0).unwrap();
    }
    for (id, _) in KNOWN_FAILURES {
        assert!(
            tests.iter().any(|test| test.id == *id),
            "{id} is listed in KNOWN_FAILURES but is not a test of the suites"
        );
    }
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
    /// Whether its `mf:resultCardinality` is `mf:LaxCardinality` (see [`Rules::lax`]).
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

fn read_graph(files: &Files, url: &str) -> Result<RdfGraph, String> {
    let mut graph = RdfGraph::new();
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
    graph: &'a RdfGraph,
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
    graph: &'a RdfGraph,
    subject: NamedOrBlankNodeRef<'_>,
    namespace: &str,
    local: &str,
) -> Vec<TermRef<'a>> {
    graph
        .objects_for_subject_predicate(subject, &iri(namespace, local))
        .collect()
}

fn read_test(graph: &RdfGraph, entry: TermRef<'_>) -> Option<TestCase> {
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

/// How the order of solutions is checked (see [`order_of`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum Order {
    /// Not checked: the query has no top-level `ORDER BY`.
    #[default]
    Unordered,
    /// Every `ORDER BY` key is a projected variable, and these are the keys: their values must
    /// come in the same sequence. Ties may come in any order.
    Keys(Vec<String>),
    /// The `ORDER BY` has a key that is an expression or a variable that is not projected, so
    /// its values are not in the rows: the rows themselves (blank nodes shown as `_:`) must come
    /// in the same sequence. This assumes the `ORDER BY` has no ties, which holds for the W3C
    /// tests.
    Rows,
}

/// How solutions are compared with the expected ones.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Rules {
    order: Order,
    /// `mf:LaxCardinality`: the actual solutions must be the expected ones, each at least once
    /// and at most as many times as expected (as `REDUCED` allows), instead of the same
    /// multiset. In order, they must be a subsequence of the expected sequence.
    lax: bool,
}

impl Rules {
    /// Compares as a multiset only, keeping `lax`.
    fn unordered(&self) -> Self {
        Self {
            order: Order::Unordered,
            lax: self.lax,
        }
    }

    /// These rules, without the order if `ordered` is false (the expected rows are in no order).
    fn ordered_if(&self, ordered: bool) -> Self {
        if ordered {
            self.clone()
        } else {
            self.unordered()
        }
    }
}

/// Compares results: solutions as multisets up to a renaming of blank nodes, in order and with
/// lax cardinality as `rules` say; graphs up to isomorphism.
fn compare(expected: &Results, actual: &Results, rules: &Rules) -> Result<(), String> {
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
        ) => compare_solutions(ev, er, av, ar, rules),
        (e, a) => Err(format!("expected {}, got {}", e.kind(), a.kind())),
    }
}

fn compare_graphs(expected: &[Triple], actual: &[Triple]) -> Result<(), String> {
    let canonical = |triples: &[Triple]| {
        let mut graph = RdfGraph::new();
        for triple in triples {
            graph.insert(triple);
        }
        graph.canonicalize(CanonicalizationAlgorithm::Unstable);
        graph
    };
    let (e, a) = (canonical(expected), canonical(actual));
    if e == a {
        return Ok(());
    }
    let only = |x: &RdfGraph, y: &RdfGraph| {
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

fn compare_solutions(
    ev: &[String],
    er: &[Row],
    av: &[String],
    ar: &[Row],
    rules: &Rules,
) -> Result<(), String> {
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
    let same = if rules.lax {
        rows_lax(er, &ar)
    } else {
        rows_isomorphic(er, &ar)
    };
    if !same {
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
        let lax = if rules.lax {
            " (with lax cardinality)"
        } else {
            ""
        };
        return Err(format!(
            "solutions differ{lax} ({} expected, {} actual) for {ev:?}; only expected: {}; only \
             actual: {}",
            er.len(),
            ar.len(),
            only(&es, &as_),
            only(&as_, &es)
        ));
    }
    // the sequence of what the order is checked on, in each row
    let sequence = |rows: &[Row], columns: &[usize]| -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| columns.iter().map(|&i| shape(&row[i])).collect())
            .collect()
    };
    // ties of the ORDER BY may come in any order, so only its keys are compared when they are
    // columns of the results, and whole rows otherwise
    let keys = match &rules.order {
        Order::Unordered => return Ok(()),
        Order::Keys(keys) => keys
            .iter()
            .map(|key| ev.iter().position(|v| v == key))
            .collect::<Option<Vec<_>>>()
            .map(|columns| (keys, columns)),
        Order::Rows => None,
    };
    let (what, columns) = match keys {
        Some((keys, columns)) => (format!("of ORDER BY {keys:?}"), columns),
        None => (
            "of the ORDER BY, compared row by row".to_owned(),
            (0..ev.len()).collect(),
        ),
    };
    let (es, as_) = (sequence(er, &columns), sequence(&ar, &columns));
    if rules.lax {
        // with fewer duplicates, the rows of a sorted result are a subsequence of the expected
        let mut expected = es.iter();
        if let Some(at) = as_
            .iter()
            .position(|row| !expected.any(|expected| expected == row))
        {
            return Err(format!(
                "the solutions are not in the order {what}: row {at}, {:?}, is out of the \
                 expected sequence",
                as_[at]
            ));
        }
    } else if es != as_ {
        let at = es.iter().zip(&as_).position(|(e, a)| e != a).unwrap_or(0);
        return Err(format!(
            "the solutions are not in the order {what}: at row {at} expected {:?}, got {:?}",
            es.get(at),
            as_.get(at)
        ));
    }
    Ok(())
}

/// Whether two multisets of rows are equal up to a renaming of blank nodes (a bijection).
fn rows_isomorphic(expected: &[Row], actual: &[Row]) -> bool {
    blank_node_bijection(expected, actual).is_some()
}

/// Whether `actual` has the solutions of `expected` up to a renaming of blank nodes, each at
/// least once and at most as many times as in `expected` (`mf:LaxCardinality`).
fn rows_lax(expected: &[Row], actual: &[Row]) -> bool {
    let counts = |rows: &[Row]| {
        let mut counts: Vec<(Row, usize)> = Vec::new();
        for row in rows {
            match counts.iter_mut().find(|(r, _)| r == row) {
                Some((_, n)) => *n += 1,
                None => counts.push((row.clone(), 1)),
            }
        }
        counts
    };
    let (ec, ac) = (counts(expected), counts(actual));
    let distinct = |counts: &[(Row, usize)]| -> Vec<Row> {
        counts.iter().map(|(row, _)| row.clone()).collect()
    };
    // the distinct solutions are the same, up to blank nodes
    let Some(renaming) = blank_node_bijection(&distinct(&ec), &distinct(&ac)) else {
        return false;
    };
    ac.iter().all(|(row, n)| {
        let row: Row = row
            .iter()
            .map(|value| match value {
                Some(Term::BlankNode(b)) => {
                    Some(Term::BlankNode(renaming.get(b).unwrap_or(b).clone()))
                }
                other => other.clone(),
            })
            .collect();
        ec.iter().any(|(r, m)| *r == row && n <= m)
    })
}

/// A renaming of the blank nodes of `actual` to those of `expected` under which the two
/// multisets of rows are equal, if there is one.
fn blank_node_bijection(expected: &[Row], actual: &[Row]) -> Option<HashMap<BlankNode, BlankNode>> {
    if expected.len() != actual.len() {
        return None;
    }
    let has_blank = |row: &&Row| {
        row.iter()
            .any(|value| matches!(value, Some(Term::BlankNode(_))))
    };
    let (eb, eg): (Vec<&Row>, Vec<&Row>) = expected.iter().partition(has_blank);
    let (ab, ag): (Vec<&Row>, Vec<&Row>) = actual.iter().partition(has_blank);
    let sorted_shapes = |rows: &[&Row]| {
        let mut shapes: Vec<Vec<String>> = rows
            .iter()
            .map(|row| row.iter().map(shape).collect())
            .collect();
        shapes.sort();
        shapes
    };
    // rows without blank nodes must be equal; the others must have the same shapes
    if sorted_shapes(&eg) != sorted_shapes(&ag) || sorted_shapes(&eb) != sorted_shapes(&ab) {
        return None;
    }
    let mut matcher = Matcher {
        forward: HashMap::new(),
        backward: HashMap::new(),
        used: vec![false; ab.len()],
        budget: 2_000_000,
    };
    // on success, `backward` maps every blank node of `actual` (all are in `ab`)
    matcher.search(&eb, &ab, 0).then_some(matcher.backward)
}

/// A backtracking search for a blank-node bijection between rows.
struct Matcher {
    forward: HashMap<BlankNode, BlankNode>,
    backward: HashMap<BlankNode, BlankNode>,
    used: Vec<bool>,
    budget: usize,
}

impl Matcher {
    fn search(&mut self, expected: &[&Row], actual: &[&Row], i: usize) -> bool {
        let Some(row) = expected.get(i) else {
            return true;
        };
        for j in 0..actual.len() {
            if self.used[j] {
                continue;
            }
            if self.budget == 0 {
                return false;
            }
            self.budget -= 1;
            let mut added = Vec::new();
            if self.unify(row, actual[j], &mut added) {
                self.used[j] = true;
                if self.search(expected, actual, i + 1) {
                    return true;
                }
                self.used[j] = false;
            }
            for (e, a) in added {
                self.forward.remove(&e);
                self.backward.remove(&a);
            }
        }
        false
    }

    fn unify(
        &mut self,
        expected: &Row,
        actual: &Row,
        added: &mut Vec<(BlankNode, BlankNode)>,
    ) -> bool {
        for (e, a) in expected.iter().zip(actual) {
            match (e, a) {
                (Some(Term::BlankNode(e)), Some(Term::BlankNode(a))) => {
                    match (self.forward.get(e), self.backward.get(a)) {
                        (None, None) => {
                            self.forward.insert(e.clone(), a.clone());
                            self.backward.insert(a.clone(), e.clone());
                            added.push((e.clone(), a.clone()));
                        }
                        (Some(mapped), _) if mapped == a => {}
                        _ => return false,
                    }
                }
                (Some(Term::BlankNode(_)), _) | (_, Some(Term::BlankNode(_))) => return false,
                (e, a) if e == a => {}
                _ => return false,
            }
        }
        true
    }
}

/// How the top-level `ORDER BY` of a `SELECT` query orders the rows of any correct result: by
/// its keys if they are all projected variables, or else row by row (see [`Order`]).
fn order_of(query: &Query) -> Order {
    let Query::Select { pattern, .. } = query else {
        return Order::Unordered;
    };
    let mut pattern = pattern;
    let mut projected = None;
    loop {
        match pattern {
            GraphPattern::Slice { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner } => pattern = inner,
            // a second projection is a sub-`SELECT`, whose order is not the result's
            GraphPattern::Project { inner, variables } if projected.is_none() => {
                projected = Some(variables);
                pattern = inner;
            }
            GraphPattern::OrderBy { expression, .. } => {
                return expression
                    .iter()
                    .map(|order| {
                        let (OrderExpression::Asc(e) | OrderExpression::Desc(e)) = order;
                        match e {
                            Expression::Variable(v)
                                if projected.is_none_or(|projected| projected.contains(v)) =>
                            {
                                Some(v.as_str().to_owned())
                            }
                            _ => None,
                        }
                    })
                    .collect::<Option<_>>()
                    .map_or(Order::Rows, Order::Keys);
            }
            _ => return Order::Unordered,
        }
    }
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

/// `text` (which parses as `query`) with `FROM <raphtory:asof:T>` inserted before the first `{`
/// or `WHERE` where the result parses as `query` with that clause, or `None` if it already has a
/// dataset clause. The text is edited because spargebra drops brackets of nested arithmetic when
/// writing queries back.
fn with_asof_from(text: &str, query: &Query) -> Option<String> {
    if query.dataset().is_some() {
        return None;
    }
    let mut target = query.clone();
    let dataset = Some(QueryDataset {
        default: vec![asof_graph()],
        named: None,
    });
    match &mut target {
        Query::Select { dataset: d, .. }
        | Query::Construct { dataset: d, .. }
        | Query::Describe { dataset: d, .. }
        | Query::Ask { dataset: d, .. } => *d = dataset,
    }
    let target = fingerprint(&target);
    let lower = text.to_ascii_lowercase();
    let from = format!(" FROM <{ASOF_NS}{T}> ");
    text.char_indices()
        .filter(|&(at, c)| c == '{' || lower[at..].starts_with("where"))
        .map(|(at, _)| at)
        .chain([text.len()])
        .map(|at| format!("{}{from}{}", &text[..at], &text[at..]))
        .find(|candidate| {
            SparqlParser::new()
                .parse_query(candidate)
                .is_ok_and(|q| fingerprint(&q) == target)
        })
}

/// The algebra of a query, with the random names spargebra gives to anonymous blank nodes and
/// hidden variables replaced by their order of appearance.
fn fingerprint(query: &Query) -> String {
    let sse = query.to_sse();
    let mut names: HashMap<&str, usize> = HashMap::new();
    let mut out = String::with_capacity(sse.len());
    let mut rest = sse.as_str();
    while let Some(at) = rest.find(['?', '_']) {
        out.push_str(&rest[..at]);
        let marker = if rest[at..].starts_with("_:") { 2 } else { 1 };
        let token = &rest[at + marker..];
        let len = token
            .find(|c: char| !matches!(c, '0'..='9' | 'a'..='f'))
            .unwrap_or(token.len());
        if len >= 20 {
            let next = names.len();
            let n = *names.entry(&token[..len]).or_insert(next);
            out.push_str(&format!("{}#{n}", &rest[at..at + marker]));
            rest = &token[len..];
        } else {
            out.push_str(&rest[at..at + marker]);
            rest = token;
        }
    }
    out.push_str(rest);
    out
}

fn asof_graph() -> NamedNode {
    NamedNode::new_unchecked(format!("{ASOF_NS}{T}"))
}

// ---------------------------------------------------------------------------------------------
// Expected results
// ---------------------------------------------------------------------------------------------

/// The rows of a CSV document, the header first.
type Csv = Vec<Vec<String>>;

enum Expected {
    /// Results, and whether their rows are in order.
    Results(Results, bool),
    /// SPARQL results CSV, which cannot be read back into terms.
    Csv(Csv),
}

fn extension(url: &str) -> &str {
    url.rsplit_once('.').map_or("", |(_, extension)| extension)
}

/// Reads the expected results of a query of the given form (`None` if the query does not
/// parse).
fn read_expected(files: &Files, url: &str, query: Option<&Query>) -> Result<Expected, String> {
    let graph_form = matches!(
        query,
        Some(Query::Construct { .. } | Query::Describe { .. })
    );
    if extension(url) == "csv" {
        return Ok(Expected::Csv(parse_csv(&files.read_string(url)?)?));
    }
    if let Some(format) = QueryResultsFormat::from_extension(extension(url)) {
        return Ok(Expected::Results(
            parse_results(&files.read(url)?, format).map_err(|e| format!("{url}: {e}"))?,
            true,
        ));
    }
    let triples = read_triples(files, url)?;
    let mut graph = RdfGraph::new();
    for triple in &triples {
        graph.insert(triple);
    }
    let result_set = graph
        .subject_for_predicate_object(rdf::TYPE, &iri(RS, "ResultSet"))
        .is_some();
    if graph_form || !result_set {
        Ok(Expected::Results(Results::Graph(triples), false))
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

/// Decodes results written in the `rs:` vocabulary. The rows are in order if every solution
/// has an `rs:index`.
fn decode_result_set(graph: &RdfGraph) -> Result<Expected, String> {
    let rs = |local| iri(RS, local);
    let set = graph
        .subject_for_predicate_object(rdf::TYPE, &rs("ResultSet"))
        .ok_or("no rs:ResultSet")?;
    let literal = |term: Option<TermRef<'_>>| match term {
        Some(TermRef::Literal(literal)) => Ok(literal.value().to_owned()),
        other => Err(format!("expected a literal, got {other:?}")),
    };
    if let Some(value) = graph.object_for_subject_predicate(set, &rs("boolean")) {
        return Ok(Expected::Results(
            Results::Boolean(literal(Some(value))? == "true"),
            false,
        ));
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
        let index = graph
            .object_for_subject_predicate(solution, &rs("index"))
            .map(|index| {
                literal(Some(index))?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())
            })
            .transpose()?;
        rows.push((index, row));
    }
    let ordered = rows.iter().all(|(index, _)| index.is_some());
    if ordered {
        rows.sort_by_key(|(index, _)| *index);
    }
    let rows = rows.into_iter().map(|(_, row)| row).collect();
    Ok(Expected::Results(
        Results::Solutions { variables, rows },
        ordered,
    ))
}

/// Parses CSV (RFC 4180): fields separated by commas, rows by CRLF or LF, and fields in double
/// quotes (`""` for a quote) can hold commas and line breaks.
fn parse_csv(text: &str) -> Result<Csv, String> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                chars.next();
                field.push('"');
            }
            (true, '"') => quoted = false,
            (true, c) => field.push(c),
            (false, '"') if field.is_empty() => quoted = true,
            (false, ',') => row.push(std::mem::take(&mut field)),
            (false, '\r') if chars.peek() == Some(&'\n') => {}
            (false, '\n') => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            (false, c) => field.push(c),
        }
    }
    if quoted {
        return Err("unterminated quoted CSV field".to_owned());
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
}

/// CSV rows as results: an empty field is unbound, `_:x` a blank node and anything else a
/// plain literal holding the text (CSV does not tell IRIs and literals apart).
fn csv_results(csv: &Csv) -> Results {
    let mut rows = csv.iter();
    let variables = rows.next().cloned().unwrap_or_default();
    let rows = rows
        .map(|row| {
            row.iter()
                .map(|field| match field.strip_prefix("_:") {
                    _ if field.is_empty() => None,
                    Some(label) => Some(BlankNode::new_unchecked(label).into()),
                    None => Some(Literal::new_simple_literal(field).into()),
                })
                .collect()
        })
        .collect();
    Results::Solutions { variables, rows }
}

/// Writes results as CSV with sparesults (not with Raphtory's writer).
fn to_csv(results: &Results) -> Result<Csv, String> {
    let serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Csv);
    let bytes = match results {
        Results::Boolean(value) => serializer
            .serialize_boolean_to_writer(Vec::new(), *value)
            .map_err(|e| e.to_string())?,
        Results::Solutions { variables, rows } => {
            let variables: Vec<_> = variables
                .iter()
                .map(|v| raphtory::rdf::Variable::new_unchecked(v.clone()))
                .collect();
            let mut writer = serializer
                .serialize_solutions_to_writer(Vec::new(), variables.clone())
                .map_err(|e| e.to_string())?;
            for row in rows {
                writer
                    .serialize(
                        variables
                            .iter()
                            .zip(row)
                            .filter_map(|(v, value)| Some((v, value.as_ref()?))),
                    )
                    .map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())?
        }
        Results::Graph(_) => return Err("a graph cannot be written as CSV".to_owned()),
    };
    parse_csv(&String::from_utf8(bytes).map_err(|e| e.to_string())?)
}

fn compare_csv(expected: &Csv, actual: &Csv, rules: &Rules) -> Result<(), String> {
    compare(&csv_results(expected), &csv_results(actual), rules)
}

/// Results with every number (an `xsd:integer` and its subtypes, `xsd:decimal`, `xsd:double`
/// or `xsd:float` literal) in one canonical lexical form, to tell results that only differ in
/// how numbers are written.
fn with_canonical_numbers(results: &Results) -> Results {
    let term = |term: &Term| match term {
        Term::Literal(literal) => {
            canonical_number(literal).map_or_else(|| term.clone(), Term::from)
        }
        _ => term.clone(),
    };
    match results {
        Results::Solutions { variables, rows } => Results::Solutions {
            variables: variables.clone(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|value| value.as_ref().map(term)).collect())
                .collect(),
        },
        Results::Boolean(value) => Results::Boolean(*value),
        Results::Graph(triples) => Results::Graph(
            triples
                .iter()
                .map(|t| Triple::new(t.subject.clone(), t.predicate.clone(), term(&t.object)))
                .collect(),
        ),
    }
}

fn canonical_number(literal: &Literal) -> Option<Literal> {
    let datatype = literal.datatype();
    let local = datatype
        .as_str()
        .strip_prefix("http://www.w3.org/2001/XMLSchema#")?;
    let value = literal.value().trim();
    let canonical = match local {
        "decimal" => bigdecimal::BigDecimal::from_str(value)
            .ok()?
            .normalized()
            .to_string(),
        "double" => format!("{:e}", value.parse::<f64>().ok()?),
        "float" => format!("{:e}", value.parse::<f32>().ok()?),
        "integer" | "long" | "int" | "short" | "byte" | "nonNegativeInteger"
        | "positiveInteger" | "nonPositiveInteger" | "negativeInteger" | "unsignedLong"
        | "unsignedInt" | "unsignedShort" | "unsignedByte" => {
            value.parse::<i128>().ok()?.to_string()
        }
        _ => return None,
    };
    Some(Literal::new_typed_literal(canonical, datatype.into_owned()))
}

// ---------------------------------------------------------------------------------------------
// Running tests
// ---------------------------------------------------------------------------------------------

/// The ways a query evaluation test is run. Each must give the expected result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Mode {
    /// An event `Graph` with the data asserted at `T`.
    Events,
    /// A `PersistentGraph` with the data asserted at `T`, queried through `snapshot_at(T)`.
    Snapshot,
    /// As `Snapshot`, with decoy history around the data that is not visible as of `T` (see
    /// [`add_decoys`]).
    Decoys,
    /// The graph of `Decoys` itself, not a snapshot, with `FROM <raphtory:asof:T>` added to the
    /// query.
    AsOfFrom,
    /// `Events`, with the results written by `sparql_to_writer` in the format of the expected
    /// results (SPARQL XML, JSON, TSV or CSV, or the RDF format of an expected graph) and read
    /// back.
    Serialized,
    /// A syntax test (one run).
    Syntax,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Events => "events",
            Self::Snapshot => "snapshot",
            Self::Decoys => "decoys",
            Self::AsOfFrom => "asof-from",
            Self::Serialized => "serialized",
            Self::Syntax => "syntax",
        })
    }
}

/// The outcome of one mode of a test.
#[derive(Debug)]
enum Outcome {
    Pass,
    /// The result is not the expected one, and the oracle gives the same result: the failure
    /// comes from spareval or the test suite. `same_values` if the results only differ in the
    /// lexical forms of numbers (see [`with_canonical_numbers`]).
    Upstream {
        diff: String,
        same_values: bool,
    },
    /// The result is not the expected one, and the oracle gives another: a bug in Raphtory.
    Bug(String),
    Skip(String),
}

/// What happened to a test.
#[derive(Debug)]
enum TestRun {
    Skipped(String),
    Ran(BTreeMap<Mode, Outcome>),
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
    let result = if test.is(MF, "QueryEvaluationTest") || test.is(MF, "CSVResultFormatTest") {
        Eval::new(files, test).map(|eval| match eval {
            Ok(eval) => eval.run(),
            Err(skip) => TestRun::Skipped(skip),
        })
    } else if test.is(MF, "PositiveSyntaxTest") || test.is(MF, "PositiveSyntaxTest11") {
        syntax_test(files, test, true)
    } else if test.is(MF, "NegativeSyntaxTest") || test.is(MF, "NegativeSyntaxTest11") {
        syntax_test(files, test, false)
    } else if test.is(RDFT, "TestTurtleEval") {
        turtle_eval_test(files, test)
    } else if test.is(RDFT, "TestTurtlePositiveSyntax") {
        turtle_syntax_test(files, test, true)
    } else if test.is(RDFT, "TestTurtleNegativeSyntax") {
        turtle_syntax_test(files, test, false)
    } else {
        let kind = test.kinds.first().map_or("none", String::as_str);
        let kind = kind.rsplit(['#', '/']).next().unwrap_or(kind);
        Ok(TestRun::Skipped(format!("test type {kind}")))
    };
    result.unwrap_or_else(TestRun::Error)
}

/// A query evaluation test: its query, data and expected results, and the graphs of the
/// modes (built when first used).
struct Eval<'a> {
    test: &'a TestCase,
    /// The query with `BASE <query URL>` in front.
    query: String,
    parsed: Option<Query>,
    /// The query with `FROM <raphtory:asof:T>`.
    asof_query: Option<String>,
    /// The data files: URL, content and format.
    data: Vec<(String, Vec<u8>, RdfFormat)>,
    expected: Expected,
    /// How solutions are compared: the order of the query (see [`order_of`]) and the
    /// cardinality of the test.
    rules: Rules,
    events: OnceCell<Result<Graph, String>>,
    persistent: OnceCell<Result<PersistentGraph, String>>,
    decoys: OnceCell<Result<PersistentGraph, String>>,
}

impl<'a> Eval<'a> {
    /// The test, or `Ok(Err(reason))` if it is skipped.
    fn new(files: &'a Files, test: &'a TestCase) -> Result<Result<Self, String>, String> {
        if test.graph_data {
            return Ok(Err("named graphs (qt:graphData)".to_owned()));
        }
        if test.service_data {
            return Ok(Err("SERVICE (qt:serviceData)".to_owned()));
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
            test,
            asof_query: parsed.as_ref().and_then(|q| with_asof_from(&query, q)),
            rules: Rules {
                order: parsed.as_ref().map(order_of).unwrap_or_default(),
                lax: test.lax,
            },
            query,
            parsed,
            data,
            expected,
            events: OnceCell::new(),
            persistent: OnceCell::new(),
            decoys: OnceCell::new(),
        }))
    }

    fn run(&self) -> TestRun {
        let modes = [
            Mode::Events,
            Mode::Snapshot,
            Mode::Decoys,
            Mode::AsOfFrom,
            Mode::Serialized,
        ];
        TestRun::Ran(modes.into_iter().map(|m| (m, self.outcome(m))).collect())
    }

    fn load<G: RdfMutationOps>(&self, g: &G) -> Result<(), String> {
        for (url, data, format) in &self.data {
            g.load_rdf(T, data.as_slice(), *format, Some(url.as_str()))
                .map_err(|e| format!("cannot load {url}: {e}"))?;
        }
        Ok(())
    }

    fn events(&self) -> Result<&Graph, String> {
        let g = self.events.get_or_init(|| {
            let g = Graph::new();
            self.load(&g).map(|()| g)
        });
        g.as_ref().map_err(Clone::clone)
    }

    fn persistent(&self) -> Result<&PersistentGraph, String> {
        let g = self.persistent.get_or_init(|| {
            let g = PersistentGraph::new();
            self.load(&g).map(|()| g)
        });
        g.as_ref().map_err(Clone::clone)
    }

    fn decoys(&self) -> Result<&PersistentGraph, String> {
        let g = self.decoys.get_or_init(|| {
            let g = PersistentGraph::new();
            self.load(&g)?;
            add_decoys(&g).map_err(|e| format!("cannot add decoys: {e}"))?;
            Ok(g)
        });
        g.as_ref().map_err(Clone::clone)
    }

    /// Raphtory's results in a mode other than `Serialized`.
    fn ours(&self, mode: Mode) -> Result<Results, String> {
        fn run<G: RdfViewOps>(view: &G, query: &str) -> Result<Results, String> {
            view.sparql(query)
                .map(Results::from)
                .map_err(|e| e.to_string())
        }
        match mode {
            Mode::Events => run(self.events()?, &self.query),
            Mode::Snapshot => run(&self.persistent()?.snapshot_at(T), &self.query),
            Mode::Decoys => run(&self.decoys()?.snapshot_at(T), &self.query),
            Mode::AsOfFrom => {
                let query = self.asof_query.as_ref().ok_or("no query with FROM")?;
                run(self.decoys()?, query)
            }
            Mode::Serialized | Mode::Syntax => unreachable!(),
        }
    }

    /// The results of `sparql_to_writer` on the graph of `Events`, in the format of the
    /// expected results, read back: as results, or as CSV rows.
    fn serialized(&self) -> Result<Result<Results, Csv>, String> {
        let g = self.events()?;
        let mut out = Vec::new();
        let write = |e: GraphError| format!("sparql_to_writer: {e}");
        if let Expected::Csv(_) = self.expected {
            g.sparql_to_writer(&self.query, &mut out, QueryResultsFormat::Csv)
                .map_err(write)?;
            return Ok(Err(parse_csv(
                &String::from_utf8(out).map_err(|e| e.to_string())?,
            )?));
        }
        let result_url = self.test.result.as_deref().unwrap_or_default();
        match &self.parsed {
            Some(Query::Construct { .. } | Query::Describe { .. }) => {
                let format = format_of(result_url).unwrap_or(RdfFormat::Turtle);
                g.sparql_to_writer(&self.query, &mut out, format)
                    .map_err(write)?;
                Ok(Ok(Results::Graph(parse_triples(&out, format, TESTS_URL)?)))
            }
            _ => {
                let format = QueryResultsFormat::from_extension(extension(result_url))
                    .unwrap_or(QueryResultsFormat::Json);
                g.sparql_to_writer(&self.query, &mut out, format)
                    .map_err(write)?;
                Ok(Ok(parse_results(&out, format)?))
            }
        }
    }

    /// The oracle's results for the query of `mode`: spareval over an `oxrdf::Dataset` of the
    /// data (in the time graph for `AsOfFrom`).
    fn oracle(&self, mode: Mode) -> Result<Results, String> {
        let (graph, query) = match mode {
            Mode::AsOfFrom => (
                GraphName::from(asof_graph()),
                self.asof_query.as_ref().ok_or("no query with FROM")?,
            ),
            _ => (GraphName::DefaultGraph, &self.query),
        };
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
                    graph.clone(),
                ));
            }
        }
        let results = evaluator()
            .parse_query(query)
            .map_err(|e| e.to_string())?
            .on_queryable_dataset(&dataset)
            .execute()
            .map_err(|e| e.to_string())?;
        SparqlResults::from_query_results(results)
            .map(Results::from)
            .map_err(|e| e.to_string())
    }

    fn outcome(&self, mode: Mode) -> Outcome {
        if mode == Mode::AsOfFrom && self.asof_query.is_none() {
            return Outcome::Skip(match self.parsed {
                None => "the query does not parse".to_owned(),
                Some(_) => "FROM cannot be added to the query text".to_owned(),
            });
        }
        // Raphtory's results, and how they compare with the expected ones
        let rules = self.rules();
        let (actual, check) = if mode == Mode::Serialized {
            match self.serialized() {
                Ok(Ok(results)) => {
                    let check = self.check(&results, &rules);
                    (Ok(results), check)
                }
                Ok(Err(csv)) => {
                    let check = match &self.expected {
                        Expected::Csv(expected) => compare_csv(expected, &csv, &rules),
                        Expected::Results(..) => Err("written as CSV".to_owned()),
                    };
                    (Err("written as CSV".to_owned()), check)
                }
                Err(e) => (Err(e.clone()), Err(e)),
            }
        } else {
            let actual = self.ours(mode);
            let check = actual
                .as_ref()
                .map_err(Clone::clone)
                .and_then(|actual| self.check(actual, &rules));
            (actual, check)
        };
        match check {
            Ok(()) => Outcome::Pass,
            Err(diff) => self.triage(mode, actual, diff),
        }
    }

    /// The rules to compare results with the expected ones: without the order if the expected
    /// rows are in no order.
    fn rules(&self) -> Rules {
        match &self.expected {
            Expected::Csv(_) => self.rules.clone(),
            Expected::Results(_, ordered) => self.rules.ordered_if(*ordered),
        }
    }

    /// Compares results with the expected ones, written as CSV if those are CSV.
    fn check(&self, actual: &Results, rules: &Rules) -> Result<(), String> {
        match &self.expected {
            Expected::Csv(expected) => compare_csv(expected, &to_csv(actual)?, rules),
            Expected::Results(expected, _) => compare(expected, actual, rules),
        }
    }

    /// Tells a bug of Raphtory (its result differs from the oracle's) from a failure that
    /// comes from spareval or the test suite (the same result as the oracle).
    ///
    /// An `ORDER BY` checked row by row ([`Order::Rows`]) is the exception: if Raphtory gives the
    /// expected solutions in another sequence, it is a bug only if the oracle gives the expected
    /// sequence.
    fn triage(&self, mode: Mode, actual: Result<Results, String>, diff: String) -> Outcome {
        let rules = self.rules();
        let (ours, oracle_mode) = if mode == Mode::Serialized {
            // the written results must read back as the results of `sparql()`, exactly
            let exact = Rules {
                lax: false,
                ..rules.clone()
            };
            let direct = self.ours(Mode::Events);
            let faithful = match (&direct, self.serialized()) {
                (Ok(direct), Ok(Ok(read_back))) => compare(direct, &read_back, &exact).is_ok(),
                (Ok(direct), Ok(Err(csv))) => to_csv(direct)
                    .and_then(|written| compare_csv(&written, &csv, &exact))
                    .is_ok(),
                (Err(_), Err(_)) => true,
                _ => false,
            };
            if !faithful {
                return Outcome::Bug(format!(
                    "sparql_to_writer does not write the results of sparql(): {diff}"
                ));
            }
            (direct, Mode::Events)
        } else {
            (actual, mode)
        };
        let oracle = self.oracle(oracle_mode);
        let same_values = match (&self.expected, &ours) {
            (Expected::Results(expected, _), Ok(ours)) => compare(
                &with_canonical_numbers(expected),
                &with_canonical_numbers(ours),
                &rules,
            )
            .is_ok(),
            _ => false,
        };
        if let (Order::Rows, Ok(ours), Ok(oracle)) = (&rules.order, &ours, &oracle) {
            if self.check(ours, &rules.unordered()).is_ok() {
                return if self.check(oracle, &rules).is_ok() {
                    Outcome::Bug(format!("{diff}; the oracle gives the expected order"))
                } else {
                    Outcome::Upstream { diff, same_values }
                };
            }
        }
        match (&ours, &oracle) {
            (Ok(ours), Ok(oracle)) if compare(oracle, ours, &rules).is_ok() => {
                Outcome::Upstream { diff, same_values }
            }
            (Err(_), Err(_)) => Outcome::Upstream { diff, same_values },
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

/// Adds history around the data of `pg` (asserted at `T`) that is not visible as of `T`: for
/// every triple `s p o`, `s p <decoy>` and `s <decoy> o` asserted at 5 and retracted at 7,
/// `<decoy> p o` asserted at 20, and the triple itself asserted at 3, retracted at 5 and
/// retracted again at 20; and a triple of decoy terms asserted at 5 and retracted at 7.
fn add_decoys(pg: &PersistentGraph) -> Result<(), GraphError> {
    let mut doc = Vec::new();
    pg.to_rdf(&mut doc, RdfFormat::NTriples)?;
    let triples = parse_triples(&doc, RdfFormat::NTriples, TESTS_URL)
        .expect("to_rdf writes N-Triples that parse");
    let decoy = |local: String| NamedNode::new_unchecked(format!("{DECOY}{local}"));
    let gone = Triple::new(decoy("s".into()), decoy("p".into()), decoy("o".into()));
    pg.add_triple(5, &gone)?;
    pg.delete_triple(7, &gone)?;
    for (i, t) in triples.iter().enumerate() {
        let other_object = Triple::new(
            t.subject.clone(),
            t.predicate.clone(),
            decoy(format!("o{i}")),
        );
        let other_layer = Triple::new(t.subject.clone(), decoy("p".into()), t.object.clone());
        for gone in [&other_object, &other_layer] {
            pg.add_triple(5, gone)?;
            pg.delete_triple(7, gone)?;
        }
        let later = Triple::new(
            decoy(format!("s{i}")),
            t.predicate.clone(),
            t.object.clone(),
        );
        pg.add_triple(20, &later)?;
        pg.add_triple(3, t)?;
        pg.delete_triple(5, t)?;
        pg.delete_triple(20, t)?;
    }
    Ok(())
}

/// A syntax test: `sparql()` must accept the query (`positive`) or reject it with a syntax
/// error. The oracle is spargebra's parser.
fn syntax_test(files: &Files, test: &TestCase, positive: bool) -> Result<TestRun, String> {
    let url = test.action.as_deref().ok_or("no mf:action")?;
    // a file that cannot be read is an error of the test, not a rejection of the query
    let outcome = match String::from_utf8(files.read(url)?) {
        Ok(text) => {
            let query = format!("BASE <{url}>\n{text}");
            let rejected = matches!(
                Graph::new().sparql(&query),
                Err(GraphError::Rdf(
                    RdfError::SparqlSyntax(_) | RdfError::SparqlTooDeep { .. }
                ))
            );
            let oracle_rejects = SparqlParser::new().parse_query(&query).is_err();
            if rejected != positive {
                Outcome::Pass
            } else {
                let diff = if positive {
                    "a valid query is rejected"
                } else {
                    "an invalid query is accepted"
                };
                if rejected == oracle_rejects {
                    Outcome::Upstream {
                        diff: diff.to_owned(),
                        same_values: false,
                    }
                } else {
                    Outcome::Bug(format!("{diff}, but not by spargebra's parser"))
                }
            }
        }
        // a query that is not UTF-8 cannot be passed to `sparql()`, which is a rejection
        Err(_) if !positive => Outcome::Pass,
        Err(e) => return Err(format!("{url} is not UTF-8: {e}")),
    };
    Ok(TestRun::Ran([(Mode::Syntax, outcome)].into()))
}

/// A Turtle evaluation test: loaded with `load_rdf` and written with `to_rdf`, the document
/// must give the expected N-Triples. The oracle is the Turtle parser.
fn turtle_eval_test(files: &Files, test: &TestCase) -> Result<TestRun, String> {
    let url = test.action.as_deref().ok_or("no mf:action")?;
    let doc = files.read(url)?;
    let expected = read_triples(files, test.result.as_deref().ok_or("no mf:result")?)?;
    let oracle = parse_triples(&doc, RdfFormat::Turtle, url);
    fn export<G: RdfViewOps>(view: &G) -> Result<Vec<Triple>, String> {
        let mut out = Vec::new();
        let stats = view
            .to_rdf(&mut out, RdfFormat::NTriples)
            .map_err(|e| e.to_string())?;
        if stats.skipped > 0 {
            return Err(format!("to_rdf skipped {} triples", stats.skipped));
        }
        parse_triples(&out, RdfFormat::NTriples, TESTS_URL)
    }
    let mut outcomes = BTreeMap::new();
    for mode in [Mode::Events, Mode::Snapshot] {
        let loaded = |e: GraphError| format!("cannot load {url}: {e}");
        let ours = match mode {
            Mode::Events => {
                let g = Graph::new();
                g.load_rdf(T, doc.as_slice(), RdfFormat::Turtle, Some(url))
                    .map_err(loaded)
                    .and_then(|_| export(&g))
            }
            _ => {
                let g = PersistentGraph::new();
                g.load_rdf(T, doc.as_slice(), RdfFormat::Turtle, Some(url))
                    .map_err(loaded)
                    .and_then(|_| export(&g.snapshot_at(T)))
            }
        };
        let outcome = match ours
            .clone()
            .and_then(|ours| compare_graphs(&expected, &ours))
        {
            Ok(()) => Outcome::Pass,
            Err(diff) => match (&ours, &oracle) {
                (Ok(ours), Ok(oracle)) if compare_graphs(oracle, ours).is_ok() => {
                    Outcome::Upstream {
                        diff,
                        same_values: false,
                    }
                }
                (Err(_), Err(_)) => Outcome::Upstream {
                    diff,
                    same_values: false,
                },
                _ => Outcome::Bug(diff),
            },
        };
        outcomes.insert(mode, outcome);
    }
    Ok(TestRun::Ran(outcomes))
}

/// A Turtle syntax test: `load_rdf` must load the document (`positive`) or fail.
fn turtle_syntax_test(files: &Files, test: &TestCase, positive: bool) -> Result<TestRun, String> {
    let url = test.action.as_deref().ok_or("no mf:action")?;
    let doc = files.read(url)?;
    let loaded = Graph::new()
        .load_rdf(T, doc.as_slice(), RdfFormat::Turtle, Some(url))
        .is_ok();
    let oracle_loads = parse_triples(&doc, RdfFormat::Turtle, url).is_ok();
    let outcome = if loaded == positive {
        Outcome::Pass
    } else {
        let diff = if positive {
            "a valid document does not load"
        } else {
            "an invalid document loads"
        };
        if loaded == oracle_loads {
            Outcome::Upstream {
                diff: diff.to_owned(),
                same_values: false,
            }
        } else {
            Outcome::Bug(format!("{diff}, unlike with the Turtle parser"))
        }
    };
    Ok(TestRun::Ran([(Mode::Syntax, outcome)].into()))
}

// ---------------------------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------------------------

/// The counts of one mode.
#[derive(Default)]
struct Counts {
    pass: usize,
    known: usize,
    fail: usize,
    skipped: BTreeMap<String, usize>,
}

/// What a suite gave.
#[derive(Default)]
struct Report {
    suite: String,
    tests: usize,
    /// Tests skipped entirely, by reason.
    skipped: BTreeMap<String, usize>,
    modes: BTreeMap<Mode, Counts>,
    /// The known failures that failed as expected, with the reason and whether every failing
    /// mode gave the expected values (with numbers written in other lexical forms).
    known: Vec<(String, String, bool)>,
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
            TestRun::Ran(outcomes) => {
                let mut failed_as_known = false;
                let mut all_pass = true;
                let mut same_values = true;
                for (mode, outcome) in outcomes {
                    let counts = self.modes.entry(mode).or_default();
                    all_pass &= matches!(outcome, Outcome::Pass | Outcome::Skip(_));
                    match outcome {
                        Outcome::Pass => counts.pass += 1,
                        Outcome::Skip(reason) => *counts.skipped.entry(reason).or_default() += 1,
                        Outcome::Upstream {
                            same_values: same, ..
                        } if listed.is_some() => {
                            counts.known += 1;
                            failed_as_known = true;
                            same_values &= same;
                        }
                        Outcome::Upstream { diff, .. } => {
                            counts.fail += 1;
                            self.problems.push(format!(
                                "{id} [{mode}]: {}; the oracle (spareval over an oxrdf Dataset) \
                                 gives the same result, so list it in KNOWN_FAILURES with the \
                                 reason",
                                truncate(&diff)
                            ));
                        }
                        Outcome::Bug(diff) => {
                            counts.fail += 1;
                            self.problems.push(format!(
                                "{id} [{mode}]: BUG, Raphtory differs from the oracle: {}",
                                truncate(&diff)
                            ));
                        }
                    }
                }
                match listed {
                    Some(reason) if failed_as_known => {
                        self.known
                            .push((id.clone(), reason.to_owned(), same_values))
                    }
                    Some(reason) if all_pass => self.problems.push(format!(
                        "{id} is listed in KNOWN_FAILURES ({reason}) but passes in every mode: \
                         remove it from the list"
                    )),
                    _ => {}
                }
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
            "== W3C suite {}: {} tests, {} skipped",
            self.suite, self.tests, skipped
        )?;
        for (reason, n) in &self.skipped {
            writeln!(f, "   skipped: {n} x {reason}")?;
        }
        writeln!(
            f,
            "   {:<11} {:>5} {:>6} {:>5} {:>8}",
            "mode", "pass", "known", "fail", "skipped"
        )?;
        for (mode, counts) in &self.modes {
            let skipped: usize = counts.skipped.values().sum();
            writeln!(
                f,
                "   {:<11} {:>5} {:>6} {:>5} {:>8}",
                mode.to_string(),
                counts.pass,
                counts.known,
                counts.fail,
                skipped
            )?;
            for (reason, n) in &counts.skipped {
                writeln!(f, "      {mode} skipped: {n} x {reason}")?;
            }
        }
        if !self.known.is_empty() {
            let same_values = self.known.iter().filter(|(_, _, same)| *same).count();
            writeln!(
                f,
                "   known failures ({}, of which {same_values} give the expected values with \
                 numbers written in another lexical form):",
                self.known.len()
            )?;
            for (id, reason, same) in &self.known {
                let same = if *same { " [same values]" } else { "" };
                writeln!(f, "      {id}{same}: {reason}")?;
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

// ---------------------------------------------------------------------------------------------
// Self-tests of the harness, which run without the W3C suites
// ---------------------------------------------------------------------------------------------

/// A small suite in the layout of w3c/rdf-tests, with tests that pass, fail and are skipped.
const MINI_SUITE: &[(&str, &str)] = &[
    (
        "sparql/mini/manifest.ttl",
        r#"
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix qt: <http://www.w3.org/2001/sw/DataAccess/tests/test-query#> .
@prefix : <http://example.org/mini#> .

<> a mf:Manifest ;
    mf:include ( <sub/manifest.ttl> ) ;
    mf:entries ( :select :ask :construct :rs :wrong :named :from :syntax-ok :syntax-bad :csv
        :lax :order :wrong-order ) .

:select a mf:QueryEvaluationTest ;
    mf:action [ qt:query <select.rq> ; qt:data <data.ttl> ] ; mf:result <select.srx> .
:ask a mf:QueryEvaluationTest ;
    mf:action [ qt:query <ask.rq> ; qt:data <data.ttl> ] ; mf:result <ask.srj> .
:construct a mf:QueryEvaluationTest ;
    mf:action [ qt:query <construct.rq> ; qt:data <data.ttl> ] ; mf:result <construct.ttl> .
:rs a mf:QueryEvaluationTest ;
    mf:action [ qt:query <rs.rq> ; qt:data <data.ttl> ] ; mf:result <rs.ttl> .
:wrong a mf:QueryEvaluationTest ;
    mf:action [ qt:query <select.rq> ; qt:data <data.ttl> ] ; mf:result <wrong.srx> .
:named a mf:QueryEvaluationTest ;
    mf:action [ qt:query <select.rq> ; qt:graphData <data.ttl> ] ; mf:result <select.srx> .
:from a mf:QueryEvaluationTest ;
    mf:action [ qt:query <from.rq> ; qt:data <data.ttl> ] ; mf:result <select.srx> .
:syntax-ok a mf:PositiveSyntaxTest11 ; mf:action <syntax-ok.rq> .
:syntax-bad a mf:NegativeSyntaxTest11 ; mf:action <syntax-bad.rq> .
:csv a mf:CSVResultFormatTest ;
    mf:action [ qt:query <select.rq> ; qt:data <data.ttl> ] ; mf:result <select.csv> .
:lax a mf:QueryEvaluationTest ; mf:resultCardinality mf:LaxCardinality ;
    mf:action [ qt:query <distinct.rq> ; qt:data <data.ttl> ] ; mf:result <lax.srx> .
:order a mf:QueryEvaluationTest ;
    mf:action [ qt:query <sum.rq> ; qt:data <numbers.ttl> ] ; mf:result <order.srx> .
:wrong-order a mf:QueryEvaluationTest ;
    mf:action [ qt:query <sum.rq> ; qt:data <numbers.ttl> ] ; mf:result <wrong-order.srx> .
"#,
    ),
    (
        "sparql/mini/sub/manifest.ttl",
        r#"
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
<> a mf:Manifest ; mf:entries ( <#included> ) .
<#included> a mf:PositiveSyntaxTest ; mf:action <../syntax-ok.rq> .
"#,
    ),
    (
        "sparql/mini/data.ttl",
        r#"
@prefix : <http://example.org/> .
:a :knows :b , _:x .
_:x :name "x" .
:b :name "Bob"@en ; :age 42 .
<rel> :p :a .
"#,
    ),
    (
        "sparql/mini/select.rq",
        "PREFIX : <http://example.org/> SELECT ?s ?o { ?s :knows ?o } ORDER BY ?o",
    ),
    (
        "sparql/mini/select.srx",
        r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head><variable name="s"/><variable name="o"/></head>
<results>
<result><binding name="s"><uri>http://example.org/a</uri></binding><binding name="o"><bnode>b0</bnode></binding></result>
<result><binding name="s"><uri>http://example.org/a</uri></binding><binding name="o"><uri>http://example.org/b</uri></binding></result>
</results>
</sparql>"#,
    ),
    (
        "sparql/mini/wrong.srx",
        r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head><variable name="s"/><variable name="o"/></head>
<results>
<result><binding name="s"><uri>http://example.org/a</uri></binding><binding name="o"><uri>http://example.org/b</uri></binding></result>
</results>
</sparql>"#,
    ),
    (
        "sparql/mini/select.csv",
        "s,o\r\nhttp://example.org/a,_:b0\r\nhttp://example.org/a,http://example.org/b\r\n",
    ),
    (
        "sparql/mini/ask.rq",
        "ASK { <http://example.org/b> <http://example.org/age> 42 }",
    ),
    ("sparql/mini/ask.srj", r#"{"head":{},"boolean":true}"#),
    (
        "sparql/mini/construct.rq",
        "PREFIX : <http://example.org/> CONSTRUCT { ?o :knownBy ?s } WHERE { ?s :knows ?o }",
    ),
    (
        "sparql/mini/construct.ttl",
        "@prefix : <http://example.org/> . :b :knownBy :a . _:y :knownBy :a .",
    ),
    (
        "sparql/mini/rs.rq",
        "PREFIX : <http://example.org/> SELECT ?s ?n { { ?s :name ?n } UNION { ?s :p ?n } }",
    ),
    (
        "sparql/mini/rs.ttl",
        r#"
@prefix rs: <http://www.w3.org/2001/sw/DataAccess/tests/result-set#> .
@prefix : <http://example.org/> .
[] a rs:ResultSet ; rs:resultVariable "s", "n" ;
    rs:solution [ rs:binding [ rs:variable "s" ; rs:value _:z ], [ rs:variable "n" ; rs:value "x" ] ] ,
        [ rs:binding [ rs:variable "s" ; rs:value :b ], [ rs:variable "n" ; rs:value "Bob"@en ] ] ,
        [ rs:binding [ rs:variable "s" ; rs:value <rel> ], [ rs:variable "n" ; rs:value :a ] ] .
"#,
    ),
    (
        "sparql/mini/from.rq",
        "SELECT * FROM <data.ttl> { ?s ?p ?o }",
    ),
    (
        "sparql/mini/distinct.rq",
        "PREFIX : <http://example.org/> SELECT DISTINCT ?s { ?s :knows ?o }",
    ),
    // a solution twice, where DISTINCT gives it once: a pass with lax cardinality only
    (
        "sparql/mini/lax.srx",
        r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head><variable name="s"/></head>
<results>
<result><binding name="s"><uri>http://example.org/a</uri></binding></result>
<result><binding name="s"><uri>http://example.org/a</uri></binding></result>
</results>
</sparql>"#,
    ),
    (
        "sparql/mini/numbers.ttl",
        "@prefix : <http://example.org/> . :s1 :p 1 ; :q 1 . :s2 :p 2 ; :q 3 . :s3 :p 0 ; :q 4 .",
    ),
    // ordered by an expression of variables that are not projected
    (
        "sparql/mini/sum.rq",
        "PREFIX : <http://example.org/> SELECT ?s { ?s :p ?a ; :q ?b } ORDER BY (?a + ?b)",
    ),
    (
        "sparql/mini/order.srx",
        r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head><variable name="s"/></head>
<results>
<result><binding name="s"><uri>http://example.org/s1</uri></binding></result>
<result><binding name="s"><uri>http://example.org/s3</uri></binding></result>
<result><binding name="s"><uri>http://example.org/s2</uri></binding></result>
</results>
</sparql>"#,
    ),
    // the right solutions in the wrong order
    (
        "sparql/mini/wrong-order.srx",
        r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head><variable name="s"/></head>
<results>
<result><binding name="s"><uri>http://example.org/s1</uri></binding></result>
<result><binding name="s"><uri>http://example.org/s2</uri></binding></result>
<result><binding name="s"><uri>http://example.org/s3</uri></binding></result>
</results>
</sparql>"#,
    ),
    ("sparql/mini/syntax-ok.rq", "SELECT * { ?s ?p ?o }"),
    ("sparql/mini/syntax-bad.rq", "SELECT * { ?s ?p ?o "),
    (
        "rdf/mini/manifest.ttl",
        r#"
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix rdft: <http://www.w3.org/ns/rdftest#> .
<> a mf:Manifest ; mf:entries ( <#eval> <#bad> ) .
<#eval> a rdft:TestTurtleEval ; mf:action <in.ttl> ; mf:result <out.nt> .
<#bad> a rdft:TestTurtleNegativeSyntax ; mf:action <bad.ttl> .
"#,
    ),
    (
        "rdf/mini/in.ttl",
        "@prefix : <http://example.org/> . :s :p ( 1 \"two\"@en ) ; :q [ :r <rel> ] .",
    ),
    (
        "rdf/mini/out.nt",
        r#"<http://example.org/s> <http://example.org/p> _:l1 .
_:l1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:l1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:l2 .
_:l2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> "two"@en .
_:l2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
<http://example.org/s> <http://example.org/q> _:b .
_:b <http://example.org/r> <https://w3c.github.io/rdf-tests/rdf/mini/rel> .
"#,
    ),
    (
        "rdf/mini/bad.ttl",
        "<http://example.org/s> <http://example.org/p> .",
    ),
];

fn mini_suite() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (path, content) in MINI_SUITE {
        let path = dir.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    dir
}

const MINI: &str = "http://example.org/mini#";

/// The harness passes, skips and fails the tests of a small suite as it should.
#[test]
fn harness_runs_a_suite() {
    let dir = mini_suite();
    let mut tests = Vec::new();
    let files = Files {
        root: dir.path().to_owned(),
    };
    read_manifest(
        &files,
        &format!("{TESTS_URL}sparql/mini/manifest.ttl"),
        &mut tests,
        0,
    )
    .unwrap();
    let lax: Vec<_> = tests
        .iter()
        .filter(|t| t.lax)
        .map(|t| t.id.as_str())
        .collect();
    assert_eq!(lax, [format!("{MINI}lax")]);

    let report = run_suite(dir.path(), "sparql/mini/manifest.ttl", &[]);
    println!("{report}");
    assert_eq!(report.tests, 14);
    let skipped: Vec<_> = report
        .skipped
        .iter()
        .map(|(r, n)| (r.as_str(), *n))
        .collect();
    assert_eq!(
        skipped,
        [("FROM / FROM NAMED", 1), ("named graphs (qt:graphData)", 1)]
    );
    // select, ask, construct, rs, csv, lax and order pass in every mode; wrong and wrong-order
    // fail in every mode
    let eval_modes = [
        Mode::Events,
        Mode::Snapshot,
        Mode::Decoys,
        Mode::AsOfFrom,
        Mode::Serialized,
    ];
    for mode in eval_modes {
        let counts = &report.modes[&mode];
        assert_eq!(
            (counts.pass, counts.known, counts.fail),
            (7, 0, 2),
            "{mode}"
        );
    }
    let syntax = &report.modes[&Mode::Syntax];
    assert_eq!((syntax.pass, syntax.fail), (3, 0));
    // the failures of `wrong` and `wrong-order` are the oracle's too, so they ask to be listed
    assert_eq!(
        report.problems.len(),
        2 * eval_modes.len(),
        "{:?}",
        report.problems
    );
    assert!(report
        .problems
        .iter()
        .all(|p| p.starts_with(&format!("{MINI}wrong")) && p.contains("KNOWN_FAILURES")));

    // listed, they are known failures; a listed test that passes is a problem
    let (wrong, wrong_order, select) = (
        format!("{MINI}wrong"),
        format!("{MINI}wrong-order"),
        format!("{MINI}select"),
    );
    let known = [
        (wrong.as_str(), "the expected result is wrong"),
        (wrong_order.as_str(), "the expected order is wrong"),
        (select.as_str(), "listed by mistake"),
    ];
    let report = run_suite(dir.path(), "sparql/mini/manifest.ttl", &known);
    let known: Vec<_> = report.known.iter().map(|(id, _, _)| id.as_str()).collect();
    assert_eq!(known, [wrong.as_str(), wrong_order.as_str()]);
    assert_eq!(report.modes[&Mode::Events].known, 2);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
    assert!(report.problems[0].contains("passes in every mode"));

    // the Turtle tests
    let report = run_suite(dir.path(), "rdf/mini/manifest.ttl", &[]);
    println!("{report}");
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    assert_eq!(report.modes[&Mode::Events].pass, 1);
    assert_eq!(report.modes[&Mode::Snapshot].pass, 1);
    assert_eq!(report.modes[&Mode::Syntax].pass, 1);
}

/// A result that differs from the oracle's is reported as a bug.
#[test]
fn harness_reports_bugs() {
    let dir = mini_suite();
    let files = Files {
        root: dir.path().to_owned(),
    };
    let test = TestCase {
        id: "test".to_owned(),
        kinds: vec![format!("{MF}QueryEvaluationTest")],
        action: Some(format!("{TESTS_URL}sparql/mini/select.rq")),
        data: vec![format!("{TESTS_URL}sparql/mini/data.ttl")],
        result: Some(format!("{TESTS_URL}sparql/mini/wrong.srx")),
        ..TestCase::default()
    };
    let eval = Eval::new(&files, &test).unwrap().unwrap();
    let actual = Results::Solutions {
        variables: vec!["s".to_owned(), "o".to_owned()],
        rows: vec![],
    };
    assert!(matches!(
        eval.triage(Mode::Events, Ok(actual), "differs".to_owned()),
        Outcome::Bug(_)
    ));
    // without the snapshot, the data is retracted (at 20) and only the decoys are visible
    let decoys = eval.decoys().unwrap();
    assert_eq!(decoys.snapshot_at(T).valid().count_edges(), 6);
    assert!(decoys.count_edges() > 6);
    let all = "SELECT ?s ?p ?o { ?s ?p ?o }";
    let Ok(Results::Solutions { rows, .. }) = Ok::<_, String>(decoys.sparql(all).unwrap().into())
    else {
        unreachable!()
    };
    assert!(
        rows.iter().all(|row| row[0]
            .as_ref()
            .is_some_and(|s| s.to_string().starts_with("<urn:decoy:"))),
        "{rows:?}"
    );
    assert_eq!(rows.len(), 6);
    assert!(matches!(
        eval.outcome(Mode::Decoys),
        Outcome::Upstream { .. }
    ));

    // Order::Rows: a wrong sequence is a bug only if the oracle gives the expected one
    let ordered = |expected: &str| TestCase {
        id: "test".to_owned(),
        kinds: vec![format!("{MF}QueryEvaluationTest")],
        action: Some(format!("{TESTS_URL}sparql/mini/sum.rq")),
        data: vec![format!("{TESTS_URL}sparql/mini/numbers.ttl")],
        result: Some(format!("{TESTS_URL}sparql/mini/{expected}")),
        ..TestCase::default()
    };
    let subjects = |subjects: &[&str]| Results::Solutions {
        variables: vec!["s".to_owned()],
        rows: subjects
            .iter()
            .map(|s| {
                vec![Some(Term::from(NamedNode::new_unchecked(format!(
                    "http://example.org/{s}"
                ))))]
            })
            .collect(),
    };
    let (order, wrong_order) = (ordered("order.srx"), ordered("wrong-order.srx"));
    let eval = Eval::new(&files, &order).unwrap().unwrap();
    assert_eq!(eval.rules.order, Order::Rows);
    assert!(matches!(eval.outcome(Mode::Events), Outcome::Pass));
    assert!(matches!(
        eval.triage(
            Mode::Events,
            Ok(subjects(&["s2", "s3", "s1"])),
            "differs".to_owned()
        ),
        Outcome::Bug(_)
    ));
    let eval = Eval::new(&files, &wrong_order).unwrap().unwrap();
    for actual in [["s1", "s3", "s2"], ["s3", "s1", "s2"]] {
        assert!(matches!(
            eval.triage(Mode::Events, Ok(subjects(&actual)), "differs".to_owned()),
            Outcome::Upstream { .. }
        ));
    }
    // other solutions are still compared with the oracle's
    assert!(matches!(
        eval.triage(
            Mode::Events,
            Ok(subjects(&["s1", "s3"])),
            "differs".to_owned()
        ),
        Outcome::Bug(_)
    ));

    // numbers in other lexical forms are equal values
    let number = |value: &str, datatype: &str| {
        Some(Term::from(Literal::new_typed_literal(
            value,
            NamedNode::new_unchecked(format!("http://www.w3.org/2001/XMLSchema#{datatype}")),
        )))
    };
    let row = |values: Vec<Option<Term>>| Results::Solutions {
        variables: (0..values.len()).map(|i| format!("v{i}")).collect(),
        rows: vec![values],
    };
    let written = row(vec![
        number("3.21E4", "double"),
        number("2.0", "decimal"),
        number("01", "integer"),
        number("1.0E0", "float"),
    ]);
    let computed = row(vec![
        number("32100", "double"),
        number("2", "decimal"),
        number("1", "integer"),
        number("1", "float"),
    ]);
    let any = Rules::default();
    assert!(compare(&written, &computed, &any).is_err());
    assert!(compare(
        &with_canonical_numbers(&written),
        &with_canonical_numbers(&computed),
        &any
    )
    .is_ok());
    let other_value = row(vec![
        number("32101", "double"),
        number("2", "decimal"),
        number("1", "integer"),
        number("1", "float"),
    ]);
    assert!(compare(
        &with_canonical_numbers(&written),
        &with_canonical_numbers(&other_value),
        &any
    )
    .is_err());
}

#[test]
fn solutions_are_compared_up_to_blank_nodes() {
    let b = |label: &str| Some(Term::from(BlankNode::new_unchecked(label)));
    let i = |local: &str| {
        Some(Term::from(NamedNode::new_unchecked(format!(
            "http://ex/{local}"
        ))))
    };
    let vars = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    let solutions = |names: &[&str], rows: Vec<Row>| Results::Solutions {
        variables: vars(names),
        rows,
    };
    let any = Rules::default();
    let expected = solutions(
        &["x", "y"],
        vec![
            vec![b("a"), i("1")],
            vec![b("a"), b("b")],
            vec![i("2"), None],
        ],
    );
    // other labels, other row order, other column order
    let renamed = solutions(
        &["y", "x"],
        vec![
            vec![None, i("2")],
            vec![b("q"), b("p")],
            vec![i("1"), b("p")],
        ],
    );
    assert!(compare(&expected, &renamed, &any).is_ok());
    // not a bijection: two blank nodes where one is expected
    let split = solutions(
        &["x", "y"],
        vec![
            vec![b("p"), i("1")],
            vec![b("q"), b("r")],
            vec![i("2"), None],
        ],
    );
    assert!(compare(&expected, &split, &any).is_err());
    // a blank node for a blank node only
    let merged = solutions(
        &["x", "y"],
        vec![
            vec![b("p"), i("1")],
            vec![b("p"), b("p")],
            vec![i("2"), None],
        ],
    );
    assert!(compare(&expected, &merged, &any).is_err());
    // multisets: a duplicate row counts
    let doubled = solutions(&["x"], vec![vec![i("1")], vec![i("1")]]);
    assert!(compare(&doubled, &solutions(&["x"], vec![vec![i("1")]]), &any).is_err());
    assert!(compare(&expected, &solutions(&["x"], vec![]), &any).is_err());

    // ordered by ?x: the order of the ?x values counts, not that of ties
    let ordered = solutions(
        &["x", "y"],
        vec![
            vec![i("1"), i("a")],
            vec![i("1"), i("b")],
            vec![i("2"), i("c")],
        ],
    );
    let ties_swapped = solutions(
        &["x", "y"],
        vec![
            vec![i("1"), i("b")],
            vec![i("1"), i("a")],
            vec![i("2"), i("c")],
        ],
    );
    let reversed = solutions(
        &["x", "y"],
        vec![
            vec![i("2"), i("c")],
            vec![i("1"), i("a")],
            vec![i("1"), i("b")],
        ],
    );
    let by_x = Rules {
        order: Order::Keys(vars(&["x"])),
        lax: false,
    };
    assert!(compare(&ordered, &ties_swapped, &by_x).is_ok());
    assert!(compare(&ordered, &reversed, &by_x).is_err());
    assert!(compare(&ordered, &reversed, &any).is_ok());
    // ordered by something that is not in the rows: the rows themselves count
    let by_rows = Rules {
        order: Order::Rows,
        lax: false,
    };
    assert!(compare(&ordered, &ordered, &by_rows).is_ok());
    assert!(compare(&ordered, &ties_swapped, &by_rows).is_err());
    assert!(compare(&ordered, &reversed, &by_rows).is_err());
    let blank_rows = |labels: [&str; 2]| solutions(&["x"], labels.map(|l| vec![b(l)]).into());
    assert!(compare(&blank_rows(["a", "b"]), &blank_rows(["q", "p"]), &by_rows).is_ok());

    // lax cardinality: each expected solution at least once, at most as often as expected
    let lax = Rules {
        order: Order::Unordered,
        lax: true,
    };
    let xs =
        |rows: &[Option<Term>]| solutions(&["x"], rows.iter().map(|v| vec![v.clone()]).collect());
    let expected = xs(&[i("1"), i("1"), i("2"), b("a"), b("a")]);
    for actual in [
        xs(&[i("2"), i("1"), b("p")]),
        xs(&[i("1"), i("2"), i("1"), b("p"), b("p")]),
    ] {
        assert!(compare(&expected, &actual, &lax).is_ok(), "{actual:?}");
    }
    for actual in [
        // a solution more often than expected
        xs(&[i("1"), i("2"), i("2"), b("p")]),
        xs(&[i("1"), i("2"), b("p"), b("p"), b("p")]),
        // a solution missing
        xs(&[i("1"), b("p")]),
        // a solution that is not expected
        xs(&[i("1"), i("2"), i("3"), b("p")]),
        // two blank nodes for one
        xs(&[i("1"), i("2"), b("p"), b("q")]),
    ] {
        assert!(compare(&expected, &actual, &lax).is_err(), "{actual:?}");
    }
    assert!(compare(&expected, &xs(&[i("1"), i("2"), b("p")]), &any).is_err());
    // in order, with lax cardinality: a subsequence of the expected sequence
    let lax_by_x = Rules {
        order: Order::Keys(vars(&["x"])),
        lax: true,
    };
    let expected = xs(&[i("1"), i("1"), i("2"), i("3"), i("3")]);
    assert!(compare(&expected, &xs(&[i("1"), i("2"), i("3")]), &lax_by_x).is_ok());
    assert!(compare(&expected, &xs(&[i("1"), i("3"), i("2")]), &lax_by_x).is_err());
    assert!(compare(&expected, &xs(&[i("1"), i("3"), i("2")]), &lax).is_ok());
}

#[test]
fn graphs_are_compared_up_to_blank_nodes() {
    let parse = |doc: &str| parse_triples(doc.as_bytes(), RdfFormat::Turtle, TESTS_URL).unwrap();
    let a = parse("<http://ex/s> <http://ex/p> _:a . _:a <http://ex/q> _:b .");
    let b = parse("<http://ex/s> <http://ex/p> _:x . _:x <http://ex/q> _:y .");
    let c = parse("<http://ex/s> <http://ex/p> _:x . _:y <http://ex/q> _:z .");
    let any = Rules::default();
    assert!(compare(&Results::Graph(a.clone()), &Results::Graph(b), &any).is_ok());
    assert!(compare(&Results::Graph(a), &Results::Graph(c), &any).is_err());
}

#[test]
fn queries_are_ordered_by_projected_keys_or_by_rows() {
    let order = |query: &str| order_of(&SparqlParser::new().parse_query(query).unwrap());
    let keys = |keys: &[&str]| Order::Keys(keys.iter().map(|k| k.to_string()).collect());
    assert_eq!(
        order("SELECT ?x ?y { ?x ?p ?y } ORDER BY ?x DESC(?y)"),
        keys(&["x", "y"])
    );
    assert_eq!(order("SELECT * { ?x ?p ?y } ORDER BY ?y"), keys(&["y"]));
    assert_eq!(
        order("SELECT (?y AS ?z) { ?x ?p ?y } ORDER BY ?z"),
        keys(&["z"])
    );
    // a key that is an expression or a variable that is not projected
    for query in [
        "SELECT ?x ?y { ?x ?p ?y } ORDER BY str(?x) ?y",
        "SELECT ?x ?y { ?x ?p ?y } ORDER BY ?x str(?y)",
        "SELECT DISTINCT ?x { ?x ?p ?y } ORDER BY ?x ?y LIMIT 2",
        "SELECT ?x { ?x ?p ?y } ORDER BY ?y",
        "SELECT ?x { ?x ?p ?a ; ?q ?b } ORDER BY (?a + ?b)",
        "SELECT ?x { ?x ?p ?y } ORDER BY <http://www.w3.org/2001/XMLSchema#integer>(?y)",
        "SELECT ?x (COUNT(*) AS ?n) { ?x ?p ?y } GROUP BY ?x ORDER BY COUNT(*)",
    ] {
        assert_eq!(order(query), Order::Rows, "{query}");
    }
    assert_eq!(order("SELECT ?x { ?x ?p ?y }"), Order::Unordered);
    assert_eq!(
        order("SELECT ?x { { SELECT ?x { ?x ?p ?y } ORDER BY ?x } }"),
        Order::Unordered
    );
    assert_eq!(order("ASK { ?x ?p ?y }"), Order::Unordered);
}

/// An unreadable syntax test file is an error; a non-UTF-8 one is a rejected query.
#[test]
fn unreadable_syntax_tests_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("sparql")).unwrap();
    fs::write(
        dir.path().join("sparql/latin1.rq"),
        b"SELECT * { ?s ?p \"caf\xe9\" }",
    )
    .unwrap();
    let files = Files {
        root: dir.path().to_owned(),
    };
    let syntax = |kind: &str, file: &str| TestCase {
        id: "test".to_owned(),
        kinds: vec![format!("{MF}{kind}")],
        action: Some(format!("{TESTS_URL}sparql/{file}")),
        ..TestCase::default()
    };
    for kind in ["NegativeSyntaxTest11", "PositiveSyntaxTest11"] {
        let run = run_test(&files, &syntax(kind, "missing.rq"));
        assert!(
            matches!(&run, TestRun::Error(e) if e.contains("cannot read")),
            "{kind}: {run:?}"
        );
    }
    let run = run_test(&files, &syntax("NegativeSyntaxTest11", "latin1.rq"));
    let TestRun::Ran(outcomes) = run else {
        panic!("{run:?}")
    };
    assert!(matches!(outcomes[&Mode::Syntax], Outcome::Pass));
    let run = run_test(&files, &syntax("PositiveSyntaxTest11", "latin1.rq"));
    assert!(
        matches!(&run, TestRun::Error(e) if e.contains("not UTF-8")),
        "{run:?}"
    );
}

#[test]
fn csv_is_parsed() {
    assert_eq!(
        parse_csv("a,b\r\n1,\"x,\"\"y\"\"\r\nz\"\r\n,\r\n").unwrap(),
        [vec!["a", "b"], vec!["1", "x,\"y\"\r\nz"], vec!["", ""]]
    );
    assert_eq!(parse_csv("a\n1").unwrap(), [["a"], ["1"]]);
    assert!(parse_csv("a\n\"1").is_err());
}

#[test]
fn queries_get_a_from_clause() {
    let parse = |query: &str| SparqlParser::new().parse_query(query).unwrap();
    let rewrite = |query: &str| with_asof_from(query, &parse(query));
    for query in [
        "SELECT * { ?s ?p ?o }",
        "BASE <http://ex/> PREFIX w: <http://ex/somewhere/> SELECT ?where WHERE { ?where ?p [] }",
        "SELECT (EXISTS { ?s ?p ?o } AS ?e) { ?s ?p ?o }",
        "SELECT ((MIN(?o) + MAX(?o)) / 2 AS ?c) { ?s ?p ?o } GROUP BY ?s",
        "CONSTRUCT { ?s ?p [] } WHERE { ?s ?p ?o }",
        "CONSTRUCT WHERE { ?s ?p ?o }",
        "ASK { ?s ?p ?o }",
        "DESCRIBE <http://ex/s>",
    ] {
        let rewritten = rewrite(query).unwrap_or_else(|| panic!("{query}"));
        assert_eq!(
            parse(&rewritten).dataset().unwrap().default,
            [asof_graph()],
            "{query}"
        );
    }
    // brackets survive (spargebra's writer would drop them)
    let rewritten = rewrite("SELECT ((1 + 2) / 3 AS ?x) {}").unwrap();
    assert!(rewritten.contains("(1 + 2) / 3"), "{rewritten}");
    assert!(rewrite("SELECT * FROM <http://ex/g> { ?s ?p ?o }").is_none());
    assert!(rewrite("ASK FROM NAMED <http://ex/g> { ?s ?p ?o }").is_none());
}

/// Without `RAPHTORY_RDF_TESTS`, the suites are read from the submodule declared in the
/// `.gitmodules` of the workspace.
#[test]
fn tests_dir_defaults_to_the_submodule() {
    if std::env::var_os(TESTS_ENV).is_some_and(|dir| !dir.is_empty()) {
        return;
    }
    let dir = tests_dir();
    assert!(dir.ends_with(SUBMODULE), "{}", dir.display());
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let gitmodules = fs::read_to_string(workspace.join(".gitmodules")).unwrap();
    assert!(
        gitmodules.contains(&format!("path = {SUBMODULE}")),
        "{gitmodules}"
    );
}

/// A non-checkout is skipped with a message saying how to get it; a checkout is canonicalized.
#[test]
fn checkout_root_tells_how_to_get_the_submodule() {
    let dir = tempfile::tempdir().unwrap();
    let message = checkout_root(dir.path()).unwrap_err();
    assert!(
        message.contains(
            "`git submodule update --init --checkout raphtory-rdf-tests/test-suites/rdf-tests`"
        ),
        "{message}"
    );
    assert!(message.contains(TESTS_ENV), "{message}");
    fs::create_dir(dir.path().join("sparql")).unwrap();
    assert_eq!(
        checkout_root(dir.path()),
        Ok(dir.path().canonicalize().unwrap())
    );
}
