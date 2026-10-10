//! The W3C SHACL test suite (core and SHACL-SPARQL), run on Raphtory-backed data graphs.
//!
//! The suite is read from a checkout of <https://github.com/w3c/data-shapes> (W3C Software and
//! Document License, see `LICENSE.md` in the checkout): the git submodule
//! `raphtory-rdf-tests/test-suites/data-shapes`, or the directory in `RAPHTORY_SHACL_TESTS` if it
//! is set and not empty. `make w3c-tests-init` checks the submodule out and `make rust-test-shacl-w3c` runs
//! these tests. Without a checkout, the test prints how to get it and passes.
//!
//! The data graph is stored with [`add_triple`](raphtory::rdf::RdfGraph::add_triple), which
//! keeps blank-node labels, so data, shapes and expected report share their blank nodes.
//! Reports are compared, as the suite specifies, by `sh:conforms` and the multiset of (focus
//! node, constraint component, severity, path, value, source shape).
use oxigraph::io::RdfParser;
use raphtory::{
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{Graph as OxGraph, NamedNode, NamedOrBlankNode, Term, Triple},
        shacl::{ShaclReport, ShaclShapes},
        RdfError, RdfFormat,
    },
};
use shacl_report::{actual, expected, list, objects, one};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// `raphtory::rdf` as `crate::rdf`, the path [`shacl_report`] uses inside raphtory.
mod rdf {
    pub use raphtory::rdf::*;
}

/// Reading reports back: the helpers of raphtory's own SHACL tests (`rdf::tests::shacl`), which
/// compare `ShaclReport::write` with them, so that this suite checks those exact helpers against
/// the expected reports.
#[path = "../../raphtory/src/rdf/tests/shacl_report.rs"]
mod shacl_report;

const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const SHT: &str = "http://www.w3.org/ns/shacl-test#";

fn nn(namespace: &str, local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{namespace}{local}"))
}

/// The environment variable that overrides the directory of the checkout of w3c/data-shapes.
const TESTS_ENV: &str = "RAPHTORY_SHACL_TESTS";

/// The git submodule of w3c/data-shapes, relative to the root of the workspace.
const SUBMODULE: &str = "raphtory-rdf-tests/test-suites/data-shapes";

/// The command that checks the submodule out at its pinned commit (`--checkout` does so whatever
/// `update` strategy the submodule has).
const INIT: &str =
    "git submodule update --init --checkout raphtory-rdf-tests/test-suites/data-shapes";

/// The directory of the checkout of w3c/data-shapes: `RAPHTORY_SHACL_TESTS` if it is set and not
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

/// The root of the checkout of w3c/data-shapes, or `None` (with a message) if [`tests_dir`] is
/// not such a checkout.
fn tests_root() -> Option<PathBuf> {
    checkout_root(&tests_dir())
        .inspect_err(|message| println!("{message}"))
        .ok()
}

/// `dir` as a canonical absolute path if it is a checkout of w3c/data-shapes, or else the skip
/// message. A relative `dir` is resolved against the test's working directory (the
/// `raphtory-rdf-tests` crate directory), which the message shows.
fn checkout_root(dir: &Path) -> Result<PathBuf, String> {
    let absolute = std::path::absolute(dir).unwrap_or_else(|_| dir.to_owned());
    if absolute
        .join("data-shapes-test-suite/tests/manifest.ttl")
        .is_file()
    {
        Ok(absolute.canonicalize().unwrap_or(absolute))
    } else {
        Err(format!(
            "skipping the W3C SHACL conformance tests: {} (resolved to {}) is not a checkout of \
             https://github.com/w3c/data-shapes. Run `{INIT}` (or `make w3c-tests-init`) in the \
             Raphtory repository, or set {TESTS_ENV} to a checkout (a relative path is resolved \
             against the raphtory-rdf-tests crate directory, where cargo runs the tests)",
            dir.display(),
            absolute.display()
        ))
    }
}

fn file_iri(path: &Path) -> String {
    format!("file://{}", path.display()).replace(' ', "%20")
}

fn iri_path(iri: &str) -> PathBuf {
    PathBuf::from(iri.strip_prefix("file://").unwrap().replace("%20", " "))
}

/// The triples of every file read, by IRI: each file is parsed once, so its blank nodes keep
/// their labels wherever it is used.
#[derive(Default)]
struct Files(BTreeMap<String, Vec<Triple>>);

impl Files {
    fn get(&mut self, iri: &str) -> Vec<Triple> {
        self.0
            .entry(iri.to_owned())
            .or_insert_with(|| {
                let data = std::fs::read(iri_path(iri)).unwrap();
                RdfParser::from_format(RdfFormat::Turtle)
                    .with_base_iri(iri)
                    .unwrap()
                    .for_slice(&data)
                    .map(|quad| quad.map(Triple::from))
                    .collect::<Result<_, _>>()
                    .unwrap_or_else(|error| panic!("{iri}: {error}"))
            })
            .clone()
    }
}

/// The manifests with entries reachable from `manifest` through `mf:include`.
fn test_manifests(files: &mut Files, manifest: &str, out: &mut Vec<String>) {
    let graph: OxGraph = files.get(manifest).iter().collect();
    let node = NamedNode::new_unchecked(manifest);
    for include in objects(&graph, node.clone(), &nn(MF, "include")) {
        if let Term::NamedNode(include) = include {
            test_manifests(files, include.as_str(), out);
        }
    }
    if !objects(&graph, node, &nn(MF, "entries")).is_empty() {
        out.push(manifest.to_owned());
    }
}

/// How a test went.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    /// The report is the expected one.
    Pass,
    /// A failure was expected (`sht:Failure`), and validating failed.
    PassFailure,
    /// The shapes use a feature that is rejected.
    Unsupported(String),
    /// Anything else.
    Fail(String),
}

/// Runs every test of the manifests under `manifest`, by test name (`dir/name`).
fn run_suite(manifest: &Path) -> BTreeMap<String, Outcome> {
    let mut files = Files::default();
    let mut manifests = Vec::new();
    test_manifests(&mut files, &file_iri(manifest), &mut manifests);
    let mut outcomes = BTreeMap::new();
    for manifest in manifests {
        let graph: OxGraph = files.get(&manifest).iter().collect();
        let node = NamedOrBlankNode::from(NamedNode::new_unchecked(&manifest));
        let entries = one(&graph, &node, &nn(MF, "entries"))
            .map(|head| list(&graph, head))
            .unwrap_or_default();
        let dir = manifest.rsplit('/').nth(1).unwrap_or_default().to_owned();
        for entry in entries {
            let name = entry.to_string();
            let name = name.trim_end_matches('>').rsplit('/').next().unwrap();
            let entry = NamedOrBlankNode::try_from(entry.clone()).unwrap();
            let action = one(&graph, &entry, &nn(MF, "action")).unwrap();
            let action = NamedOrBlankNode::try_from(action).unwrap();
            let Some(Term::NamedNode(data)) = one(&graph, &action, &nn(SHT, "dataGraph")) else {
                panic!("{name}: no data graph")
            };
            let Some(Term::NamedNode(shapes)) = one(&graph, &action, &nn(SHT, "shapesGraph"))
            else {
                panic!("{name}: no shapes graph")
            };
            let result = one(&graph, &entry, &nn(MF, "result")).unwrap();
            let data = files.get(data.as_str());
            let shapes = files.get(shapes.as_str());
            let validated = (|| -> Result<ShaclReport, GraphError> {
                let g = RdfGraph::new();
                for triple in &data {
                    g.add_triple(1, triple)?;
                }
                g.validate_shacl(&ShaclShapes::from_triples(
                    shapes.iter().map(Triple::as_ref),
                )?)
            })();
            let failure_expected = result == Term::from(nn(SHT, "Failure"));
            let outcome = match validated {
                Err(_) if failure_expected => Outcome::PassFailure,
                Err(GraphError::Rdf(RdfError::ShaclUnsupported(feature))) => {
                    Outcome::Unsupported(feature)
                }
                Err(error) => Outcome::Fail(format!("error: {error}")),
                Ok(_) if failure_expected => Outcome::Fail("a failure was expected".to_owned()),
                Ok(report) => {
                    // the expected report is in the test file, which is its own manifest
                    let expected = expected(&graph, &result);
                    let actual = actual(&report);
                    if expected == actual {
                        Outcome::Pass
                    } else {
                        Outcome::Fail(format!("expected {expected:#?}\ngot {actual:#?}"))
                    }
                }
            };
            outcomes.insert(format!("{dir}/{name}"), outcome);
        }
    }
    outcomes
}

/// The whole suite through its root manifest: every core test passes, and every SHACL-SPARQL
/// test is rejected explicitly or expects a failure.
#[test]
fn w3c_root_manifest() {
    let Some(root) = tests_root() else { return };
    let outcomes = run_suite(&root.join("data-shapes-test-suite/tests/manifest.ttl"));
    let failed: Vec<_> = outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, Outcome::Fail(_)))
        .collect();
    assert_eq!(failed, vec![]);
    let count = |pred: fn(&Outcome) -> bool| outcomes.values().filter(|o| pred(*o)).count();
    assert_eq!(
        (
            count(|o| *o == Outcome::Pass),
            count(|o| matches!(o, Outcome::Unsupported(_))),
            count(|o| *o == Outcome::PassFailure),
            outcomes.len(),
        ),
        (98, 15, 7, 120)
    );
}
