//! The W3C SHACL test suite (core and SHACL-SPARQL, plus the SHACL 1.2 `sh:singleLine` test),
//! run on Raphtory-backed data graphs.
//!
//! The suite is read from a checkout of <https://github.com/w3c/data-shapes> (W3C Software and
//! Document License, see `LICENSE.md` in the checkout): the git submodule
//! `raphtory-rdf-tests/test-suites/data-shapes`, or the directory in `RAPHTORY_SHACL_TESTS` if it
//! is set and not empty. `make w3c-tests-init` checks the submodule out and `make rust-test-shacl-w3c` runs
//! these tests. Without a checkout (or a test file), a test prints how to get it and passes.
//!
//! The data graph is stored with [`add_triple`](raphtory::rdf::RdfMutationOps::add_triple), which
//! keeps blank-node labels, so data, shapes and expected report share their blank nodes.
//! Reports are compared, as the suite specifies, by `sh:conforms` and the multiset of (focus
//! node, constraint component, severity, path, value, source shape).
use raphtory::{
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{Graph as RdfGraph, NamedNode, NamedOrBlankNode, Term, Triple},
        shacl::{ShaclReport, ShaclShapes},
        RdfError, RdfFormat, RdfMutationOps, RdfParser,
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

/// `root.join(file)` if that file is in the checkout `root`, or else `None`, with a message.
fn suite_file(root: &Path, file: &str) -> Option<PathBuf> {
    let path = root.join(file);
    if path.is_file() {
        Some(path)
    } else {
        println!(
            "skipping {file}: it is not in {} (the submodule {SUBMODULE} at its pinned commit has \
             it: `{INIT}`)",
            root.display()
        );
        None
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
    let graph: RdfGraph = files.get(manifest).iter().collect();
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
        let graph: RdfGraph = files.get(&manifest).iter().collect();
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
                let g = Graph::new();
                for triple in &data {
                    g.add_triple(1, triple)?;
                }
                ShaclShapes::from_triples(shapes.iter().map(Triple::as_ref))?.validate(&g)
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

fn failures(outcomes: &BTreeMap<String, Outcome>) -> Vec<(&String, &Outcome)> {
    outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, Outcome::Fail(_)))
        .collect()
}

#[test]
fn w3c_core() {
    let Some(root) = tests_root() else { return };
    let outcomes = run_suite(&root.join("data-shapes-test-suite/tests/core/manifest.ttl"));
    assert_eq!(failures(&outcomes), vec![]);
    assert_eq!(outcomes.len(), 98);
    let not_passed: Vec<_> = outcomes
        .iter()
        .filter(|(_, outcome)| **outcome != Outcome::Pass)
        .collect();
    assert_eq!(not_passed, vec![]);
}

/// Every SHACL-SPARQL test is rejected explicitly, or expects a failure.
#[test]
fn w3c_sparql() {
    let Some(root) = tests_root() else { return };
    let outcomes = run_suite(&root.join("data-shapes-test-suite/tests/sparql/manifest.ttl"));
    assert_eq!(failures(&outcomes), vec![]);
    let unsupported = outcomes
        .values()
        .filter(|outcome| matches!(outcome, Outcome::Unsupported(_)))
        .count();
    let failure = outcomes
        .values()
        .filter(|outcome| **outcome == Outcome::PassFailure)
        .count();
    assert_eq!((outcomes.len(), unsupported, failure), (22, 15, 7));
    for outcome in outcomes.values() {
        if let Outcome::Unsupported(feature) = outcome {
            assert!(
                feature.contains("SHACL-SPARQL is not supported"),
                "{feature}"
            );
        }
    }
}

/// The whole suite through its root manifest: the core and SHACL-SPARQL tests.
#[test]
fn w3c_root_manifest() {
    let Some(root) = tests_root() else { return };
    let outcomes = run_suite(&root.join("data-shapes-test-suite/tests/manifest.ttl"));
    assert_eq!(outcomes.len(), 120);
    assert_eq!(failures(&outcomes), vec![]);
}

/// The SHACL 1.2 test of `sh:singleLine`, in a checkout of w3c/data-shapes.
const SINGLE_LINE: &str = "shacl12-test-suite/tests/core/property/singleLine-001.ttl";

/// The SHACL 1.2 test of `sh:singleLine`, which the validator does not support, is rejected.
#[test]
fn w3c_shacl12_single_line() {
    let Some(root) = tests_root() else { return };
    let Some(path) = suite_file(&root, SINGLE_LINE) else {
        return;
    };
    let single_line = |feature: &str| {
        feature.starts_with("sh:singleLine ")
            && feature.contains("SHACL 1.2 features are not supported")
    };
    let mut files = Files::default();
    let shapes = files.get(&file_iri(&path));
    let error = ShaclShapes::from_triples(shapes.iter().map(Triple::as_ref)).unwrap_err();
    let GraphError::Rdf(RdfError::ShaclUnsupported(feature)) = &error else {
        panic!("{error:?}")
    };
    assert!(single_line(feature), "{feature}");
    let outcomes = run_suite(&path);
    assert!(!outcomes.is_empty());
    assert!(
        outcomes.values().all(|outcome| matches!(
            outcome,
            Outcome::Unsupported(feature) if single_line(feature)
        )),
        "{outcomes:?}"
    );
}

/// Without `RAPHTORY_SHACL_TESTS`, the tests read the submodule declared in the `.gitmodules`
/// of the workspace.
#[test]
fn tests_dir_defaults_to_the_submodule() {
    if std::env::var_os(TESTS_ENV).is_some_and(|dir| !dir.is_empty()) {
        return;
    }
    let dir = tests_dir();
    assert!(dir.ends_with(SUBMODULE), "{}", dir.display());
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let gitmodules = std::fs::read_to_string(workspace.join(".gitmodules")).unwrap();
    assert!(
        gitmodules.contains(&format!("path = {SUBMODULE}")),
        "{gitmodules}"
    );
}

/// A directory that is not a checkout (such as an uninitialized submodule) is skipped with a message.
#[test]
fn checkout_root_tells_how_to_get_the_submodule() {
    let empty = tempfile::tempdir().unwrap();
    let message = checkout_root(empty.path()).unwrap_err();
    assert!(
        message.contains(
            "`git submodule update --init --checkout raphtory-rdf-tests/test-suites/data-shapes`"
        ),
        "{message}"
    );
    assert!(message.contains(TESTS_ENV), "{message}");
}

/// A test file that is not in the checkout is skipped with a message.
#[test]
fn suite_file_skips_a_file_the_checkout_lacks() {
    let checkout = tempfile::tempdir().unwrap();
    let tests = checkout.path().join("data-shapes-test-suite/tests");
    std::fs::create_dir_all(&tests).unwrap();
    std::fs::write(tests.join("manifest.ttl"), "").unwrap();
    let root = checkout_root(checkout.path()).unwrap();
    assert_eq!(suite_file(&root, SINGLE_LINE), None);
    assert_eq!(
        suite_file(&root, "data-shapes-test-suite/tests/manifest.ttl"),
        Some(root.join("data-shapes-test-suite/tests/manifest.ttl"))
    );
    // The submodule at its pinned commit has the SHACL 1.2 test.
    if std::env::var_os(TESTS_ENV).is_none_or(|dir| dir.is_empty()) {
        if let Some(root) = tests_root() {
            assert!(suite_file(&root, SINGLE_LINE).is_some());
        }
    }
}

/// A relative `RAPHTORY_SHACL_TESTS` is resolved against the working directory of the test, and
/// the skip message shows where it was resolved to.
#[test]
fn checkout_root_shows_the_resolved_path() {
    // Joined with the separator of the platform, as `std::path::absolute` gives it: on Windows,
    // it turns `/` into `\`.
    let relative = Path::new("no-such-dir").join("data-shapes");
    let message = checkout_root(&relative).unwrap_err();
    let resolved = std::env::current_dir().unwrap().join(&relative);
    assert!(
        message.contains(&format!("(resolved to {})", resolved.display())),
        "{message}"
    );
}

/// A checkout is accepted by an absolute or a relative path, and given as a canonical path, so
/// the file IRIs of the tests are absolute.
#[test]
fn checkout_root_accepts_a_checkout() {
    let checkout = tempfile::tempdir().unwrap();
    let tests = checkout.path().join("data-shapes-test-suite/tests");
    std::fs::create_dir_all(&tests).unwrap();
    std::fs::write(tests.join("manifest.ttl"), "").unwrap();
    let canonical = checkout.path().canonicalize().unwrap();
    assert_eq!(checkout_root(checkout.path()), Ok(canonical.clone()));
    // From the working directory up to the root, then down to the checkout (on Unix, where the
    // root is shared).
    if cfg!(unix) {
        let cwd = std::env::current_dir().unwrap();
        let mut relative: PathBuf = cwd.ancestors().skip(1).map(|_| "..").collect();
        relative.push(canonical.strip_prefix("/").unwrap());
        assert!(relative.is_relative());
        assert_eq!(checkout_root(&relative), Ok(canonical));
    }
}
