//! The W3C SHACL test suite (core and SHACL-SPARQL), run on Raphtory-backed data graphs.
//!
//! The suite is read from the git submodule `raphtory-rdf-tests/test-suites/data-shapes`, a
//! checkout of <https://github.com/w3c/data-shapes> (W3C Software and Document License, see
//! `LICENSE.md` in the checkout). `make w3c-tests-init` checks the submodule out and
//! `make rust-test-rdf-w3c` runs these tests.
//!
//! The data graph is stored with [`add_triple`](raphtory_rdf::RdfGraph::add_triple), which
//! keeps blank-node labels, so data, shapes and expected report share their blank nodes.
//! Reports are compared, as the suite specifies, by `sh:conforms` and the multiset of (focus
//! node, constraint component, severity, path, value, source shape).
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{Graph as OxGraph, NamedOrBlankNode},
};
use raphtory_rdf::{
    shacl::{self, ShaclFeature, ShaclOptions, ShaclReport, ShaclShapes},
    Literal, NamedNode, RdfError, RdfGraph, Term, Triple,
};
use shacl_report::{actual, expected, list, objects, one};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
use url::Url;

/// Reading reports back: the helpers of raphtory-rdf's own SHACL tests (`tests::shacl`), which
/// compare `ShaclReport::write` with them, so that this suite checks those exact helpers against
/// the expected reports. They name `crate::shacl` and `crate::Literal`, imported above.
#[path = "../../raphtory-rdf/src/tests/shacl_report.rs"]
mod shacl_report;

const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const SHT: &str = "http://www.w3.org/ns/shacl-test#";

fn nn(namespace: &str, local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{namespace}{local}"))
}

/// The root manifest of the suite (a git submodule).
const MANIFEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-suites/data-shapes/data-shapes-test-suite/tests/manifest.ttl"
);

fn file_iri(path: &Path) -> String {
    Url::from_file_path(path).unwrap().to_string()
}

fn iri_path(iri: &str) -> PathBuf {
    Url::parse(iri).unwrap().to_file_path().unwrap()
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
                let data = fs::read(iri_path(iri)).unwrap();
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
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The report is the expected one.
    Pass,
    /// The shapes use a feature that is rejected.
    Unsupported(ShaclFeature),
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
            let Term::NamedNode(id) = entry else {
                panic!("{manifest}: an entry is not an IRI: {entry}")
            };
            let name = id.as_str().rsplit('/').next().unwrap();
            let entry = NamedOrBlankNode::from(id.clone());
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
            let validated = (|| -> Result<ShaclReport, RdfError> {
                let g = RdfGraph::new();
                for triple in &data {
                    g.add_triple(1, triple)?;
                }
                g.validate_shacl(
                    &ShaclShapes::from_triples(shapes.iter().map(Triple::as_ref))?,
                    &ShaclOptions::default(),
                )
            })();
            let failure_expected = result == Term::from(nn(SHT, "Failure"));
            let outcome = match validated {
                Err(RdfError::ShaclUnsupported { feature, .. }) => Outcome::Unsupported(feature),
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
/// test is rejected explicitly.
#[test]
fn w3c_root_manifest() {
    assert!(
        Path::new(MANIFEST).is_file(),
        "the W3C SHACL suite is missing: run `make w3c-tests-init`"
    );
    let outcomes = run_suite(Path::new(MANIFEST));
    let failed: Vec<_> = outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, Outcome::Fail(_)))
        .collect();
    assert_eq!(failed, vec![]);
    let count = |pred: fn(&Outcome) -> bool| outcomes.values().filter(|o| pred(o)).count();
    assert_eq!(
        (
            count(|o| *o == Outcome::Pass),
            count(|o| matches!(o, Outcome::Unsupported(_))),
            outcomes.len(),
        ),
        (98, 22, 120)
    );
}
