//! The W3C SHACL test suite (test-suites/data-shapes) run on `RdfGraph`. SHACL-SPARQL tests
//! (`tests/sparql`) are skipped: Raphtory implements SHACL core only.
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{Graph as OxGraph, NamedOrBlankNode},
};
use raphtory_rdf::{
    shacl::{self, ShaclOptions, ShaclReport, ShaclShapes},
    Literal, NamedNode, RdfError, RdfGraph, Term, Triple,
};
use shacl_report::{actual, expected, list, objects, one};
use std::{error::Error, fs};
use url::Url;

// shared with raphtory-rdf's own SHACL tests; its `crate::` paths resolve to the imports above
#[path = "../../raphtory-rdf/src/tests/shacl_report.rs"]
mod shacl_report;

const MANIFEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-suites/data-shapes/data-shapes-test-suite/tests/manifest.ttl"
);
const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const SHT: &str = "http://www.w3.org/ns/shacl-test#";

type Outcome = Option<Result<(), Box<dyn Error>>>;

#[test]
fn w3c_root_manifest() {
    let mut outcomes = Vec::new();
    read_manifest(
        &Url::from_file_path(MANIFEST).unwrap().to_string(),
        &mut outcomes,
    );
    let failures: Vec<_> = outcomes
        .iter()
        .filter_map(|(id, outcome)| match outcome {
            Some(Err(e)) => Some(format!("{id}: {e}")),
            _ => None,
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    let tests = outcomes.len();
    let skipped = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_none())
        .count();
    assert_eq!((tests, tests - skipped, skipped), (120, 98, 22));
}

fn nn(namespace: &str, local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{namespace}{local}"))
}

// blank-node labels are kept, so data, shapes and expected report share their blank nodes
fn parse(iri: &str) -> OxGraph {
    let path = Url::parse(iri).unwrap().to_file_path().unwrap();
    let data = fs::read(&path).unwrap_or_else(|e| panic!("{iri}: {e}; run make w3c-tests-init"));
    RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri(iri)
        .unwrap()
        .for_slice(&data)
        .map(|quad| Triple::from(quad.unwrap_or_else(|e| panic!("{iri}: {e}"))))
        .collect()
}

fn read_manifest(iri: &str, outcomes: &mut Vec<(String, Outcome)>) {
    let graph = parse(iri);
    let manifest = NamedOrBlankNode::from(NamedNode::new_unchecked(iri));
    for include in objects(&graph, manifest.clone(), &nn(MF, "include")) {
        read_manifest(NamedNode::try_from(include).unwrap().as_str(), outcomes);
    }
    let entries =
        one(&graph, &manifest, &nn(MF, "entries")).map_or(vec![], |head| list(&graph, head));
    for entry in entries {
        let entry = NamedOrBlankNode::try_from(entry).unwrap();
        let outcome = (!iri.contains("/tests/sparql/")).then(|| run_test(&graph, &entry));
        outcomes.push((entry.to_string(), outcome));
    }
}

fn run_test(graph: &OxGraph, entry: &NamedOrBlankNode) -> Result<(), Box<dyn Error>> {
    let action = NamedOrBlankNode::try_from(one(graph, entry, &nn(MF, "action")).unwrap())?;
    let file = |local| -> Result<OxGraph, Box<dyn Error>> {
        let iri = NamedNode::try_from(one(graph, &action, &nn(SHT, local)).unwrap())?;
        Ok(parse(iri.as_str()))
    };
    let report = validate(&file("dataGraph")?, &file("shapesGraph")?)?;
    let expected = expected(graph, &one(graph, entry, &nn(MF, "result")).unwrap());
    let actual = actual(&report);
    if expected == actual {
        return Ok(());
    }
    Err(format!("expected {expected:#?}\ngot {actual:#?}").into())
}

fn validate(data: &OxGraph, shapes: &OxGraph) -> Result<ShaclReport, RdfError> {
    let g = RdfGraph::new();
    for triple in data {
        g.add_triple(1, triple)?;
    }
    g.validate_shacl(
        &ShaclShapes::from_triples(shapes)?,
        &ShaclOptions::default(),
    )
}
