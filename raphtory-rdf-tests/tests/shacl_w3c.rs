//! The W3C SHACL test suite (test-suites/data-shapes) run on `RdfGraph`. SHACL-SPARQL tests
//! (`tests/sparql`) are skipped: Raphtory implements SHACL core only.
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{vocab::rdf, Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, Term},
};
use raphtory_rdf::{
    shacl::{ShaclOptions, ShaclShapes},
    RdfFormat as Format, RdfGraph, Triple,
};
use rudof_rdf::{rdf_core::vocabs::ShaclVocab, rdf_impl::OxigraphInMemory};
use shacl::validator::report::ValidationReport;
use std::{error::Error, fs};
use url::Url;

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
        Url::from_file_path(MANIFEST).unwrap().as_ref(),
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
fn parse(data: &[u8], format: RdfFormat, base_iri: &str) -> Vec<Triple> {
    RdfParser::from_format(format)
        .with_base_iri(base_iri)
        .unwrap()
        .for_slice(data)
        .map(|quad| Triple::from(quad.unwrap_or_else(|e| panic!("{base_iri}: {e}"))))
        .collect()
}

fn parse_file(iri: &str) -> Vec<Triple> {
    let path = Url::parse(iri).unwrap().to_file_path().unwrap();
    let data = fs::read(&path).unwrap_or_else(|e| panic!("{iri}: {e}; run make w3c-tests-init"));
    parse(&data, RdfFormat::Turtle, iri)
}

fn store(triples: &[Triple]) -> OxigraphInMemory {
    let mut store = OxigraphInMemory::new();
    for triple in triples {
        store
            .add_triple_ref(&triple.subject, &triple.predicate, &triple.object)
            .unwrap();
    }
    store
}

fn objects<'a>(
    graph: &Graph,
    subject: &NamedOrBlankNode,
    predicate: impl Into<NamedNodeRef<'a>>,
) -> Vec<Term> {
    graph
        .objects_for_subject_predicate(subject, predicate.into())
        .map(|object| object.into_owned())
        .collect()
}

fn one<'a>(
    graph: &Graph,
    subject: &NamedOrBlankNode,
    predicate: impl Into<NamedNodeRef<'a>>,
) -> Option<Term> {
    objects(graph, subject, predicate).pop()
}

fn list(graph: &Graph, mut head: Term) -> Vec<Term> {
    let mut members = Vec::new();
    while let Ok(node) = NamedOrBlankNode::try_from(head) {
        let (Some(first), Some(rest)) =
            (one(graph, &node, rdf::FIRST), one(graph, &node, rdf::REST))
        else {
            break;
        };
        members.push(first);
        head = rest;
    }
    members
}

fn read_manifest(iri: &str, outcomes: &mut Vec<(String, Outcome)>) {
    let triples = parse_file(iri);
    let graph: Graph = triples.iter().collect();
    let manifest = NamedOrBlankNode::from(NamedNode::new_unchecked(iri));
    for include in objects(&graph, &manifest, &nn(MF, "include")) {
        read_manifest(NamedNode::try_from(include).unwrap().as_str(), outcomes);
    }
    let entries =
        one(&graph, &manifest, &nn(MF, "entries")).map_or(vec![], |head| list(&graph, head));
    let mut store = store(&triples);
    for entry in entries {
        let entry = NamedOrBlankNode::try_from(entry).unwrap();
        let outcome =
            (!iri.contains("/tests/sparql/")).then(|| run_test(&graph, &mut store, &entry));
        outcomes.push((entry.to_string(), outcome));
    }
}

fn run_test(
    graph: &Graph,
    manifest: &mut OxigraphInMemory,
    entry: &NamedOrBlankNode,
) -> Result<(), Box<dyn Error>> {
    let action = NamedOrBlankNode::try_from(one(graph, entry, &nn(MF, "action")).unwrap())?;
    let file = |local| -> Result<Vec<Triple>, Box<dyn Error>> {
        let iri = NamedNode::try_from(one(graph, &action, &nn(SHT, local)).unwrap())?;
        Ok(parse_file(iri.as_str()))
    };
    let data = RdfGraph::new();
    for triple in file("dataGraph")? {
        data.add_triple(1, &triple)?;
    }
    let shapes = ShaclShapes::from_triples(file("shapesGraph")?.iter().map(Triple::as_ref))?;
    let written = data
        .validate_shacl(&shapes, &ShaclOptions::default())?
        .to_rdf_string(Format::NTriples)?;
    let written = parse(written.as_bytes(), RdfFormat::NTriples, "urn:report");
    let root = written
        .iter()
        .find(|triple| {
            triple.predicate == rdf::TYPE
                && matches!(&triple.object, Term::NamedNode(class) if class.as_str() == ShaclVocab::SH_VALIDATION_REPORT)
        })
        .map(|triple| Term::from(triple.subject.clone()))
        .ok_or("the written report has no sh:ValidationReport")?;
    let actual = ValidationReport::parse(&mut store(&written), root)?;
    let expected =
        ValidationReport::parse(manifest, one(graph, entry, &nn(MF, "result")).unwrap())?;
    if expected == actual {
        return Ok(());
    }
    Err(format!("expected {expected}\ngot {actual}").into())
}
