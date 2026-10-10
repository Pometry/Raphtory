//! Reading a SHACL validation report back from RDF, to compare it with a [`ShaclReport`].
//!
//! The W3C SHACL suite (`raphtory-rdf-tests/tests/shacl_w3c.rs`) includes this file with
//! `#[path]`, so the helpers that `rdf::tests::shacl` compares [`ShaclReport::write`] with are the
//! ones checked against the official expected reports. It therefore uses only `crate::rdf` paths,
//! which that crate provides as a re-export of `raphtory::rdf`.
use crate::rdf::{
    model::{vocab::rdf, Graph as OxGraph, NamedNode, NamedOrBlankNode, Term},
    shacl::{ShaclPath, ShaclReport},
};

const SH: &str = "http://www.w3.org/ns/shacl#";

fn sh(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{SH}{local}"))
}

pub(super) fn objects(
    graph: &OxGraph,
    subject: impl Into<NamedOrBlankNode>,
    predicate: &NamedNode,
) -> Vec<Term> {
    let subject = subject.into();
    graph
        .objects_for_subject_predicate(subject.as_ref(), predicate.as_ref())
        .map(|object| object.into_owned())
        .collect()
}

pub(super) fn one(
    graph: &OxGraph,
    subject: &NamedOrBlankNode,
    predicate: &NamedNode,
) -> Option<Term> {
    objects(graph, subject.clone(), predicate).pop()
}

/// The members of an RDF list.
pub(super) fn list(graph: &OxGraph, mut head: Term) -> Vec<Term> {
    let mut members = Vec::new();
    let nil = Term::from(rdf::NIL.into_owned());
    while head != nil {
        let Ok(node) = NamedOrBlankNode::try_from(head) else {
            break;
        };
        let Some(first) = one(graph, &node, &rdf::FIRST.into_owned()) else {
            break;
        };
        members.push(first);
        let Some(rest) = one(graph, &node, &rdf::REST.into_owned()) else {
            break;
        };
        head = rest;
    }
    members
}

/// A constructor of a path that wraps another.
type Wrap = fn(Box<ShaclPath>) -> ShaclPath;

/// Reads a SHACL path from a graph.
fn path_of(graph: &OxGraph, term: &Term) -> ShaclPath {
    if let Term::NamedNode(predicate) = term {
        return ShaclPath::Predicate(predicate.clone());
    }
    let node = NamedOrBlankNode::try_from(term.clone()).unwrap();
    let wrapped: [(&str, Wrap); 4] = [
        ("inversePath", ShaclPath::Inverse),
        ("zeroOrMorePath", ShaclPath::ZeroOrMore),
        ("oneOrMorePath", ShaclPath::OneOrMore),
        ("zeroOrOnePath", ShaclPath::ZeroOrOne),
    ];
    for (local, wrap) in wrapped {
        if let Some(inner) = one(graph, &node, &sh(local)) {
            return wrap(Box::new(path_of(graph, &inner)));
        }
    }
    if let Some(paths) = one(graph, &node, &sh("alternativePath")) {
        return ShaclPath::Alternative(
            list(graph, paths)
                .iter()
                .map(|path| path_of(graph, path))
                .collect(),
        );
    }
    ShaclPath::Sequence(
        list(graph, term.clone())
            .iter()
            .map(|path| path_of(graph, path))
            .collect(),
    )
}

/// A validation result as the W3C SHACL suite compares them: focus node, constraint component,
/// severity, path, value, source shape.
type Key = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// `sh:conforms` and the sorted results of the report `report` in `graph`.
pub(super) fn expected(graph: &OxGraph, report: &Term) -> (bool, Vec<Key>) {
    let report = NamedOrBlankNode::try_from(report.clone()).unwrap();
    let conforms = one(graph, &report, &sh("conforms"))
        .is_some_and(|conforms| conforms.to_string().contains("true"));
    let mut keys: Vec<Key> = objects(graph, report, &sh("result"))
        .into_iter()
        .map(|result| {
            let result = NamedOrBlankNode::try_from(result).unwrap();
            let get = |local: &str| one(graph, &result, &sh(local));
            (
                get("focusNode").unwrap().to_string(),
                get("sourceConstraintComponent").unwrap().to_string(),
                get("resultSeverity").unwrap().to_string(),
                get("resultPath").map(|path| path_of(graph, &path).to_string()),
                get("value").map(|value| value.to_string()),
                get("sourceShape").map(|shape| shape.to_string()),
            )
        })
        .collect();
    keys.sort();
    (conforms, keys)
}

/// `conforms` and the sorted results of a report, as [`expected`] gives them.
pub(super) fn actual(report: &ShaclReport) -> (bool, Vec<Key>) {
    let mut keys: Vec<Key> = report
        .results
        .iter()
        .map(|result| {
            (
                result.focus_node.to_string(),
                result.constraint_component.to_string(),
                result.severity.to_string(),
                result.path.as_ref().map(ShaclPath::to_string),
                result.value.as_ref().map(Term::to_string),
                result.source_shape.as_ref().map(Term::to_string),
            )
        })
        .collect();
    keys.sort();
    (report.conforms, keys)
}
