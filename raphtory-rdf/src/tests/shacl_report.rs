//! Reading a SHACL validation report back from RDF, to compare it with a [`ShaclReport`].
//!
//! The W3C SHACL suite (`raphtory-rdf-tests/tests/shacl_w3c.rs`) includes this file with
//! `#[path]`, so the helpers that `tests::shacl` compares [`ShaclReport::write`] with are the
//! ones checked against the official expected reports. It therefore uses only `oxigraph`,
//! `rudof_rdf`, `rustc_hash` and `crate` paths, which that crate imports from `raphtory_rdf`.
use crate::{
    shacl::{ShaclPath, ShaclReport, ShaclResult},
    Literal, NamedNode, Term,
};
use oxigraph::model::{vocab::rdf, Graph as OxGraph, NamedNodeRef, NamedOrBlankNode};
use rudof_rdf::rdf_core::vocabs::ShaclVocab;
use rustc_hash::FxHashMap;
use std::hash::Hash;

pub(super) fn objects<'a>(
    graph: &OxGraph,
    subject: impl Into<NamedOrBlankNode>,
    predicate: impl Into<NamedNodeRef<'a>>,
) -> Vec<Term> {
    let subject = subject.into();
    graph
        .objects_for_subject_predicate(subject.as_ref(), predicate.into())
        .map(|object| object.into_owned())
        .collect()
}

pub(super) fn one<'a>(
    graph: &OxGraph,
    subject: &NamedOrBlankNode,
    predicate: impl Into<NamedNodeRef<'a>>,
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
        let Some(first) = one(graph, &node, rdf::FIRST) else {
            break;
        };
        members.push(first);
        let Some(rest) = one(graph, &node, rdf::REST) else {
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
        (ShaclVocab::SH_INVERSE_PATH, ShaclPath::Inverse),
        (ShaclVocab::SH_ZERO_OR_MORE_PATH, ShaclPath::ZeroOrMore),
        (ShaclVocab::SH_ONE_OR_MORE_PATH, ShaclPath::OneOrMore),
        (ShaclVocab::SH_ZERO_OR_ONE_PATH, ShaclPath::ZeroOrOne),
    ];
    for (predicate, wrap) in wrapped {
        if let Some(inner) = one(graph, &node, NamedNodeRef::new_unchecked(predicate)) {
            return wrap(Box::new(path_of(graph, &inner)));
        }
    }
    let alternative = NamedNodeRef::new_unchecked(ShaclVocab::SH_ALTERNATIVE_PATH);
    if let Some(paths) = one(graph, &node, alternative) {
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
pub(super) type Key = (
    Term,
    NamedNode,
    NamedNode,
    Option<ShaclPath>,
    Option<Term>,
    Option<Term>,
);

/// How many times each item occurs.
pub(super) fn multiset<T: Hash + Eq>(items: impl IntoIterator<Item = T>) -> FxHashMap<T, usize> {
    let mut counts = FxHashMap::default();
    for item in items {
        *counts.entry(item).or_default() += 1;
    }
    counts
}

/// The key of a result of a [`ShaclReport`].
pub(super) fn key(result: &ShaclResult) -> Key {
    (
        result.focus_node.clone(),
        result.constraint_component.clone(),
        result.severity.clone(),
        result.path.clone(),
        result.value.clone(),
        result.source_shape.clone(),
    )
}

/// `sh:conforms` and the results of the report `report` in `graph`.
pub(super) fn expected(graph: &OxGraph, report: &Term) -> (bool, FxHashMap<Key, usize>) {
    let report = NamedOrBlankNode::try_from(report.clone()).unwrap();
    let conforms = one(
        graph,
        &report,
        NamedNodeRef::new_unchecked(ShaclVocab::SH_CONFORMS),
    ) == Some(Literal::from(true).into());
    let results = objects(
        graph,
        report,
        NamedNodeRef::new_unchecked(ShaclVocab::SH_RESULT),
    );
    let keys = results.into_iter().map(|result| {
        let result = NamedOrBlankNode::try_from(result).unwrap();
        let get = |predicate| one(graph, &result, NamedNodeRef::new_unchecked(predicate));
        let iri = |predicate| NamedNode::try_from(get(predicate).unwrap()).unwrap();
        (
            get(ShaclVocab::SH_FOCUS_NODE).unwrap(),
            iri(ShaclVocab::SH_SOURCE_CONSTRAINT_COMPONENT),
            iri(ShaclVocab::SH_RESULT_SEVERITY),
            get(ShaclVocab::SH_RESULT_PATH).map(|path| path_of(graph, &path)),
            get(ShaclVocab::SH_VALUE),
            get(ShaclVocab::SH_SOURCE_SHAPE),
        )
    });
    (conforms, multiset(keys))
}

/// `conforms` and the results of a report, as [`expected`] gives them.
pub(super) fn actual(report: &ShaclReport) -> (bool, FxHashMap<Key, usize>) {
    (report.conforms, multiset(report.results.iter().map(key)))
}
