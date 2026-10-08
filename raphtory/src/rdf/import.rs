//! Writing RDF triples to a graph: one edge event per triple.
//!
//! Asserting `s p o` at `t` is `add_edge(t, name(s), name(o), NO_PROPS, Some(name(p)))` and
//! retracting it is `delete_edge(t, name(s), name(o), Some(name(p)))`.
use crate::{
    db::api::view::internal::CoreGraphOps,
    errors::GraphError,
    prelude::*,
    rdf::{
        mapping::{is_rdf11_term, name_of},
        RdfError, RESERVED_NS,
    },
};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{TermRef, TripleRef},
};
use raphtory_api::core::{entities::GidType, utils::time::InputTime};
use std::{borrow::Cow, io::Read};

/// Fails early, before anything is written, if the graph uses u64 node ids.
fn check_string_ids<G: CoreGraphOps>(g: &G) -> Result<(), RdfError> {
    if g.id_type() == Some(GidType::U64) {
        Err(RdfError::NonStringIds)
    } else {
        Ok(())
    }
}

/// The name of a term, or why it cannot be stored: an RDF 1.2 term ([`RdfError::Rdf12Term`]) or
/// an IRI under [`RESERVED_NS`] that does not encode a name ([`RdfError::NonCanonicalTerm`]).
fn name(t: TermRef<'_>) -> Result<String, RdfError> {
    name_of(t).ok_or_else(|| {
        if is_rdf11_term(t) {
            RdfError::NonCanonicalTerm(t.to_string())
        } else {
            RdfError::Rdf12Term(t.to_string())
        }
    })
}

/// [`name`] for the terms of an N-Triples or N-Quads document parsed without a base IRI.
///
/// Those parsers already validate IRIs as `NamedNode::new` does, so an IRI outside
/// [`RESERVED_NS`] is its own name without the [`name_of`] round trip.
fn fast_name(t: TermRef<'_>) -> Result<Cow<'_, str>, RdfError> {
    match t {
        TermRef::NamedNode(n) if !n.as_str().starts_with(RESERVED_NS) => {
            Ok(Cow::Borrowed(n.as_str()))
        }
        t => name(t).map(Cow::Owned),
    }
}

/// Writes one triple event: an assertion (`add_edge`) or a retraction (`delete_edge`).
fn write_names<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    (s, p, o): (&str, &str, &str),
    retract: bool,
) -> Result<(), GraphError> {
    if retract {
        g.delete_edge(time, s, o, Some(p))?;
    } else {
        g.add_edge(time, s, o, NO_PROPS, Some(p))?;
    }
    Ok(())
}

/// Writes one triple, naming its terms with `name` ([`fast_name`] or the full mapping).
fn write_triple<'a, G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'a>,
    retract: bool,
    name: impl Fn(TermRef<'a>) -> Result<Cow<'a, str>, RdfError>,
) -> Result<(), GraphError> {
    let s = name(triple.subject.into())?;
    let p = name(triple.predicate.into())?;
    let o = name(triple.object)?;
    write_names(g, time, (&s, &p, &o), retract)
}

/// Asserts (or retracts) one triple. Blank-node labels are used as written.
pub(crate) fn write_single<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'_>,
    retract: bool,
) -> Result<(), GraphError> {
    check_string_ids(g)?;
    write_triple(g, time, triple, retract, |t| name(t).map(Cow::Owned))
}

/// Asserts (or retracts) every triple of an RDF document at `time`, in document order, and
/// returns the number of triples read.
///
/// When asserting, blank nodes are renamed to fresh random labels; when retracting, labels are
/// used as written. Not atomic: triples before an error stay in the graph.
pub(crate) fn read_document<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    data: impl Read,
    format: RdfFormat,
    base_iri: Option<&str>,
    retract: bool,
) -> Result<usize, GraphError> {
    check_string_ids(g)?;
    let mut parser = RdfParser::from_format(format).without_named_graphs();
    if !retract {
        parser = parser.rename_blank_nodes();
    }
    if let Some(base_iri) = base_iri {
        parser = parser.with_base_iri(base_iri)?;
    }
    let fast_iris = base_iri.is_none() && matches!(format, RdfFormat::NTriples | RdfFormat::NQuads);
    let mut count = 0;
    // The parsers buffer their input themselves.
    for quad in parser.for_reader(data) {
        let quad = quad?;
        let triple = quad.as_ref().into();
        if fast_iris {
            write_triple(g, time, triple, retract, fast_name)?;
        } else {
            write_triple(g, time, triple, retract, |t| name(t).map(Cow::Owned))?;
        }
        count += 1;
    }
    Ok(count)
}
