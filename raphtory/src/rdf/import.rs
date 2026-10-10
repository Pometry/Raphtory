//! Writing RDF triples to a graph: one edge event per triple.
//!
//! Asserting `s p o` at `t` is `add_edge(t, name(s), name(o), NO_PROPS, Some(layer(p)))` and
//! retracting it is `delete_edge(t, name(s), name(o), Some(layer(p)))`, where `name` is
//! [`name_of`] and `layer` is [`layer_name_of`].
use crate::{
    db::api::view::internal::CoreGraphOps,
    errors::GraphError,
    prelude::*,
    rdf::{
        mapping::{escape_layer, is_rdf11_term, layer_name_of, name_of},
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

/// Why a term cannot be stored: an RDF 1.2 term ([`RdfError::Rdf12Term`]) or an IRI under
/// [`RESERVED_NS`] that does not encode a name ([`RdfError::NonCanonicalTerm`]).
fn unstorable(t: TermRef<'_>) -> RdfError {
    if is_rdf11_term(t) {
        RdfError::NonCanonicalTerm(t.to_string())
    } else {
        RdfError::Rdf12Term(t.to_string())
    }
}

/// The node name of a term, or why it cannot be stored.
fn name(t: TermRef<'_>) -> Result<String, RdfError> {
    name_of(t).ok_or_else(|| unstorable(t))
}

/// The printable characters that Windows or Unix do not allow in file names.
const UNSAFE_LAYER_CHARS: [char; 9] = ['/', '\\', ':', '*', '?', '"', '<', '>', '|'];

/// The layer name of a predicate, or why it cannot be stored.
fn layer(t: TermRef<'_>) -> Result<String, RdfError> {
    let name = layer_name_of(t).ok_or_else(|| unstorable(t))?;
    // only `<raphtory:..>` predicates can decode to these; escaped IRIs never do
    if name.trim().is_empty()
        || name.contains(|c: char| c.is_control() || UNSAFE_LAYER_CHARS.contains(&c))
    {
        return Err(RdfError::UnsafeLayerName(t.to_string()));
    }
    Ok(name)
}

/// How a triple's terms are named: by the full mappings, or by the fast ones.
#[derive(Clone, Copy)]
enum Naming {
    Checked,
    /// For the terms of an N-Triples or N-Quads document parsed without a base IRI. Those
    /// parsers already validate IRIs as `NamedNode::new` does, so an IRI outside
    /// [`RESERVED_NS`] needs no round trip.
    Fast,
}

impl Naming {
    fn node<'a>(self, t: TermRef<'a>) -> Result<Cow<'a, str>, RdfError> {
        match (self, t) {
            (Self::Fast, TermRef::NamedNode(n)) if !n.as_str().starts_with(RESERVED_NS) => {
                Ok(Cow::Borrowed(n.as_str()))
            }
            _ => name(t).map(Cow::Owned),
        }
    }

    fn layer<'a>(self, t: TermRef<'a>) -> Result<Cow<'a, str>, RdfError> {
        match (self, t) {
            (Self::Fast, TermRef::NamedNode(n)) if !n.as_str().starts_with(RESERVED_NS) => {
                Ok(escape_layer(n.as_str()))
            }
            _ => layer(t).map(Cow::Owned),
        }
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

/// Writes one triple, naming its terms with `naming`.
fn write_triple<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'_>,
    retract: bool,
    naming: Naming,
) -> Result<(), GraphError> {
    let s = naming.node(triple.subject.into())?;
    let p = naming.layer(triple.predicate.into())?;
    let o = naming.node(triple.object)?;
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
    write_triple(g, time, triple, retract, Naming::Checked)
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
    let naming = if base_iri.is_none() && matches!(format, RdfFormat::NTriples | RdfFormat::NQuads)
    {
        Naming::Fast
    } else {
        Naming::Checked
    };
    let mut count = 0;
    // The parsers buffer their input themselves.
    for quad in parser.for_reader(data) {
        let quad = quad?;
        let triple = quad.as_ref().into();
        write_triple(g, time, triple, retract, naming)?;
        count += 1;
    }
    Ok(count)
}
