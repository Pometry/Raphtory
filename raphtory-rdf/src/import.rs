//! Writing RDF triples to a graph: one edge event per triple.
//!
//! Asserting `s p o` at `t` is `add_edge(t, name(s), name(o), NO_PROPS, Some(layer(p)))` and
//! retracting it is `delete_edge(t, name(s), name(o), Some(layer(p)))`, where `name` is
//! [`name_of`] and `layer` is [`layer_name_of`].
use crate::{
    mapping::{is_rdf11_term, layer_name_of, name_of},
    RdfError, RdfFormat,
};
use oxigraph::{
    io::RdfParser,
    model::{TermRef, TripleRef},
};
use raphtory::{errors::GraphError, prelude::*};
use raphtory_api::core::utils::time::InputTime;
use std::{fs::File, io::Read, path::Path};

/// Why a term cannot be stored: an RDF 1.2 term ([`RdfError::Rdf12Term`]) or an IRI under
/// [`RESERVED_NS`](crate::RESERVED_NS) that does not encode a name
/// ([`RdfError::NonCanonicalTerm`]).
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

/// Writes one triple event: an assertion (`add_edge`) or a retraction (`delete_edge`).
/// Blank-node labels are used as written.
pub(crate) fn write_triple<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'_>,
    retract: bool,
) -> Result<(), RdfError> {
    let s = name(triple.subject.into())?;
    let p = layer(triple.predicate.into())?;
    let o = name(triple.object)?;
    if retract {
        g.delete_edge(time, &s, &o, Some(&p))?;
    } else {
        g.add_edge(time, &s, &o, NO_PROPS, Some(&p))?;
    }
    Ok(())
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
) -> Result<usize, RdfError> {
    let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
    if !retract {
        parser = parser.rename_blank_nodes();
    }
    if let Some(base_iri) = base_iri {
        parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
    }
    let mut count = 0;
    // The parsers buffer their input themselves.
    for quad in parser.for_reader(data) {
        let quad = quad.map_err(RdfError::from)?;
        let triple = quad.as_ref().into();
        write_triple(g, time, triple, retract)?;
        count += 1;
    }
    Ok(count)
}

/// Opens the RDF document at `path` for reading, with its format: `format`, else the one its
/// extension names ([`RdfFormat::from_path`]).
pub(crate) fn open_document(
    path: &Path,
    format: Option<RdfFormat>,
) -> Result<(File, RdfFormat), RdfError> {
    let format = format.map_or_else(|| RdfFormat::from_path(path), Ok)?;
    // `File::open` opens a directory on Unix, and reading it fails later with a vaguer error
    if path.is_dir() {
        return Err(GraphError::IOErrorMsg(format!(
            "cannot open '{}': it is a directory",
            path.display()
        ))
        .into());
    }
    let file = File::open(path).map_err(|error| {
        GraphError::IOErrorMsg(format!("cannot open '{}': {error}", path.display()))
    })?;
    Ok((file, format))
}
