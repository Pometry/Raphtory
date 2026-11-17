//! Serialising the triples visible in a view.
use crate::{
    mapping::{iri_of, term_of},
    RdfError, RdfFormat, WriteStats,
};
use oxigraph::{
    io::RdfSerializer,
    model::{NamedNode, NamedOrBlankNode, TripleRef},
};
use raphtory::{db::api::view::DynamicGraph, errors::GraphError, prelude::*};
use raphtory_api::core::entities::LayerId;
use rustc_hash::FxHashMap;
use std::{
    collections::hash_map::Entry,
    io::{BufWriter, Write},
};

pub(crate) fn write_rdf<W: Write>(
    view: DynamicGraph,
    writer: W,
    format: RdfFormat,
) -> Result<WriteStats, RdfError> {
    let mut out =
        RdfSerializer::from_format(format.to_oxigraph()).for_writer(BufWriter::new(writer));
    let mut stats = WriteStats::default();
    for_each_triple(&view, |triple| {
        match triple {
            Some(triple) => {
                out.serialize_triple(triple)?;
                stats.written += 1;
            }
            None => stats.skipped += 1,
        }
        Ok::<_, RdfError>(())
    })?;
    out.finish()?.flush()?;
    Ok(stats)
}

// `None` for a pair whose subject is a literal
pub(crate) fn for_each_triple<E: From<GraphError>>(
    view: &DynamicGraph,
    mut f: impl FnMut(Option<TripleRef<'_>>) -> Result<(), E>,
) -> Result<(), E> {
    let mut predicates: FxHashMap<Option<LayerId>, NamedNode> = FxHashMap::default();
    for node in view.nodes() {
        let mut subject = None;
        for edge in node.out_edges().explode_layers() {
            let subject = subject
                .get_or_insert_with(|| NamedOrBlankNode::try_from(term_of(&node.name())).ok());
            let predicate = match predicates.entry(edge.edge.layer()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(iri_of(&edge.layer_name()?)),
            };
            match subject {
                Some(subject) => {
                    let object = term_of(&edge.dst().name());
                    f(Some(TripleRef::new(&*subject, &*predicate, &object)))?;
                }
                None => f(None)?,
            }
        }
    }
    Ok(())
}
