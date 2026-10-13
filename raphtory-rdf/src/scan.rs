//! Reading the triples of a graph view: one per `(edge, layer)` pair visible in it.
use raphtory::{
    db::{
        api::view::{BoxedIter, DynamicGraph, IntoDynamic},
        graph::edge::EdgeView,
    },
    prelude::*,
};
use raphtory_api::core::entities::VID;
use std::iter;

/// The `(edge, layer)` pairs visible in `view`, each an edge exploded into one layer, whose
/// source is `subject`, whose layer is named `layer` and whose destination is `object` (`None`
/// matches any).
pub(crate) fn matching(
    view: &DynamicGraph,
    subject: Option<VID>,
    layer: Option<&str>,
    object: Option<VID>,
) -> BoxedIter<EdgeView<DynamicGraph>> {
    let view = match layer {
        Some(layer) => view.valid_layers(layer).into_dynamic(),
        None => view.clone(),
    };
    let edges = match (subject, object) {
        (Some(s), Some(o)) => view.edge(s, o).map(|e| e.explode_layers()),
        (Some(s), None) => view.node(s).map(|node| node.out_edges().explode_layers()),
        (None, Some(o)) => view.node(o).map(|node| node.in_edges().explode_layers()),
        (None, None) => Some(view.edges().explode_layers()),
    };
    match edges {
        Some(edges) => edges.into_iter(),
        None => Box::new(iter::empty()),
    }
}
