//! Defines the `ViewApi` trait, which represents the API for querying a view of the graph.

mod edge;
mod exploded_edge_property_filter;
pub(crate) mod filter_ops;
pub(crate) mod graph;
pub mod internal;
mod layer;
pub(crate) mod node;
pub(crate) mod time;

pub(crate) use edge::BaseEdgeViewOps;
pub use edge::EdgeViewOps;
use ouroboros::self_referencing;
use std::marker::PhantomData;

use crate::db::api::view::internal::{filtered_node::FilteredNodeStorageOps, GraphView};
pub use filter_ops::{BaseFilterOps, IterFilterOps};
pub use graph::*;
pub use internal::{
    BoxableGraphView, DynamicGraph, InheritViewOps, IntoDynHop, IntoDynamic, MaterializedGraph,
};
pub use layer::*;
pub(crate) use node::BaseNodeViewOps;
pub use node::NodeViewOps;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    Direction,
};
pub use raphtory_api::{
    inherit::Base,
    iter::{BoxedIter, BoxedLDIter, BoxedLIter, IntoDynBoxed},
};
use raphtory_storage::graph::{graph::GraphStorage, nodes::node_entry::NodeStorageEntry};
pub use time::*;

#[self_referencing]
pub struct EdgesIter<'graph, G: GraphViewOps<'graph>> {
    view: G,
    storage: GraphStorage,
    #[borrows(storage)]
    #[covariant]
    node: NodeStorageEntry<'this>,
    #[borrows(node, view)]
    #[covariant]
    iter: BoxedLIter<'this, EdgeRef>,
    marker: PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>> Iterator for EdgesIter<'graph, G> {
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.with_iter(|iter| iter.size_hint())
    }
}

pub(crate) fn node_edges<'graph, G: GraphView + 'graph>(
    storage: GraphStorage,
    view: G,
    node: VID,
    dir: Direction,
) -> BoxedLIter<'graph, EdgeRef> {
    EdgesIterBuilder {
        storage,
        view,
        node_builder: |view| view.core_node(node),
        iter_builder: move |node, graph| {
            node.filtered_edges_iter(graph, graph.layer_ids(), dir)
                .into_dyn_boxed()
        },
        marker: Default::default(),
    }
    .build()
    .into_dyn_boxed()
}
