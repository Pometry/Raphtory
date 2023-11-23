mod core_deletion_ops;
mod core_ops;
mod edge_filter_ops;
mod graph_ops;
mod inherit;
mod into_dynamic;
mod layer_ops;
mod materialize;
pub(crate) mod time_semantics;
mod wrapped_graph;

mod one_hop_filter;

use crate::{
    db::api::{
        properties::internal::{ConstPropertiesOps, InheritPropertiesOps, PropertiesOps},
        view::StaticGraphViewOps,
    },
    prelude::GraphViewOps,
};
pub use core_deletion_ops::*;
pub use core_ops::*;
pub use edge_filter_ops::*;
pub use graph_ops::*;
pub use inherit::Base;
pub use into_dynamic::IntoDynamic;
pub use layer_ops::{DelegateLayerOps, InheritLayerOps, InternalLayerOps};
pub use materialize::*;
pub use one_hop_filter::*;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
pub use time_semantics::*;

pub trait BoxableGraphBase:
    CoreGraphOps
    + InternalLayerOps
    + TimeSemantics
    + InternalMaterialize
    + PropertiesOps
    + ConstPropertiesOps
    + Send
    + Sync
{
}

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView<'graph>:
    BoxableGraphBase + GraphOps<'graph> + EdgeFilterOps<'graph>
{
}

impl<
        G: CoreGraphOps
            + InternalLayerOps
            + TimeSemantics
            + InternalMaterialize
            + PropertiesOps
            + ConstPropertiesOps
            + Send
            + Sync,
    > BoxableGraphBase for G
{
}

impl<'graph, G: BoxableGraphBase + GraphOps<'graph> + EdgeFilterOps<'graph>>
    BoxableGraphView<'graph> for G
{
}

pub trait InheritViewOps<'graph>: Base + Send + Sync + 'graph {}

impl<'graph, G: InheritViewOps<'graph>> InheritCoreDeletionOps for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritGraphOps<'graph> for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritEdgeFilterOps for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritLayerOps for G {}
impl<'graph, G: InheritViewOps<'graph> + CoreGraphOps> InheritTimeSemantics for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritCoreOps for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritMaterialize for G {}
impl<'graph, G: InheritViewOps<'graph>> InheritPropertiesOps for G {}

/// Trait for marking a struct as not dynamically dispatched.
/// Used to avoid conflicts when implementing `From` for dynamic wrappers.
pub trait Static {}

impl<G: BoxableGraphView<'static> + Static + 'static> From<G> for DynamicGraph {
    fn from(value: G) -> Self {
        DynamicGraph(Arc::new(value))
    }
}

impl From<Arc<dyn BoxableGraphView<'static>>> for DynamicGraph {
    fn from(value: Arc<dyn BoxableGraphView<'static>>) -> Self {
        DynamicGraph(value)
    }
}

/// Trait for marking a graph view as immutable to avoid conflicts when implementing conversions for mutable and immutable views
pub trait Immutable {}

#[derive(Clone)]
pub struct DynamicGraph(pub(crate) Arc<dyn BoxableGraphView<'static>>);

impl Debug for DynamicGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DynamicGraph(num_vertices={}, num_edges={})",
            self.count_vertices(),
            self.count_edges()
        )
    }
}

impl DynamicGraph {
    pub fn new<G: GraphViewOps<'static>>(graph: G) -> Self {
        Self(Arc::new(graph))
    }

    pub fn new_from_arc<G: GraphViewOps<'static>>(graph_arc: Arc<G>) -> Self {
        Self(graph_arc)
    }
}

impl Base for DynamicGraph {
    type Base = dyn BoxableGraphView<'static>;

    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.0
    }
}

impl Immutable for DynamicGraph {}

impl InheritViewOps<'static> for DynamicGraph {}

impl<'graph, G: StaticGraphViewOps> InheritViewOps<'graph> for &'graph G {}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            api::{
                mutation::AdditionOps,
                view::{internal::BoxableGraphView, *},
            },
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new();
        g.add_vertex(0, 1, NO_PROPS).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(boxed.vertices().id().collect_vec(), vec![1])
    }
}
