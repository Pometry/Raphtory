mod core_deletion_ops;
mod core_ops;
mod edge_filter_ops;
mod graph_ops;
mod inherit;
mod into_dynamic;
mod layer_ops;
mod materialize;
mod one_hop_filter;
pub(crate) mod time_semantics;
mod wrapped_graph;

use crate::{
    db::api::properties::internal::{ConstPropertiesOps, InheritPropertiesOps, PropertiesOps},
    prelude::GraphViewOps,
};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
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
pub use time_semantics::*;

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView<'graph>:
    CoreGraphOps
    + GraphOps<'graph>
    + EdgeFilterOps
    + InternalLayerOps
    + TimeSemantics
    + InternalMaterialize
    + PropertiesOps
    + ConstPropertiesOps
    + Send
    + Sync
{
}

impl<
        'graph,
        G: CoreGraphOps
            + GraphOps<'graph>
            + EdgeFilterOps
            + InternalLayerOps
            + TimeSemantics
            + InternalMaterialize
            + PropertiesOps
            + ConstPropertiesOps
            + Send
            + Sync,
    > BoxableGraphView<'graph> for G
{
}

pub trait InheritViewOps: Base + Send + Sync {}

impl<G: InheritViewOps> InheritCoreDeletionOps for G {}
impl<G: InheritViewOps> InheritGraphOps for G {}
impl<G: InheritViewOps> InheritEdgeFilterOps for G {}
impl<G: InheritViewOps> InheritLayerOps for G {}
impl<G: InheritViewOps + CoreGraphOps> InheritTimeSemantics for G {}
impl<G: InheritViewOps> InheritCoreOps for G {}
impl<G: InheritViewOps> InheritMaterialize for G {}
impl<G: InheritViewOps> InheritPropertiesOps for G {}

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
            "DynamicGraph(num_nodes={}, num_edges={})",
            self.count_nodes(),
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

impl InheritViewOps for DynamicGraph {}

impl<'graph1, 'graph2: 'graph1, G: GraphViewOps<'graph2>> InheritViewOps for &'graph1 G {}

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
        g.add_node(0, 1, NO_PROPS, None).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(boxed.nodes().id().collect_vec(), vec![1])
    }
}
