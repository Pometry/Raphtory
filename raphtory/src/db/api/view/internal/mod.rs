#![allow(dead_code)]
mod core_deletion_ops;
pub mod core_ops;
mod edge_filter_ops;
mod filter_ops;
mod inherit;
mod into_dynamic;
mod layer_ops;
mod list_ops;
mod materialize;
mod node_filter_ops;
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

use crate::{db::graph::views::deletion_graph::PersistentGraph, prelude::Graph};
pub use core_deletion_ops::*;
pub use core_ops::*;
pub use edge_filter_ops::*;
pub use filter_ops::*;
pub use inherit::Base;
pub use into_dynamic::{IntoDynHop, IntoDynamic};
pub use layer_ops::{DelegateLayerOps, InheritLayerOps, InternalLayerOps};
pub use list_ops::*;
pub use materialize::*;
pub use node_filter_ops::*;
pub use one_hop_filter::*;
pub use time_semantics::*;

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView:
    CoreGraphOps
    + ListOps
    + EdgeFilterOps
    + NodeFilterOps
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
        G: CoreGraphOps
            + ListOps
            + EdgeFilterOps
            + NodeFilterOps
            + InternalLayerOps
            + TimeSemantics
            + InternalMaterialize
            + PropertiesOps
            + ConstPropertiesOps
            + Send
            + Sync,
    > BoxableGraphView for G
{
}

pub trait InheritViewOps: Base + Send + Sync {}

impl<G: InheritViewOps> InheritNodeFilterOps for G {}

impl<G: InheritViewOps> InheritListOps for G {}

impl<G: InheritViewOps + HasDeletionOps> HasDeletionOps for G {}
impl<G: InheritViewOps> InheritEdgeFilterOps for G {}
impl<G: InheritViewOps> InheritLayerOps for G {}
impl<G: InheritViewOps + CoreGraphOps> InheritTimeSemantics for G {}
impl<G: InheritViewOps> InheritCoreOps for G {}
impl<G: InheritViewOps> InheritMaterialize for G {}
impl<G: InheritViewOps> InheritPropertiesOps for G {}

/// Trait for marking a struct as not dynamically dispatched.
/// Used to avoid conflicts when implementing `From` for dynamic wrappers.
pub trait Static {}

impl<G: BoxableGraphView + Static + 'static> From<G> for DynamicGraph {
    fn from(value: G) -> Self {
        DynamicGraph(Arc::new(value))
    }
}

impl From<Arc<dyn BoxableGraphView>> for DynamicGraph {
    fn from(value: Arc<dyn BoxableGraphView>) -> Self {
        DynamicGraph(value)
    }
}

/// Trait for marking a graph view as immutable to avoid conflicts when implementing conversions for mutable and immutable views
pub trait Immutable {}

pub enum DynOrMutableGraph {
    Dyn(DynamicGraph),
    Mutable(MaterializedGraph),
}
pub trait IntoDynamicOrMutable: IntoDynamic {
    fn into_dynamic_or_mutable(self) -> DynOrMutableGraph;
}

impl<G: IntoDynamic + Immutable> IntoDynamicOrMutable for G {
    fn into_dynamic_or_mutable(self) -> DynOrMutableGraph {
        DynOrMutableGraph::Dyn(self.into_dynamic())
    }
}

impl IntoDynamicOrMutable for MaterializedGraph {
    fn into_dynamic_or_mutable(self) -> DynOrMutableGraph {
        DynOrMutableGraph::Mutable(self)
    }
}

impl IntoDynamicOrMutable for Graph {
    fn into_dynamic_or_mutable(self) -> DynOrMutableGraph {
        DynOrMutableGraph::Mutable(self.into())
    }
}

impl IntoDynamicOrMutable for PersistentGraph {
    fn into_dynamic_or_mutable(self) -> DynOrMutableGraph {
        DynOrMutableGraph::Mutable(self.into())
    }
}

#[derive(Clone)]
pub struct DynamicGraph(pub(crate) Arc<dyn BoxableGraphView>);

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
    pub fn new<G: BoxableGraphView + 'static>(graph: G) -> Self {
        Self(Arc::new(graph))
    }

    pub fn new_from_arc<G: BoxableGraphView + 'static>(graph_arc: Arc<G>) -> Self {
        Self(graph_arc)
    }
}

impl Base for DynamicGraph {
    type Base = dyn BoxableGraphView;

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
        prelude::{NodeStateOps, NO_PROPS},
    };
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new();
        g.add_node(0, 1u64, NO_PROPS, None).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(
            boxed
                .nodes()
                .id()
                .iter_values()
                .filter_map(|v| v.as_u64())
                .collect_vec(),
            vec![1]
        );
    }
}
