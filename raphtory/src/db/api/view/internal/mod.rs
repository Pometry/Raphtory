mod core_deletion_ops;
mod edge_filter_ops;
mod filter_ops;
mod into_dynamic;
mod list_ops;
mod materialize;
mod node_filter_ops;
mod one_hop_filter;
pub(crate) mod time_semantics;
mod wrapped_graph;

use crate::{
    db::{
        api::{
            properties::internal::{ConstantPropertiesOps, InheritPropertiesOps, PropertiesOps},
            storage::storage::Storage,
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::{Graph, GraphViewOps},
};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

pub use core_deletion_ops::*;
pub use edge_filter_ops::*;
pub use filter_ops::*;
pub use into_dynamic::{IntoDynHop, IntoDynamic};
pub use list_ops::*;
pub use materialize::*;
pub use node_filter_ops::*;
pub use one_hop_filter::*;
pub use raphtory_api::inherit::Base;
pub use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    layer_ops::{InheritLayerOps, InternalLayerOps},
};
pub use time_semantics::*;

pub trait InheritViewOps: Base + Send + Sync {}

/// Marker trait to indicate that an object is a valid graph view
pub trait BoxableGraphView:
    CoreGraphOps
    + ListOps
    + InternalEdgeFilterOps
    + InternalEdgeLayerFilterOps
    + InternalExplodedEdgeFilterOps
    + InternalNodeFilterOps
    + InternalLayerOps
    + GraphTimeSemanticsOps
    + InternalMaterialize
    + PropertiesOps
    + ConstantPropertiesOps
    + InternalStorageOps
    + NodeHistoryFilter
    + EdgeHistoryFilter
    + Send
    + Sync
{
}

impl<
        G: CoreGraphOps
            + ListOps
            + InternalEdgeFilterOps
            + InternalEdgeLayerFilterOps
            + InternalExplodedEdgeFilterOps
            + InternalNodeFilterOps
            + InternalLayerOps
            + GraphTimeSemanticsOps
            + InternalMaterialize
            + PropertiesOps
            + ConstantPropertiesOps
            + InternalStorageOps
            + NodeHistoryFilter
            + EdgeHistoryFilter
            + Send
            + Sync,
    > BoxableGraphView for G
{
}

pub trait GraphView: BoxableGraphView + Sized + Clone {}

impl<T: BoxableGraphView + Sized + Clone> GraphView for T {}

impl<G: InheritViewOps> InheritNodeFilterOps for G {}

impl<G: InheritViewOps> InheritListOps for G {}

impl<G: InheritViewOps + HasDeletionOps> HasDeletionOps for G {}

impl<G: InheritViewOps> InheritAllEdgeFilterOps for G {}

impl<G: InheritViewOps + CoreGraphOps> InheritTimeSemantics for G {}

impl<G: InheritViewOps> InheritMaterialize for G {}

impl<G: InheritViewOps> InheritPropertiesOps for G {}

pub trait InheritStorageOps: Base {}

pub trait InternalStorageOps {
    fn get_storage(&self) -> Option<&Storage>;
}

impl<G: InheritStorageOps> InternalStorageOps for G
where
    G::Base: InternalStorageOps,
{
    fn get_storage(&self) -> Option<&Storage> {
        self.base().get_storage()
    }
}

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
pub struct DynamicGraph(pub Arc<dyn BoxableGraphView>);

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

impl InheritCoreGraphOps for DynamicGraph {}

impl InheritLayerOps for DynamicGraph {}

impl InheritStorageOps for DynamicGraph {}

impl InheritNodeHistoryFilter for DynamicGraph {}

impl InheritEdgeHistoryFilter for DynamicGraph {}

impl<'graph1, 'graph2: 'graph1, G: GraphViewOps<'graph2>> InheritViewOps for &'graph1 G {}

impl<'graph1, 'graph2: 'graph1, G: GraphViewOps<'graph2>> InheritStorageOps for &'graph1 G {}

impl<'graph1, 'graph2: 'graph1, G: GraphViewOps<'graph2>> InheritNodeHistoryFilter for &'graph1 G {}

impl<'graph1, 'graph2: 'graph1, G: GraphViewOps<'graph2>> InheritEdgeHistoryFilter for &'graph1 G {}

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
