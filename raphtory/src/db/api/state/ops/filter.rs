use crate::db::{
    api::state::{
        ops::{Const, IntoDynNodeOp},
        NodeOp,
    },
    graph::views::filter::model::Filter,
};
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps};
use std::sync::Arc;
use raphtory_api::core::storage::arc_str::OptionAsStr;
use raphtory_storage::core_ops::CoreGraphOps;
use crate::db::api::view::internal::GraphView;
use crate::db::graph::views::filter::internal::CreateFilter;
use crate::db::graph::views::filter::model::node_filter::NodeFilter;
use crate::db::graph::views::filter::node_filtered_graph::NodeFilteredGraph;
use crate::errors::GraphError;
use crate::prelude::{GraphViewOps, PropertyFilter};

#[derive(Clone, Debug)]
pub struct Mask<Op> {
    op: Op,
    mask: Arc<[bool]>,
}

impl<Op: NodeOp<Output = usize>> NodeOp for Mask<Op> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.mask
            .get(self.op.apply(storage, node))
            .copied()
            .unwrap_or(false)
    }
}

impl<Op: 'static> IntoDynNodeOp for Mask<Op> where Self: NodeOp {}

pub trait MaskOp: Sized {
    fn mask(self, mask: Arc<[bool]>) -> Mask<Self>;
}

impl<Op: NodeOp<Output = usize>> MaskOp for Op {
    fn mask(self, mask: Arc<[bool]>) -> Mask<Self> {
        Mask { op: self, mask }
    }
}

pub const NO_FILTER: Const<bool> = Const(true);

#[derive(Debug, Clone)]
pub struct NodeIdFilterOp {
    filter: Filter,
}

impl NodeIdFilterOp {
    pub(crate) fn new(filter: Filter) -> Self {
        Self { filter }
    }
}

impl NodeOp for NodeIdFilterOp {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        self.filter.id_matches(node.id())
    }
}

#[derive(Debug, Clone)]
pub struct NodeNameFilterOp {
    filter: Filter,
}

impl NodeNameFilterOp {
    pub(crate) fn new(filter: Filter) -> Self {
        Self { filter }
    }
}

impl NodeOp for NodeNameFilterOp {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_ref = storage.core_node(node);
        self.filter.matches(node_ref.name().as_str())
    }
}

#[derive(Debug, Clone)]
pub struct NodePropertyFilterOp<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<NodeFilter>,
}

impl<G> NodePropertyFilterOp<G> {
    pub(crate) fn new(graph: G, prop_id: usize, filter: PropertyFilter<NodeFilter>) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl CreateFilter for PropertyFilter<NodeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, NodePropertyFilterOp<G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodePropertyFilterOp<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.node_meta(), false)?;
        Ok(NodePropertyFilterOp::new(graph, prop_id, self))
    }
}

impl<G: GraphView> NodeOp for NodePropertyFilterOp<G> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node = storage.core_node(node);
        self.filter
            .matches_node(&self.graph, self.prop_id, node.as_ref())
    }
}