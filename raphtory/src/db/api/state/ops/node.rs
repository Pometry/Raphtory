use crate::{
    db::api::{
        storage::graph::storage_ops::GraphStorage,
        view::{
            internal::{CoreGraphOps, OneHopFilter, Static},
            IntoDynamic,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use std::{ops::Deref, sync::Arc};

pub trait NodeOp: Send + Sync {
    type Output: Clone + Send + Sync;
    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output;

    fn map<V: Clone + Send + Sync>(self, map: fn(Self::Output) -> V) -> Map<Self, V>
    where
        Self: Sized,
    {
        Map { op: self, map }
    }
}

// Cannot use OneHopFilter because there is no way to specify the bound on Output
pub trait NodeOpFilter<'graph>: NodeOp + 'graph {
    type Graph: GraphViewOps<'graph>;
    type Filtered<G: GraphViewOps<'graph>>: NodeOp<Output = Self::Output>
        + NodeOpFilter<'graph, Graph = G>
        + 'graph;

    fn graph(&self) -> &Self::Graph;

    fn filtered<G: GraphViewOps<'graph>>(&self, graph: G) -> Self::Filtered<G>;
}

#[derive(Debug, Clone, Copy)]
pub struct Name;

impl NodeOp for Name {
    type Output = String;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Id;

impl NodeOp for Id {
    type Output = GID;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Type;
impl NodeOp for Type {
    type Output = Option<ArcStr>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;
impl NodeOp for TypeId {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }
}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) graph: G,
    pub(crate) dir: Direction,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for Degree<G> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        storage.node_degree(node, self.dir, &self.graph)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOpFilter<'graph> for Degree<G> {
    type Graph = G;
    type Filtered<GH: GraphViewOps<'graph> + 'graph> = Degree<GH>;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH> {
        Degree {
            graph: filtered_graph,
            dir: self.dir,
        }
    }
}

impl<V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V>> {
    type Output = V;
    fn apply(&self, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(storage, node)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Map<Op: NodeOp, V> {
    op: Op,
    map: fn(Op::Output) -> V,
}

impl<Op: NodeOp, V: Clone + Send + Sync> NodeOp for Map<Op, V> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        (self.map)(self.op.apply(storage, node))
    }
}

impl<'graph, Op: NodeOpFilter<'graph>, V: Clone + Send + Sync + 'graph> NodeOpFilter<'graph>
    for Map<Op, V>
{
    type Graph = Op::Graph;
    type Filtered<G: GraphViewOps<'graph>> = Map<Op::Filtered<G>, V>;

    fn graph(&self) -> &Self::Graph {
        self.op.graph()
    }

    fn filtered<G: GraphViewOps<'graph>>(&self, graph: G) -> Self::Filtered<G> {
        let op = self.op.filtered(graph);
        Map { op, map: self.map }
    }
}
