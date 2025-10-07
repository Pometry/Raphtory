use crate::{
    db::api::{
        state::{generic_node_state::InputNodeStateValue, NodeStateValue},
        view::internal::{
            time_semantics::filtered_node::FilteredNodeStorageOps, FilterOps, FilterState,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::Deref, sync::Arc};

pub trait NodeOp: Send + Sync {
    type Output: Clone + Send + Sync;
    type ArrowOutput: InputNodeStateValue + Send + Sync;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output;

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput;

    fn map<V: Clone + Send + Sync>(self, map: fn(Self::Output) -> V) -> Map<Self, V>
    where
        Self: Sized,
    {
        Map { op: self, map }
    }

    // TODO: add arrow map
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

#[derive(Serialize, Clone)]
pub struct NameStruct {
    name: String,
}

impl NodeOp for Name {
    type Output = String;
    type ArrowOutput = NameStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        NameStruct {
            name: self.apply(storage, node),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Id;

#[derive(Serialize, Clone)]
pub struct IdStruct {
    id: GID,
}

impl NodeOp for Id {
    type Output = GID;
    type ArrowOutput = IdStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        IdStruct {
            id: self.apply(storage, node),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Type;

#[derive(Serialize, Clone)]
pub struct TypeStruct {
    node_type: Option<ArcStr>,
}

impl NodeOp for Type {
    type Output = Option<ArcStr>;
    type ArrowOutput = TypeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        TypeStruct {
            node_type: self.apply(storage, node),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;

#[derive(Serialize, Clone)]
pub struct TypeIdStruct {
    type_id: usize,
}

impl NodeOp for TypeId {
    type Output = usize;
    type ArrowOutput = TypeIdStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        TypeIdStruct {
            type_id: self.apply(storage, node),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) graph: G,
    pub(crate) dir: Direction,
}

#[derive(Serialize, Clone)]
pub struct DegreeStruct {
    degree: usize,
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for Degree<G> {
    type Output = usize;
    type ArrowOutput = DegreeStruct;

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        if matches!(self.graph.filter_state(), FilterState::Neither) {
            node.degree(self.graph.layer_ids(), self.dir)
        } else {
            node.filtered_neighbours_iter(&self.graph, self.graph.layer_ids(), self.dir)
                .count()
        }
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        DegreeStruct {
            degree: self.apply(storage, node),
        }
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

impl<V: Clone + Send + Sync> NodeOp for Arc<dyn NodeOp<Output = V, ArrowOutput = ()>> {
    type Output = V;
    type ArrowOutput = ();

    fn apply(&self, storage: &GraphStorage, node: VID) -> V {
        self.deref().apply(storage, node)
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        // TODO: self.deref().arrow_apply(storage, node)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Map<Op: NodeOp, V> {
    op: Op,
    map: fn(Op::Output) -> V,
}

#[derive(Debug, Copy, Clone)]
pub struct ArrowMap<Op: NodeOp, V> {
    op: Op,
    map: fn(Op::ArrowOutput) -> V,
}

impl<Op: NodeOp, V: Clone + Send + Sync> NodeOp for Map<Op, V> {
    type Output = V;
    type ArrowOutput = ();

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        (self.map)(self.op.apply(storage, node))
    }

    fn arrow_apply(&self, storage: &GraphStorage, node: VID) -> Self::ArrowOutput {
        // TODO: (self.arrow_map)(self.op.arrow_apply(storage, node))
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
