use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{ops::node::NodeOp, NodeState, NodeStateOps},
            storage::graph::{nodes::node_storage_ops::NodeStorageOps, storage_ops::GraphStorage},
            view::{
                internal::{CoreGraphOps, NodeList, OneHopFilter},
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::GraphViewOps,
};
use rayon::prelude::*;
use std::{marker::PhantomData, sync::Arc};

pub struct LazyNodeState<'graph, Op, G, GH = G> {
    nodes: Nodes<'graph, G, GH>,
    op: Op,
}

impl<'graph, Op: OneHopFilter<'graph>, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    OneHopFilter<'graph> for LazyNodeState<'graph, Op, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = Op::FilteredGraph;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> =
        LazyNodeState<'graph, Op::Filtered<GHH>, G, GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        self.op.current_filter()
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        self.nodes.base_graph()
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        LazyNodeState {
            nodes: self.nodes.clone(),
            op: self.op.one_hop_filtered(filtered_graph),
        }
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for LazyNodeState<'graph, Op, G, GH>
{
    type Item = Op::Output;
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_values().into_dyn_boxed()
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    LazyNodeState<'graph, Op, G, GH>
{
    pub(crate) fn new(op: Op, nodes: Nodes<'graph, G, GH>) -> Self {
        Self { nodes, op }
    }

    pub fn collect<C: FromParallelIterator<Op::Output>>(&self) -> C {
        self.par_values().collect()
    }

    pub fn collect_vec(&self) -> Vec<Op::Output> {
        self.collect()
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    NodeStateOps<'graph> for LazyNodeState<'graph, Op, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;
    type Value<'a>
        = Op::Output
    where
        'graph: 'a,
        Self: 'a;
    type OwnedValue = Op::Output;

    fn graph(&self) -> &Self::Graph {
        &self.nodes.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.nodes.base_graph
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    > + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    >
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        if self.graph().nodes_filtered() {
            self.iter().nth(index)
        } else {
            let vid = match self.graph().node_list() {
                NodeList::All { num_nodes } => {
                    if index < num_nodes {
                        VID(index)
                    } else {
                        return None;
                    }
                }
                NodeList::List { nodes } => nodes.key(index)?,
            };
            let cg = self.graph().core_graph();
            Some((
                NodeView::new_one_hop_filtered(self.base_graph(), self.graph(), vid),
                self.op.apply(cg, vid),
            ))
        }
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let node = (&self.graph()).node(node);
        node.map(|node| self.op.apply(self.graph().core_graph(), node.node))
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::api::{
            state::{
                lazy_node_state::LazyNodeState,
                ops::node::{Degree, NodeOp},
            },
            view::{internal::CoreGraphOps, IntoDynamic},
        },
        prelude::*,
    };
    use raphtory_api::core::{entities::VID, Direction};
    use std::sync::Arc;

    struct TestWrapper<Op: NodeOp>(Op);
    #[test]
    fn test_compile() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

        let nodes = g.nodes();

        let node_state = LazyNodeState {
            nodes,
            op: Degree {
                graph: g.clone(),
                dir: Direction::BOTH,
            },
        };
        let node_state_window = node_state.after(1);

        let deg: Vec<_> = node_state.values().collect();
        let deg_w: Vec<_> = node_state_window.values().collect();

        let node_state_filter = node_state.valid_layers("bla");

        assert_eq!(deg, [1, 1]);
        assert_eq!(deg_w, [0, 0]);

        let g_dyn = g.clone().into_dynamic();

        let deg = Degree {
            graph: g_dyn,
            dir: Direction::BOTH,
        };
        let arc_deg: Arc<dyn NodeOp<Output = usize>> = Arc::new(deg);

        let node_state_dyn = LazyNodeState {
            nodes: g.nodes(),
            op: arc_deg.clone(),
        };

        let dyn_deg: Vec<_> = node_state_dyn.values().collect();
        assert_eq!(dyn_deg, [1, 1]);
        assert_eq!(arc_deg.apply(g.core_graph(), VID(0)), 1);

        let test_struct = TestWrapper(arc_deg);
    }
}
