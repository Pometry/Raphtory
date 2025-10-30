use super::node_state_ops::ToOwnedValue;
use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops::{node::NodeOp, NodeOpFilter},
                GenericNodeState, Index, NodeState, NodeStateOps,
            },
            view::{
                internal::{FilterOps, NodeList, OneHopFilter},
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::*,
};
use indexmap::IndexSet;
use rayon::prelude::*;
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct LazyNodeState<'graph, Op, G, GH = G> {
    nodes: Nodes<'graph, G, GH>,
    pub(crate) op: Op,
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, RHS>
    PartialEq<&[RHS]> for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &&[RHS]) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<
        'graph,
        Op: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        RHS,
        const N: usize,
    > PartialEq<[RHS; N]> for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &[RHS; N]) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<'a, 'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    PartialEq<LazyNodeState<'graph, Op, G, GH>> for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: PartialEq,
{
    fn eq(&self, other: &LazyNodeState<'graph, Op, G, GH>) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| v.to_owned_value() == value)
                    .unwrap_or(false)
            })
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, RHS>
    PartialEq<Vec<RHS>> for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>, Op: NodeOp + 'graph> Debug
    for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter_values()).finish()
    }
}

impl<'graph, Op: NodeOpFilter<'graph>, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    OneHopFilter<'graph> for LazyNodeState<'graph, Op, G, GH>
{
    type BaseGraph = G;
    type FilteredGraph = Op::Graph;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> =
        LazyNodeState<'graph, Op::Filtered<GHH>, G, GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        self.op.graph()
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
            op: self.op.filtered(filtered_graph),
        }
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> IntoIterator
    for LazyNodeState<'graph, Op, G, GH>
{
    type Item = (NodeView<'graph, G, GH>, Op::Output);
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<'graph, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    LazyNodeState<'graph, Op, G, GH>
{
    pub(crate) fn new(op: Op, nodes: Nodes<'graph, G, GH>) -> Self {
        Self { nodes, op }
    }

    pub fn collect<C: FromParallelIterator<Op::Output>>(&self) -> C {
        self.par_iter_values().collect()
    }

    pub fn collect_vec(&self) -> Vec<Op::Output> {
        self.collect()
    }

    pub fn compute(&self) -> NodeState<'graph, Op::Output, G, GH> {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = self
                .par_iter()
                .map(|(node, value)| (node.node, value))
                .unzip();
            NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values.into(),
                Some(Index::new(keys)),
            )
        } else {
            let values = self.collect_vec();
            NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values.into(),
                None,
            )
        }
    }

    pub fn arrow_compute(&self) -> GenericNodeState<'graph, G, GH> {
        if self.nodes.is_filtered() {
            let storage = self.graph().core_graph().lock();
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = self
                .nodes
                .par_iter()
                .map(move |node| (node.node, self.op.arrow_apply(&storage, node.node)))
                .unzip();
            GenericNodeState::new_from_eval_with_index(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values,
                Some(Index::new(keys)),
            )
        } else {
            let storage = self.graph().core_graph().lock();
            let values = self
                .nodes
                .par_iter_refs()
                .map(move |vid| self.op.arrow_apply(&storage, vid))
                .collect();
            GenericNodeState::new_from_eval_with_index(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values,
                None,
            )
        }
    }
}

impl<'a, 'graph: 'a, Op: NodeOp + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    NodeStateOps<'a, 'graph> for LazyNodeState<'graph, Op, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;
    type Value
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

    fn iter_values(&'a self) -> impl Iterator<Item = Self::Value> + 'a {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn par_iter_values(&'a self) -> impl ParallelIterator<Item = Self::Value> + 'a {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    #[allow(refining_impl_trait)]
    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    #[allow(refining_impl_trait)]
    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn iter(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value,
        ),
    > + 'a {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph> {
        self.nodes.clone()
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value,
        ),
    > {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        if self.graph().filtered() {
            self.iter().nth(index)
        } else {
            let vid = match self.graph().node_list() {
                NodeList::All { len } => {
                    if index < len {
                        VID(index)
                    } else {
                        return None;
                    }
                }
                NodeList::List { elems } => elems.key(index)?,
            };
            let cg = self.graph().core_graph();
            Some((
                NodeView::new_one_hop_filtered(self.base_graph(), self.graph(), vid),
                self.op.apply(cg, vid),
            ))
        }
    }

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let node = (&self.graph()).node(node);
        node.map(|node| self.op.apply(self.graph().core_graph(), node.node))
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod test {
    /*
    use crate::{
        db::api::{
            state::{
                lazy_node_state::LazyNodeState,
                ops::node::{Degree, NodeOp},
            },
            view::IntoDynamic,
        },
        prelude::*,
    };
    use raphtory_api::core::{entities::VID, Direction};
    use raphtory_storage::core_ops::CoreGraphOps;
    use std::sync::Arc;

    struct TestWrapper<Op: NodeOp>(Op);
    #[test]
    fn test_compile() {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let deg = g.nodes().degree();

        assert_eq!(deg.collect_vec(), [1, 1]);
        assert_eq!(deg.after(1).collect_vec(), [0, 0]);

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

        let dyn_deg: Vec<_> = node_state_dyn.iter_values().collect();
        assert_eq!(dyn_deg, [1, 1]);
        assert_eq!(arc_deg.apply(g.core_graph(), VID(0)), 1);

        let _test_struct = TestWrapper(arc_deg);
    }
    */
}
