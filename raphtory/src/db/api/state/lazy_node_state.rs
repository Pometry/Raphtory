use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops,
                ops::{node::NodeOp, EarliestTime, HistoryOp, LatestTime, NodeOpFilter},
                Index, NodeState, NodeStateOps,
            },
            view::{
                history::{History, HistoryDateTime, HistoryEventId, HistoryTimestamp, Intervals},
                internal::{FilterOps, NodeList, OneHopFilter},
                BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::*,
};
use chrono::{DateTime, Utc};
use indexmap::IndexSet;
use raphtory_api::core::storage::timeindex::{AsTime, EventTime, TimeError};
use rayon::prelude::*;
use std::{
    borrow::Borrow,
    fmt::{Debug, Formatter},
    marker::PhantomData,
};

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

impl<
        'graph,
        Op: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        RHS: NodeStateOps<'graph, OwnedValue = Op::Output>,
    > PartialEq<RHS> for LazyNodeState<'graph, Op, G, GH>
where
    Op::Output: PartialEq,
{
    fn eq(&self, other: &RHS) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| v.borrow() == &value)
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

impl<'graph, OpG: GraphViewOps<'graph>, BaseG: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    OneHopFilter<'graph> for LazyNodeState<'graph, HistoryOp<'graph, OpG>, BaseG, GH>
{
    type BaseGraph = BaseG;
    type FilteredGraph = OpG;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> =
        LazyNodeState<'graph, HistoryOp<'graph, GHH>, BaseG, GH>;

    fn current_filter(&self) -> &Self::FilteredGraph {
        &self.op.graph
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
            op: HistoryOp {
                graph: filtered_graph,
                _phantom: PhantomData,
            },
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

    /// Computes a NodeState where the output values are results. Instead of keeping error values,
    /// the function fails if an error is encountered, and only Ok values are kept in the NodeState
    pub fn compute_result_type<T: Send, E: Send>(&self) -> Result<NodeState<'graph, T, G, GH>, E>
    where
        Op: NodeOp<Output = Result<T, E>>,
    {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Result<Vec<T>, E>) = self
                .par_iter()
                .map(|(node, value)| (node.node, value))
                .collect();
            Ok(NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values?.into(),
                Some(Index::new(keys)),
            ))
        } else {
            let values: Result<Vec<T>, E> = self.collect::<Result<Vec<T>, E>>();
            Ok(NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values?.into(),
                None,
            ))
        }
    }

    /// Computes a NodeState where only the Ok values of the Results are kept. Errors are discarded.
    pub fn compute_valid_results<T: Send, E: Send>(&self) -> NodeState<'graph, T, G, GH>
    where
        Op: NodeOp<Output = Result<T, E>>,
    {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<T>) = self
                .par_iter()
                .filter_map(|(node, value)| value.ok().map(|value| (node.node, value)))
                .unzip();
            NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values.into(),
                Some(Index::new(keys)),
            )
        } else {
            let values: Vec<T> = self
                .par_iter_values()
                .filter_map(|value| value.ok())
                .collect();
            NodeState::new(
                self.nodes.base_graph.clone(),
                self.nodes.graph.clone(),
                values.into(),
                None,
            )
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH>
{
    pub fn earliest_time(&self) -> LazyNodeState<'graph, EarliestTime<GH>, G, GH> {
        self.nodes.earliest_time()
    }

    pub fn latest_time(&self) -> LazyNodeState<'graph, LatestTime<GH>, G, GH> {
        self.nodes.latest_time()
    }

    pub fn flatten(&self) -> History<'graph, LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH>> {
        History::new(self.clone())
    }

    pub fn intervals(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<HistoryOp<'graph, GH>, Intervals<NodeView<GH, GH>>>, G, GH>
    {
        let op = self.op.clone().map(|hist| hist.intervals());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn t(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryTimestamp<NodeView<GH, GH>>>,
        G,
        GH,
    > {
        let op = self.op.clone().map(|hist| hist.t());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryDateTime<NodeView<GH, GH>>>,
        G,
        GH,
    > {
        let op = self.op.clone().map(|hist| hist.dt());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryEventId<NodeView<GH, GH>>>,
        G,
        GH,
    > {
        let op = self.op.clone().map(|hist| hist.event_id());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn collect_time_entries(&self) -> Vec<EventTime> {
        self.flatten().collect()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    LazyNodeState<'graph, EarliestTime<GH>, G, GH>
{
    pub fn t(&self) -> LazyNodeState<'graph, ops::Map<EarliestTime<GH>, Option<i64>>, G, GH> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.t()));
        LazyNodeState::new(op, self.nodes())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<EarliestTime<GH>, Result<Option<DateTime<Utc>>, TimeError>>,
        G,
        GH,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op, self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<EarliestTime<GH>, Option<usize>>, G, GH> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op, self.nodes())
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    LazyNodeState<'graph, LatestTime<GH>, G, GH>
{
    pub fn t(&self) -> LazyNodeState<'graph, ops::Map<LatestTime<GH>, Option<i64>>, G, GH> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.t()));
        LazyNodeState::new(op, self.nodes())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<LatestTime<GH>, Result<Option<DateTime<Utc>>, TimeError>>,
        G,
        GH,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op, self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<LatestTime<GH>, Option<usize>>, G, GH> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op, self.nodes())
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

    fn iter_values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn par_iter_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter_refs()
            .map(move |vid| self.op.apply(&storage, vid))
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
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

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph> {
        self.nodes.clone()
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
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
    ) -> Option<(
        NodeView<'_, &Self::BaseGraph, &Self::Graph>,
        Self::Value<'_>,
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
}
