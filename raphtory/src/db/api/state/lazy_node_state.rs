use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops,
                ops::{
                    Const, DynNodeFilter, EarliestTime, HistoryOp, IntoDynNodeOp, LatestTime,
                    NodeFilterOp, NodeOp,
                },
                Index, NodeState, NodeStateOps,
            },
            view::{
                history::{History, HistoryDateTime, HistoryEventId, HistoryTimestamp, Intervals},
                internal::NodeList,
                BoxedLIter, DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{
            node::NodeView,
            nodes::{IntoDynNodes, Nodes},
        },
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
};

#[derive(Clone)]
pub struct LazyNodeState<'graph, Op, G, GH = G, F = Const<bool>> {
    nodes: Nodes<'graph, G, GH, F>,
    pub(crate) op: Op,
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        RHS,
    > PartialEq<&[RHS]> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &&[RHS]) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        RHS,
        const N: usize,
    > PartialEq<[RHS; N]> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &[RHS; N]) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        RHS: NodeStateOps<'graph, OwnedValue = O::Output>,
    > PartialEq<RHS> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq,
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

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
        RHS,
    > PartialEq<Vec<RHS>> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq<RHS>,
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.len() == other.len() && self.iter_values().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + 'graph,
        O: NodeOp + 'graph,
    > Debug for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter_values()).finish()
    }
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > IntoIterator for LazyNodeState<'graph, O, G, GH, F>
{
    type Item = (NodeView<'graph, GH>, O::Output);
    type IntoIter = BoxedLIter<'graph, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<O, G: IntoDynamic, GH: IntoDynamic, F: IntoDynNodeOp + NodeFilterOp + 'static>
    LazyNodeState<'static, O, G, GH, F>
{
    pub fn into_dyn(self) -> LazyNodeState<'static, O, DynamicGraph, DynamicGraph, DynNodeFilter> {
        LazyNodeState {
            nodes: self.nodes.into_dyn(),
            op: self.op,
        }
    }
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > LazyNodeState<'graph, O, G, GH, F>
{
    pub(crate) fn new(op: O, nodes: Nodes<'graph, G, GH, F>) -> Self {
        Self { nodes, op }
    }

    pub fn collect<C: FromParallelIterator<O::Output>>(&self) -> C {
        self.par_iter_values().collect()
    }

    pub fn collect_vec(&self) -> Vec<O::Output> {
        self.collect()
    }

    pub fn compute(&self) -> NodeState<'graph, O::Output, GH> {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = self
                .par_iter()
                .map(|(node, value)| (node.node, value))
                .unzip();
            NodeState::new(
                self.nodes.graph.clone(),
                values.into(),
                Some(Index::new(keys)),
            )
        } else {
            let values = self.collect_vec();
            NodeState::new(self.nodes.graph.clone(), values.into(), None)
        }
    }

    /// Computes a NodeState where the output values are results. Instead of keeping error values,
    /// the function fails if an error is encountered, and only Ok values are kept in the NodeState
    pub fn compute_result_type<T: Send, E: Send>(&self) -> Result<NodeState<'graph, T, G>, E>
    where
        O: NodeOp<Output = Result<T, E>>,
    {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Result<Vec<T>, E>) = self
                .par_iter()
                .map(|(node, value)| (node.node, value))
                .collect();
            Ok(NodeState::new(
                self.nodes.base_graph.clone(),
                values?.into(),
                Some(Index::new(keys)),
            ))
        } else {
            let values: Result<Vec<T>, E> = self.collect::<Result<Vec<T>, E>>();
            Ok(NodeState::new(
                self.nodes.base_graph.clone(),
                values?.into(),
                None,
            ))
        }
    }

    /// Computes a NodeState where only the Ok values of the Results are kept. Errors are discarded.
    pub fn compute_valid_results<T: Send, E: Send>(&self) -> NodeState<'graph, T, G>
    where
        O: NodeOp<Output = Result<T, E>>,
    {
        if self.nodes.is_filtered() {
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<T>) = self
                .par_iter()
                .filter_map(|(node, value)| value.ok().map(|value| (node.node, value)))
                .unzip();
            NodeState::new(
                self.nodes.base_graph.clone(),
                values.into(),
                Some(Index::new(keys)),
            )
        } else {
            let values: Vec<T> = self
                .par_iter_values()
                .filter_map(|value| value.ok())
                .collect();
            NodeState::new(self.nodes.base_graph.clone(), values.into(), None)
        }
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH, F>
{
    pub fn earliest_time(&self) -> LazyNodeState<'graph, EarliestTime<GH>, G, GH, F> {
        self.nodes.earliest_time()
    }

    pub fn latest_time(&self) -> LazyNodeState<'graph, LatestTime<GH>, G, GH, F> {
        self.nodes.latest_time()
    }

    pub fn flatten(
        &self,
    ) -> History<'graph, LazyNodeState<'graph, HistoryOp<'graph, GH>, G, GH, F>> {
        History::new(self.clone())
    }

    pub fn intervals(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<HistoryOp<'graph, GH>, Intervals<NodeView<'_, GH>>>, G, GH, F>
    {
        let op = self.op.clone().map(|hist| hist.intervals());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn t(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryTimestamp<NodeView<'_, GH>>>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.t());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryDateTime<NodeView<'_, GH>>>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.dt());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<
        'graph,
        ops::Map<HistoryOp<'graph, GH>, HistoryEventId<NodeView<'_, GH>>>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.event_id());
        LazyNodeState::new(op, self.nodes.clone())
    }

    pub fn collect_time_entries(&self) -> Vec<EventTime> {
        self.flatten().collect()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > LazyNodeState<'graph, EarliestTime<GH>, G, GH, F>
{
    pub fn t(&self) -> LazyNodeState<'graph, ops::Map<EarliestTime<GH>, Option<i64>>, G, GH, F> {
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
        F,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op, self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<EarliestTime<GH>, Option<usize>>, G, GH, F> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op, self.nodes())
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > LazyNodeState<'graph, LatestTime<GH>, G, GH, F>
{
    pub fn t(&self) -> LazyNodeState<'graph, ops::Map<LatestTime<GH>, Option<i64>>, G, GH, F> {
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
        F,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op, self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<'graph, ops::Map<LatestTime<GH>, Option<usize>>, G, GH, F> {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op, self.nodes())
    }
}

impl<
        'graph,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + 'graph,
    > NodeStateOps<'graph> for LazyNodeState<'graph, O, G, GH, F>
{
    type Select = F;
    type BaseGraph = G;
    type Graph = GH;
    type Value<'a>
        = O::Output
    where
        'graph: 'a,
        Self: 'a;
    type OwnedValue = O::Output;

    fn graph(&self) -> &Self::Graph {
        &self.nodes.graph
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
    ) -> impl Iterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value<'a>)> + 'a
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph, Self::Select> {
        self.nodes.clone()
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value<'a>)>
    where
        'graph: 'a,
    {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn get_by_index(&self, index: usize) -> Option<(NodeView<'_, &Self::Graph>, Self::Value<'_>)> {
        if self.nodes().is_list_filtered() {
            self.iter().nth(index)
        } else {
            let vid = match self.nodes().node_list() {
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
                NodeView::new_internal(self.graph(), vid),
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
                ops::{node::Degree, NodeOp},
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
        let nodes = g.nodes();

        assert_eq!(nodes.degree().collect_vec(), [1, 1]);
        assert_eq!(nodes.after(1).degree().collect_vec(), [0, 0]);

        let g_dyn = g.clone().into_dynamic();

        let deg = Degree {
            view: g_dyn,
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
