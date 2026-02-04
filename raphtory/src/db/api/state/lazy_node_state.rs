use super::node_state_ops::ToOwnedValue;
use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                ops::{
                    ArrowMap, ArrowNodeOp, Const, DynNodeFilter, EarliestTime, FilterOps,
                    HistoryOp, IntoArrowNodeOp, IntoDynNodeOp, LatestTime, Map, NodeFilterOp,
                    NodeOp,
                },
                GenericNodeState, Index, NodeState, NodeStateOps,
            },
            view::{
                history::{
                    History, HistoryDateTime, HistoryEventId, HistoryTimestamp, InternalHistoryOps,
                    Intervals,
                },
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
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

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
        'a,
        'graph: 'a,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > PartialEq<LazyNodeState<'graph, O, G, GH, F>> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq,
{
    fn eq(&self, other: &LazyNodeState<'graph, O, G, GH, F>) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| v.to_owned_value() == value)
                    .unwrap_or(false)
            })
    }
}

impl<
        'a,
        'graph: 'a,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > PartialEq<NodeState<'graph, O::Output, GH>> for LazyNodeState<'graph, O, G, GH, F>
where
    O::Output: PartialEq,
{
    fn eq(&self, other: &NodeState<'graph, O::Output, GH>) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| *v == value)
                    .unwrap_or(false)
            })
    }
}

impl<
        'a,
        'graph: 'a,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > PartialEq<LazyNodeState<'graph, O, G, GH, F>> for NodeState<'graph, O::Output, GH>
where
    O::Output: PartialEq,
{
    fn eq(&self, other: &LazyNodeState<'graph, O, G, GH, F>) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| v == *value)
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
        O: ArrowNodeOp + 'graph,
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

    // TODO(wyatt): if value is node(s), construct node_cols accordingly
    // Convert LazyNodeState to GenericNodeState
    pub fn arrow_compute(&self) -> GenericNodeState<'graph, G> {
        if self.nodes.is_filtered() {
            let storage = self.graph().core_graph().lock();
            let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = self
                .nodes
                .par_iter()
                .map(move |node| (node.node, self.op.arrow_apply(&storage, node.node)))
                .unzip();
            GenericNodeState::new_from_eval_with_index(
                self.nodes.base_graph.clone(),
                values,
                Some(Index::new(keys)),
                None,
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
                values,
                None,
                None,
            )
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IntervalsStruct {
    pub intervals: Vec<i64>,
}
impl<T: InternalHistoryOps> From<Intervals<T>> for IntervalsStruct {
    fn from(intervals: Intervals<T>) -> Self {
        IntervalsStruct {
            intervals: intervals.collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IntervalStruct {
    pub interval: Option<i64>,
}
impl From<Option<i64>> for IntervalStruct {
    fn from(interval: Option<i64>) -> Self {
        IntervalStruct { interval }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AvgIntervalStruct {
    pub avg_interval: Option<f64>,
}
impl From<Option<f64>> for AvgIntervalStruct {
    fn from(avg_interval: Option<f64>) -> Self {
        AvgIntervalStruct { avg_interval }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TimeStampsStruct {
    pub timestamps: Vec<i64>,
}
impl<T: InternalHistoryOps> From<HistoryTimestamp<T>> for TimeStampsStruct {
    fn from(history: HistoryTimestamp<T>) -> Self {
        TimeStampsStruct {
            timestamps: history.collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DateTimesStruct {
    pub datetimes: Option<Vec<DateTime<Utc>>>,
}
impl<T: InternalHistoryOps> From<HistoryDateTime<T>> for DateTimesStruct {
    fn from(history: HistoryDateTime<T>) -> Self {
        DateTimesStruct {
            datetimes: history.collect().ok(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EventIdsStruct {
    pub event_ids: Vec<usize>,
}
impl<T: InternalHistoryOps> From<HistoryEventId<T>> for EventIdsStruct {
    fn from(history: HistoryEventId<T>) -> Self {
        EventIdsStruct {
            event_ids: history.collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TimeStampStruct {
    pub timestamp: Option<i64>,
}
impl From<Option<i64>> for TimeStampStruct {
    fn from(timestamp: Option<i64>) -> Self {
        TimeStampStruct { timestamp }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DateTimeStruct {
    pub datetime: Option<DateTime<Utc>>,
}
impl From<Result<Option<DateTime<Utc>>, TimeError>> for DateTimeStruct {
    fn from(datetime: Result<Option<DateTime<Utc>>, TimeError>) -> Self {
        DateTimeStruct {
            datetime: datetime.ok().flatten(),
        }
    }
}
impl From<Option<DateTime<Utc>>> for DateTimeStruct {
    fn from(datetime: Option<DateTime<Utc>>) -> Self {
        DateTimeStruct { datetime }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EventIdStruct {
    pub event_id: Option<usize>,
}
impl From<Option<usize>> for EventIdStruct {
    fn from(event_id: Option<usize>) -> Self {
        EventIdStruct { event_id }
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
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<HistoryOp<'graph, GH>, Intervals<NodeView<'graph, GH>>>, IntervalsStruct>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.intervals());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes.clone())
    }

    pub fn t(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<
            Map<HistoryOp<'graph, GH>, HistoryTimestamp<NodeView<'graph, GH>>>,
            TimeStampsStruct,
        >,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.t());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes.clone())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<
            Map<HistoryOp<'graph, GH>, HistoryDateTime<NodeView<'graph, GH>>>,
            DateTimesStruct,
        >,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.dt());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes.clone())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<HistoryOp<'graph, GH>, HistoryEventId<NodeView<'_, GH>>>, EventIdsStruct>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|hist| hist.event_id());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes.clone())
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
    pub fn t(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<EarliestTime<GH>, Option<i64>>, TimeStampStruct>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.t()));
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<EarliestTime<GH>, Result<Option<DateTime<Utc>>, TimeError>>, DateTimeStruct>,
        G,
        GH,
        F,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<EarliestTime<GH>, Option<usize>>, EventIdStruct>,
        G,
        GH,
        F,
    > {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + Clone + 'graph,
    > LazyNodeState<'graph, LatestTime<GH>, G, GH, F>
{
    pub fn t(
        &self,
    ) -> LazyNodeState<'graph, ArrowMap<Map<LatestTime<GH>, Option<i64>>, TimeStampStruct>, G, GH, F>
    {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.t()));
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }

    pub fn dt(
        &self,
    ) -> LazyNodeState<
        'graph,
        ArrowMap<Map<LatestTime<GH>, Result<Option<DateTime<Utc>>, TimeError>>, DateTimeStruct>,
        G,
        GH,
        F,
    > {
        let op = self
            .op
            .clone()
            .map(|t_opt| t_opt.map(|t| t.dt()).transpose());
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }

    pub fn event_id(
        &self,
    ) -> LazyNodeState<'graph, ArrowMap<Map<LatestTime<GH>, Option<usize>>, EventIdStruct>, G, GH, F>
    {
        let op = self.op.clone().map(|t_opt| t_opt.map(|t| t.i()));
        LazyNodeState::new(op.into_arrow_node_op(), self.nodes())
    }
}

impl<
        'a,
        'graph: 'a,
        O: NodeOp + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        F: NodeFilterOp + 'graph,
    > NodeStateOps<'a, 'graph> for LazyNodeState<'graph, O, G, GH, F>
{
    type Graph = GH;
    type BaseGraph = G;
    type Select = F;
    type Value
        = O::Output
    where
        'graph: 'a,
        Self: 'a;
    type OwnedValue = O::Output;
    type OutputType = NodeState<'graph, Self::OwnedValue, Self::Graph>;

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

    fn iter(&'a self) -> impl Iterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value)> + 'a {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph, Self::Select> {
        self.nodes.clone()
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value)> {
        let storage = self.graph().core_graph().lock();
        self.nodes
            .par_iter()
            .map(move |node| (node, self.op.apply(&storage, node.node)))
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(NodeView<'a, &'a Self::Graph>, Self::Value)> {
        if self.graph().filtered() {
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

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let node = (&self.graph()).node(node);
        node.map(|node| self.op.apply(self.graph().core_graph(), node.node))
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn construct(
        &self,
        _base_graph: Self::BaseGraph,
        graph: Self::Graph,
        keys: IndexSet<VID, ahash::RandomState>,
        values: Vec<Self::OwnedValue>,
    ) -> Self::OutputType
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        NodeState::new(graph, values.into(), Some(Index::new(keys)))
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
