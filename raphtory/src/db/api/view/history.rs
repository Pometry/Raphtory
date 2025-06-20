#![allow(unused_imports)]
#![allow(dead_code)]

use crate::{
    core::{
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::iter::GenLockedIter,
    },
    db::{
        api::{
            properties::{
                internal::{PropertiesOps, TemporalPropertiesOps},
                TemporalPropertyView,
            },
            state::{
                ops,
                ops::{node::NodeOp, EarliestTime},
            },
            view::{
                internal::{
                    filtered_node::FilteredNodeStorageOps, EdgeTimeSemanticsOps,
                    GraphTimeSemanticsOps, InternalLayerOps, NodeTimeSemanticsOps,
                },
                BaseNodeViewOps, BoxableGraphView, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::{
            edge::{edge_valid_layer, EdgeView},
            node::NodeView,
        },
    },
    prelude::*,
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::{
    entities::LayerIds,
    storage::timeindex::{TimeError, TimeIndexOps},
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{iter, ops::Deref, slice::Iter, sync::Arc};
// TODO: Do we want to implement gt, lt, etc?

pub trait InternalHistoryOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    // Option<> because they are options in time_semantics.rs
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
}

// FIXME: Doesn't support deletions of edges yet
#[derive(Debug, Clone, Copy)]
pub struct History<T>(pub T);

impl<T: InternalHistoryOps> History<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    // see if there's a way to get secondary temporal information (two entries for the same time value)
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Box<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        T::latest_time(self)
    }
}

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for Arc<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter(self)
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        T::iter_rev(self)
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        T::earliest_time(self)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        T::latest_time(self)
    }
}

impl<T: InternalHistoryOps> History<T> {
    pub fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }

    pub fn iter_t(&self) -> BoxedLIter<i64> {
        self.0.iter().map(|t| t.t()).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    pub fn iter_rev_t(&self) -> BoxedLIter<i64> {
        self.0.iter_rev().map(|t| t.t()).into_dyn_boxed()
    }

    pub fn collect_items(&self) -> Vec<TimeIndexEntry> {
        self.0.iter().collect_vec()
    }

    pub fn collect_timestamps(&self) -> Vec<i64> {
        self.0.iter().map(|x| x.t()).collect_vec()
    }

    pub fn collect_date_times(&self) -> Result<Vec<DateTime<Utc>>, TimeError> {
        self.0
            .iter()
            .map(|x| x.dt())
            .collect::<Result<Vec<_>, TimeError>>()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    pub fn intervals(&self) -> Intervals {
        Intervals(self.iter().map(|x| x.t()).collect_vec())
    }

    pub fn merge<R: InternalHistoryOps>(self, right: History<R>) -> History<MergedHistory<T, R>> {
        History::new(MergedHistory::new(self.0, right.0))
    }

    pub fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.0.earliest_time()
    }

    pub fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.0.latest_time()
    }

    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

impl<T: InternalHistoryOps, L, I: Copy> PartialEq<L> for History<T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    TimeIndexEntry: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<T: InternalHistoryOps> PartialEq for History<T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T: InternalHistoryOps> Eq for History<T> {}

impl<T: InternalHistoryOps> std::hash::Hash for History<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for item in self.iter() {
            item.hash(state);
        }
    }
}

/// Separate from CompositeHistory in that it can only hold two items. They can be nested.
/// More efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
/// TODO: Write benchmark to evaluate performance benefit tradeoff (ie when there are no more performance benefits)
#[derive(Debug, Clone, Copy)]
pub struct MergedHistory<L, R> {
    left: L,
    right: R,
}

impl<L: InternalHistoryOps, R: InternalHistoryOps> MergedHistory<L, R> {
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<L: InternalHistoryOps, R: InternalHistoryOps> InternalHistoryOps for MergedHistory<L, R> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.left.iter().merge(self.right.iter()).into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.left
            .iter_rev()
            .merge_by(self.right.iter_rev(), |a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

/// Holds a vector of multiple items implementing InternalHistoryOps. If the composite will only hold 2 items, MergedHistory is more efficient.
/// TODO: Write benchmark to see performance hit of Boxes
pub struct CompositeHistory {
    history_objects: Vec<Arc<dyn InternalHistoryOps>>,
}

impl CompositeHistory {
    pub fn new(history_objects: Vec<Arc<dyn InternalHistoryOps>>) -> Self {
        Self { history_objects }
    }
}

// Note: All the items held by their respective History objects must already be of type Arc<T>
pub fn compose_multiple_histories(
    objects: impl IntoIterator<Item = History<Arc<dyn InternalHistoryOps>>>,
) -> History<CompositeHistory> {
    History::new(CompositeHistory::new(
        objects.into_iter().map(|h| h.0).collect(),
    ))
}

// Note: Items supplied by the iterator must already be of type Arc<T>
pub fn compose_history_from_items(
    objects: impl IntoIterator<Item = Arc<dyn InternalHistoryOps>>,
) -> History<CompositeHistory> {
    History::new(CompositeHistory::new(objects.into_iter().collect()))
}

impl InternalHistoryOps for CompositeHistory {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects
            .iter()
            .map(|object| object.iter())
            .kmerge()
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects
            .iter()
            .map(|object| object.iter_rev())
            .kmerge_by(|a, b| a >= b)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

impl<'graph, G: GraphViewOps<'graph> + Send + Sync> InternalHistoryOps for NodeView<'graph, G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let semantics = self.graph.node_time_semantics();
        let node = self.graph.core_node(self.node);
        GenLockedIter::from(node, move |node| {
            semantics
                .node_history_rev(node.as_ref(), &self.graph)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        ops::EarliestTime {
            graph: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        ops::LatestTime {
            graph: self.graph().clone(),
        }
        .apply(self.graph.core_graph(), self.node)
    }
}

impl<G: BoxableGraphView + Clone> InternalHistoryOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(&g, e) {
            match e.time() {
                Some(t) => iter::once(t).into_dyn_boxed(),
                None => {
                    let time_semantics = g.edge_time_semantics();
                    let edge = g.core_edge(e.pid());
                    match e.layer() {
                        None => GenLockedIter::from(edge, move |edge| {
                            time_semantics
                                .edge_history(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed(),
                        Some(layer) => {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_history(edge.as_ref(), g, layer_ids)
                                    .map(|(ti, _)| ti)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        }
                    }
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let g = &self.graph;
        let e = self.edge;
        if edge_valid_layer(&g, e) {
            match e.time() {
                Some(t) => iter::once(t).into_dyn_boxed(),
                None => {
                    let time_semantics = g.edge_time_semantics();
                    let edge = g.core_edge(e.pid());
                    match e.layer() {
                        None => GenLockedIter::from(edge, move |edge| {
                            time_semantics
                                .edge_history_rev(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti)
                                .into_dyn_boxed()
                        })
                        .into_dyn_boxed(),
                        Some(layer) => {
                            let layer_ids = LayerIds::One(layer);
                            GenLockedIter::from((edge, layer_ids), move |(edge, layer_ids)| {
                                time_semantics
                                    .edge_history_rev(edge.as_ref(), g, layer_ids)
                                    .map(|(ti, _)| ti)
                                    .into_dyn_boxed()
                            })
                            .into_dyn_boxed()
                        }
                    }
                }
            }
        } else {
            iter::empty().into_dyn_boxed()
        }
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

impl<P: PropertiesOps + Clone> InternalHistoryOps for TemporalPropertyView<P> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.props
            .temporal_iter(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.props
            .temporal_iter_rev(self.id)
            .map(|(t, _)| t)
            .into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter_rev(self).next()
    }
}

// FIXME: Currently holds a vector of all the intervals, might not be efficient. can't hold an iterator because they can't be reused
// Change to hold history object instead of vector, can call iterator to keep generating them
// TODO: Change to hold T, intervals calculated in iter()
pub struct Intervals(Vec<i64>);

impl Intervals {
    pub fn new(timestamps: impl IntoIterator<Item = i64>) -> Self {
        Intervals(
            timestamps
                .into_iter()
                .tuple_windows()
                .map(|(w1, w2)| w2 - w1)
                .collect(),
        )
    }

    pub fn items(&self) -> &Vec<i64> {
        &self.0
    }

    pub fn iter(&self) -> Iter<i64> {
        self.0.iter()
    }

    pub fn mean(&self) -> Option<f64> {
        if self.0.is_empty() {
            return None;
        }
        Some(self.0.iter().sum::<i64>() as f64 / self.0.len() as f64)
    }

    pub fn median(&self) -> Option<f64> {
        if self.0.is_empty() {
            return None;
        }
        let mut intervals_copy = self.0.to_vec();
        intervals_copy.sort_unstable();

        let mid = intervals_copy.len() / 2;
        if intervals_copy.len() % 2 == 0 {
            Some((intervals_copy[mid - 1] as f64 + intervals_copy[mid] as f64) / 2.0)
        } else {
            Some(intervals_copy[mid] as f64)
        }
    }

    pub fn max(&self) -> Option<i64> {
        self.0.iter().max().cloned()
    }

    pub fn min(&self) -> Option<i64> {
        self.0.iter().min().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::api::view::internal::CoreGraphOps;

    #[test]
    fn intervals() -> Result<(), Box<dyn std::error::Error>> {
        let timestamps = [1, 4, 10, 30];
        let interval = Intervals::new(timestamps);
        assert_eq!(interval.items(), &[3, 6, 20]);
        Ok(())
    }

    #[test]
    fn intervals_mean() -> Result<(), Box<dyn std::error::Error>> {
        let timestamps = [1, 4, 10, 30];
        let interval = Intervals::new(timestamps);
        assert_eq!(interval.mean(), Some(29f64 / 3f64));
        let timestamps2 = [1];
        let interval2 = Intervals::new(timestamps2);
        assert_eq!(interval2.mean(), None);
        Ok(())
    }

    #[test]
    fn intervals_median() -> Result<(), Box<dyn std::error::Error>> {
        let timestamps = [1, 30, 31, 40]; // intervals are 29, 1, 9
        let interval = Intervals::new(timestamps);
        assert_eq!(interval.median(), Some(9.0));
        let timestamps2 = [1];
        let interval2 = Intervals::new(timestamps2);
        assert_eq!(interval2.median(), None);
        Ok(())
    }

    #[test]
    fn intervals_max() -> Result<(), Box<dyn std::error::Error>> {
        let timestamps = [1, 30, 31, 40]; // intervals are 29, 1, 9
        let interval = Intervals::new(timestamps);
        assert_eq!(interval.max(), Some(29));
        let timestamps2 = [1];
        let interval2 = Intervals::new(timestamps2);
        assert_eq!(interval2.max(), None);
        Ok(())
    }

    // test nodes and edges
    #[test]
    fn basic() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let dumbledore_node = graph
            .add_node(1, "Dumbledore", [("type", Prop::str("Character"))], None)
            .unwrap();

        let harry_node = graph
            .add_node(2, "Harry", [("type", Prop::str("Character"))], None)
            .unwrap();

        let character_edge = graph
            .add_edge(
                3,
                "Dumbledore",
                "Harry",
                [("meeting", Prop::str("Character Co-occurrence"))],
                None,
            )
            .unwrap();

        // create dumbledore node history object
        let dumbledore_node_history_object = History::new(dumbledore_node.clone());
        assert_eq!(
            dumbledore_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(1, 0), TimeIndexEntry::new(3, 2)]
        );

        // create Harry node history object
        let harry_node_history_object = History::new(harry_node.clone());
        assert_eq!(
            harry_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(2, 1), TimeIndexEntry::new(3, 2)]
        );

        // create edge history object
        let edge_history_object = History::new(character_edge.clone());
        assert_eq!(
            edge_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(3, 2)]
        );

        // create Composite History Object
        let tmp_vector: Vec<Arc<dyn InternalHistoryOps>> = vec![
            Arc::new(dumbledore_node),
            Arc::new(harry_node),
            Arc::new(character_edge),
        ];
        let composite_history_object = compose_history_from_items(tmp_vector);
        assert_eq!(
            composite_history_object.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(3, 2)
            ]
        );

        Ok(())
    }

    // test a layer
    #[test]
    fn test_single_layer() -> Result<(), Box<dyn std::error::Error>> {
        // generate graph
        let graph = Graph::new();
        let dumbledore_node = graph
            .add_node(1, "Dumbledore", [("type", Prop::str("Character"))], None)
            .unwrap();
        let dumbledore_node_id = dumbledore_node.id();

        let harry_node = graph
            .add_node(2, "Harry", [("type", Prop::str("Character"))], None)
            .unwrap();
        let harry_node_id = harry_node.id();

        let character_edge = graph
            .add_edge(
                3,
                "Dumbledore",
                "Harry",
                [("meeting", Prop::str("Character Co-occurrence"))],
                None,
            )
            .unwrap();

        // add broom node
        let broom_node = graph
            .add_node(4, "Broom", [("type", Prop::str("Magical Object"))], None)
            .unwrap();
        let broom_node_id = broom_node.id();

        let broom_harry_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Harry",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_harry_magical_edge_id = broom_harry_magical_edge.id();

        let broom_dumbledore_magical_edge = graph
            .add_edge(
                4,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Flying on broom"))],
                Some("Magical Object Uses"),
            )
            .unwrap();
        let broom_dumbledore_magical_edge_id = broom_dumbledore_magical_edge.id();

        let broom_harry_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Harry",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_harry_normal_edge_id = broom_harry_normal_edge.id();

        let broom_dumbledore_normal_edge = graph
            .add_edge(
                5,
                "Broom",
                "Dumbledore",
                [("use", Prop::str("Cleaning with broom"))],
                None,
            )
            .unwrap();
        let broom_dumbledore_normal_edge_id = broom_dumbledore_normal_edge.id();

        let layer_id = graph.get_layer_id("Magical Object Uses").unwrap();

        // node history objects
        let dumbledore_history = History::new(dumbledore_node);
        assert_eq!(
            dumbledore_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(1, 0),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(5, 7)
            ]
        );

        let harry_history = History::new(harry_node);
        assert_eq!(
            harry_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(2, 1),
                TimeIndexEntry::new(3, 2),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(5, 6)
            ]
        );

        let broom_history = History::new(broom_node);
        assert_eq!(
            broom_history.iter().collect_vec(),
            vec![
                TimeIndexEntry::new(4, 3),
                TimeIndexEntry::new(4, 4),
                TimeIndexEntry::new(4, 5),
                TimeIndexEntry::new(5, 6),
                TimeIndexEntry::new(5, 7)
            ]
        );

        // edge history objects
        // let character_edge_history = HistoryImplemented::new(character_edge);
        // character_edge_history.print("Character Edge History: ");
        //
        // let broom_harry_history = HistoryImplemented::new(broom_harry_edge);
        // broom_harry_history.print("Broom-Harry History: ");
        //
        // let broom_dumbledore_history = HistoryImplemented::new(broom_dumbledore_edge);
        // broom_dumbledore_history.print("Broom-Dumbledore History: ");

        // make graphview using layer
        let magical_graph_view = graph.layers("Magical Object Uses").unwrap();
        let dumbledore_node_magical_view =
            magical_graph_view.node(dumbledore_node_id.clone()).unwrap();
        let harry_node_magical_view = magical_graph_view.node(harry_node_id.clone()).unwrap();
        let broom_node_magical_view = magical_graph_view.node(broom_node_id.clone()).unwrap();

        // FIXME: After applying the layer, the node still returns the same history information. Wait for new update
        let harry_magical_history = History::new(harry_node_magical_view);
        harry_magical_history.print("Harry Magical History: ");

        let x = magical_graph_view
            .edge(broom_node_id, dumbledore_node_id)
            .unwrap();

        println!("{:?}", x.layer_name());

        Ok(())
    }
}
