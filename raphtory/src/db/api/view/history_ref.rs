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
                history::{History, InternalHistoryOps, Intervals, MergedHistory},
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

// FIXME: Doesn't support deletions of edges yet
#[derive(Debug, Clone, Copy)]
pub struct HistoryRef<'item, T>(pub &'item T);

impl<T: InternalHistoryOps + ?Sized> InternalHistoryOps for &T {
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

impl<'item, T: InternalHistoryOps> HistoryRef<'item, T> {
    pub fn new(item: &'item T) -> Self {
        Self(item)
    }
}

impl<'item, T: InternalHistoryOps> HistoryRef<'item, T> {
    pub fn iter(&self) -> BoxedLIter<'item, TimeIndexEntry> {
        self.0.iter()
    }

    pub fn iter_t(&self) -> BoxedLIter<'item, i64> {
        self.0.iter().map(|t| t.t()).into_dyn_boxed()
    }

    pub fn iter_rev(&self) -> BoxedLIter<'item, TimeIndexEntry> {
        self.0.iter_rev()
    }

    pub fn iter_rev_t(&self) -> BoxedLIter<'item, i64> {
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

    pub fn intervals(&self) -> Intervals<T> {
        Intervals::new(self.0)
    }

    pub fn merge<R: InternalHistoryOps>(
        self,
        right: HistoryRef<'item, R>,
    ) -> History<MergedHistory<&'item T, &'item R>> {
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

impl<'item, T: InternalHistoryOps + Clone> HistoryRef<'item, T> {
    pub fn to_owned(&self) -> History<T> {
        History::new(self.0.clone())
    }
}

impl<'item, T: InternalHistoryOps> PartialEq for HistoryRef<'item, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'item, T: InternalHistoryOps, L, I: Copy> PartialEq<L> for HistoryRef<'item, T>
where
    for<'a> &'a L: IntoIterator<Item = &'a I>,
    TimeIndexEntry: PartialEq<I>,
{
    fn eq(&self, other: &L) -> bool {
        self.iter().eq(other.into_iter().copied())
    }
}

impl<'item, T: InternalHistoryOps> Eq for HistoryRef<'item, T> {}

impl<'item, T: InternalHistoryOps> std::hash::Hash for HistoryRef<'item, T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for item in self.iter() {
            item.hash(state);
        }
    }
}

/// Separate from CompositeHistory in that it can only hold two items. They can be nested.
/// More efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
/// TODO: Write benchmark to evaluate performance benefit tradeoff (ie when there are no more performance benefits)
pub struct MergedHistoryRef<'item, L, R> {
    left: &'item L,
    right: &'item R,
}

impl<'item, L: InternalHistoryOps, R: InternalHistoryOps> MergedHistoryRef<'item, L, R> {
    pub fn new(left: &'item L, right: &'item R) -> Self {
        Self { left, right }
    }
}

impl<'item, L: InternalHistoryOps, R: InternalHistoryOps> InternalHistoryOps
    for MergedHistoryRef<'item, L, R>
{
    fn iter(&self) -> BoxedLIter<'item, TimeIndexEntry> {
        self.left.iter().merge(self.right.iter()).into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<'item, TimeIndexEntry> {
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::api::view::internal::CoreGraphOps;

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
        let dumbledore_node_history_object = HistoryRef::new(&dumbledore_node);
        assert_eq!(
            dumbledore_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(1, 0), TimeIndexEntry::new(3, 2)]
        );

        // create Harry node history object
        let harry_node_history_object = HistoryRef::new(&harry_node);
        assert_eq!(
            harry_node_history_object.iter().collect_vec(),
            vec![TimeIndexEntry::new(2, 1), TimeIndexEntry::new(3, 2)]
        );

        // create edge history object
        let edge_history_object = HistoryRef::new(&character_edge);
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
