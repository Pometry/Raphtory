#![allow(unused_imports)]
#![allow(dead_code)]

use crate::core::storage::timeindex::{AsTime, TimeIndexEntry};
use crate::db::api::properties::internal::{PropertiesOps, TemporalPropertiesOps};
use crate::db::api::properties::{TemporalProperties, TemporalPropertyView};
use crate::db::api::view::internal::{InternalLayerOps, TimeSemantics};
use crate::db::api::view::{BoxedLIter, IntoDynBoxed};
use crate::db::graph::edge::EdgeView;
use crate::db::graph::node::NodeView;
use crate::prelude::*;
use arrow_ipc::Time;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
// TODO: Do we want to implement gt, lt, etc?

pub trait InternalHistoryOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    // Option<> because they are options in time_semantics.rs
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
}

// FIXME: Doesn't support deletions of edges yet
#[derive(Debug, Clone)]
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

    pub fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    pub fn collect_items(&self) -> Vec<TimeIndexEntry> {
        self.0.iter().collect_vec()
    }

    pub fn collect_timestamps(&self) -> Vec<i64> {
        self.0.iter().map(|x| x.t()).collect_vec()
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

impl<T: InternalHistoryOps> PartialEq for History<T> {
    fn eq(&self, other: &Self) -> bool {
        // Optimization: check latest_time first
        if self.latest_time() != other.latest_time() {
            return false;
        }

        // If latest_time was None for both, iter().eq(other.iter()) will correctly return true.
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

// Note: All the items held by their respective HistoryImplemented objects must already be of type Box<T>
pub fn compose_multiple_histories(
    objects: impl IntoIterator<Item = History<Arc<dyn InternalHistoryOps>>>,
) -> History<CompositeHistory> {
    History::new(CompositeHistory::new(
        objects.into_iter().map(|h| h.0).collect(),
    ))
}

// Note: Items supplied by the iterator must already be of type Box<T>
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

    // Performance consideration: Is it more efficient to use the kmerged iterator or to call each object's implemented earliest_time() and return the smallest?
    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

// TODO: Change earliest_time and latest_time implementations to use the built in earliest_time and latest_time functions
// TODO: Might wanna remove NodeView and EdgeView if I'm gonna have the NodeHistory struct above
// They are probably more efficient. New PR will have iter_rev implementation
impl<G: TimeSemantics + Send + Sync> InternalHistoryOps for NodeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.graph.node_history(self.node)
    }

    // Implementation is not efficient, only for testing purposes
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let mut x = self.graph.node_history(self.node).collect_vec();
        x.reverse();
        x.into_iter().into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

impl<G: TimeSemantics + InternalLayerOps + Send + Sync> InternalHistoryOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.graph.edge_history(self.edge, self.graph.layer_ids())
    }

    // FIXME: Implementation is not efficient, only for testing purposes
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let mut x = self
            .graph
            .edge_history(self.edge, self.graph.layer_ids())
            .collect_vec();
        x.reverse();
        x.into_iter().into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

impl<P: PropertiesOps + Clone> InternalHistoryOps for TemporalPropertyView<P> {
    // FIXME: This probably isn't great, also we're defining iter() twice but I'm not getting an error
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history()
            .map(|x| TimeIndexEntry::from(x))
            .into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let mut x = InternalHistoryOps::iter(self).collect::<Vec<TimeIndexEntry>>();
        x.reverse();
        x.into_iter().into_dyn_boxed()
    }

    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter(self).next()
    }

    fn latest_time(&self) -> Option<TimeIndexEntry> {
        InternalHistoryOps::iter_rev(self).next()
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
