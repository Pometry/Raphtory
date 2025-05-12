#![allow(unused_imports)]
#![allow(dead_code)]

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use crate::core::storage::timeindex::{AsTime, TimeIndexEntry};
use crate::db::api::properties::internal::TemporalPropertiesOps;
use crate::db::api::view::internal::{InternalLayerOps, TimeSemantics};
use crate::db::api::view::{BoxedLIter, IntoDynBoxed};
use crate::db::graph::edge::EdgeView;
use crate::db::graph::node::NodeView;
use crate::prelude::*;

// TODO: Do we want to implement gt, lt, etc?

pub trait InternalHistoryOps: Send + Sync {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    // Option<> because they are options in time_semantics.rs
    fn earliest_time(&self) -> Option<TimeIndexEntry>;
    fn latest_time(&self) -> Option<TimeIndexEntry>;
}

// FIXME: Doesn't support deletions of edges yet
// TODO: Implement hashable so they can be used in maps in python
#[derive(Clone)]
pub struct History<T>(pub T);

impl<T: InternalHistoryOps> History<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }

    // see if theres a way to get secondary temporal information (two entries for the same time value)

    // Currently wont include any deletions which are available on edges but are not available on nodes
    // We will have node deletions soon, and we'll get 4 iterators in here, for insertions and deletions
    // TODO: Implement tests (filters, combinations of filters, layer filter on edges, windowing filter)
    // find some non-trivial data (like AML example) and run some tests.
    // try to do some operations using the things above and make sure the desired effect is observed on the history object.
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

/// Separate from CompositeHistory in that it can only hold two items. They can be nested.
/// More efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
/// TODO: Write benchmark to evaluate performance benefit tradeoff (ie when there are no more performance benefits)
pub struct MergedHistory<L, R> {
    left: L,
    right: R
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
        self.left.iter_rev().merge_by(self.right.iter_rev(), |a, b| a >= b).into_dyn_boxed()
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
    history_objects: Vec<Box<dyn InternalHistoryOps>>
}

impl CompositeHistory {
    pub fn new(history_objects: Vec<Box<dyn InternalHistoryOps>>) -> Self {
        Self { history_objects }
    }
}

// Note: All the items held by their respective HistoryImplemented objects must already be of type Box<T>
pub fn compose_multiple_histories(objects: impl IntoIterator<Item = History<Box<dyn InternalHistoryOps>>>) -> History<CompositeHistory> {
    History::new(CompositeHistory::new(objects.into_iter().map(|h| h.0).collect()))
}

// Note: Items supplied by the iterator must already be of type Box<T>
pub fn compose_history_from_items(objects: impl IntoIterator<Item = Box<dyn InternalHistoryOps>>) -> History<CompositeHistory> {
    History::new(CompositeHistory::new(objects.into_iter().collect()))
}

impl InternalHistoryOps for CompositeHistory {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects.iter().map(|object| object.iter()).kmerge().into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects.iter().map(|object| object.iter_rev()).kmerge_by(|a, b| a >= b).into_dyn_boxed()
    }
    
    // Performance consideration: Is it more efficient to use the kmerged iterator or to call each object's implemented earliest_time() and return the smallest?
    fn earliest_time(&self) -> Option<TimeIndexEntry> {
        self.iter().next()
    }
    
    fn latest_time(&self) -> Option<TimeIndexEntry> {
        self.iter_rev().next()
    }
}

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
        let mut x = self.graph.edge_history(self.edge, self.graph.layer_ids()).collect_vec();
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

#[cfg(test)]
mod tests {
    use crate::db::api::view::internal::CoreGraphOps;
    use super::*;

    // test nodes and edges
    #[test]
    fn basic() -> Result<(), Box<dyn std::error::Error>> {
        let graph = Graph::new();
        let gandalf_node = graph.add_node(
            1,
            "Gandalf",
            [("type", Prop::str("Character"))],
            None
        ).unwrap();

        let harry_node = graph.add_node(
            2,
            "Harry",
            [("type", Prop::str("Character"))],
            None
        ).unwrap();

        let character_edge = graph.add_edge(
            3,
            "Gandalf",
            "Harry",
            [(
                "meeting",
                Prop::str("Character Co-occurrence"),
            )],
            None,
        ).unwrap();

        // create gandalf node history object
        let gandalf_node_history_object = History::new(gandalf_node.clone());
        assert_eq!(gandalf_node_history_object.iter().collect_vec(),
                    vec![TimeIndexEntry::new(1, 0),
                         TimeIndexEntry::new(3, 2)]);

        // create Harry node history object
        let harry_node_history_object = History::new(harry_node.clone());
        assert_eq!(harry_node_history_object.iter().collect_vec(),
                   vec![TimeIndexEntry::new(2, 1),
                        TimeIndexEntry::new(3, 2)]);

        // create edge history object
        let edge_history_object = History::new(character_edge.clone());
        assert_eq!(edge_history_object.iter().collect_vec(),
                   vec![TimeIndexEntry::new(3, 2)]);

        // create Composite History Object
        let tmp_vector: Vec<Box<dyn InternalHistoryOps>> = vec![Box::new(gandalf_node), Box::new(harry_node), Box::new(character_edge)];
        let composite_history_object = compose_history_from_items(tmp_vector);
        assert_eq!(composite_history_object.iter().collect_vec(),
                   vec![TimeIndexEntry::new(1, 0),
                        TimeIndexEntry::new(2, 1),
                        TimeIndexEntry::new(3, 2),
                        TimeIndexEntry::new(3, 2),
                        TimeIndexEntry::new(3, 2)]);

        Ok(())
    }

    // test a layer
    #[test]
    fn test_single_layer() -> Result<(), Box<dyn std::error::Error>> {
        // generate graph
        let graph = Graph::new();
        let gandalf_node = graph.add_node(
            1,
            "Gandalf",
            [("type", Prop::str("Character"))],
            None
        ).unwrap();
        let gandalf_node_id = gandalf_node.id();

        let harry_node = graph.add_node(
            2,
            "Harry",
            [("type", Prop::str("Character"))],
            None
        ).unwrap();
        let harry_node_id = harry_node.id();

        let character_edge = graph.add_edge(
            3,
            "Gandalf",
            "Harry",
            [(
                "meeting",
                Prop::str("Character Co-occurrence"),
            )],
            None,
        ).unwrap();

        // add broom node
        let broom_node = graph.add_node(
            4,
            "Broom",
            [("type", Prop::str("Magical Object"))],
            None
        ).unwrap();
        let broom_node_id = broom_node.id();

        let broom_harry_magical_edge = graph.add_edge(
            4,
            "Broom",
            "Harry",
            [(
                "use",
                Prop::str("Flying on broom"),
            )],
            Some("Magical Object Uses"),
        ).unwrap();
        let broom_harry_magical_edge_id = broom_harry_magical_edge.id();

        let broom_gandalf_magical_edge = graph.add_edge(
            4,
            "Broom",
            "Gandalf",
            [(
                "use",
                Prop::str("Flying on broom"),
            )],
            Some("Magical Object Uses"),
        ).unwrap();
        let broom_gandalf_magical_edge_id = broom_gandalf_magical_edge.id();

        let broom_harry_normal_edge = graph.add_edge(
            5,
            "Broom",
            "Harry",
            [(
                "use",
                Prop::str("Cleaning with broom"),
            )],
            None,
        ).unwrap();
        let broom_harry_normal_edge_id = broom_harry_normal_edge.id();

        let broom_gandalf_normal_edge = graph.add_edge(
            5,
            "Broom",
            "Gandalf",
            [(
                "use",
                Prop::str("Cleaning with broom"),
            )],
            None,
        ).unwrap();
        let broom_gandalf_normal_edge_id = broom_gandalf_normal_edge.id();

        let layer_id = graph.get_layer_id("Magical Object Uses").unwrap();

        // node history objects
        let gandalf_history = History::new(gandalf_node);
        assert_eq!(gandalf_history.iter().collect_vec(),
                   vec![TimeIndexEntry::new(1, 0),
                        TimeIndexEntry::new(3, 2),
                        TimeIndexEntry::new(4, 5),
                        TimeIndexEntry::new(5, 7)]);

        let harry_history = History::new(harry_node);
        assert_eq!(harry_history.iter().collect_vec(),
                   vec![TimeIndexEntry::new(2, 1),
                        TimeIndexEntry::new(3, 2),
                        TimeIndexEntry::new(4, 4),
                        TimeIndexEntry::new(5, 6)]);

        let broom_history = History::new(broom_node);
        assert_eq!(broom_history.iter().collect_vec(),
                   vec![TimeIndexEntry::new(4, 3),
                        TimeIndexEntry::new(4, 4),
                        TimeIndexEntry::new(4, 5),
                        TimeIndexEntry::new(5, 6),
                        TimeIndexEntry::new(5, 7)]);

        // edge history objects
        // let character_edge_history = HistoryImplemented::new(character_edge);
        // character_edge_history.print("Character Edge History: ");
        //
        // let broom_harry_history = HistoryImplemented::new(broom_harry_edge);
        // broom_harry_history.print("Broom-Harry History: ");
        //
        // let broom_gandalf_history = HistoryImplemented::new(broom_gandalf_edge);
        // broom_gandalf_history.print("Broom-Gandalf History: ");

        // make graphview using layer
        let magical_graph_view = graph.layers("Magical Object Uses").unwrap();
        let gandalf_node_magical_view = magical_graph_view.node(gandalf_node_id.clone()).unwrap();
        let harry_node_magical_view = magical_graph_view.node(harry_node_id.clone()).unwrap();
        let broom_node_magical_view = magical_graph_view.node(broom_node_id.clone()).unwrap();

        // FIXME: After applying the layer, the node still returns the same history information. Wait for new update
        let harry_magical_history = History::new(harry_node_magical_view);
        harry_magical_history.print("Harry Magical History: ");

        let x = magical_graph_view.edge(broom_node_id, gandalf_node_id).unwrap();

        println!("{:?}", x.layer_name());

        Ok(())
    }

}
