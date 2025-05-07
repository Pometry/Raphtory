#![allow(unused_imports)]
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory::core::storage::timeindex::{AsTime, TimeIndexEntry};
use raphtory::db::api::properties::internal::TemporalPropertiesOps;
use raphtory::db::api::view::internal::{InternalLayerOps, TimeSemantics};
use raphtory::db::api::view::{BoxedLIter, IntoDynBoxed};
use raphtory::db::graph::edge::EdgeView;
use raphtory::db::graph::node::NodeView;
use raphtory::prelude::*;

pub struct RaphtoryTime {
    time_index: TimeIndexEntry
}

impl RaphtoryTime {
    fn apply(time_index: TimeIndexEntry) -> Self { Self{time_index} }
    pub fn dt(&self) -> Option<DateTime<Utc>> {
        self.time_index.dt()
    }
    pub fn epoch(&self) -> i64 { 
        self.time_index.t()
    }
}

pub trait InternalHistoryOps {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
}

// FIXME: Doesn't support deletions of edges yet
pub struct HistoryImplemented<T>(T);

impl<T: InternalHistoryOps> HistoryImplemented<T> {
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

impl<T: InternalHistoryOps> HistoryImplemented<T> {
    pub fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }

    pub fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }

    pub fn merge<R: InternalHistoryOps>(self, right: HistoryImplemented<R>) -> HistoryImplemented<MergedHistory<T, R>> {
        HistoryImplemented::new(MergedHistory::new(self.0, right.0))
    }
    
    // FIXME: Ask if we want this vec to contain InternalHistoryOps objects or HistoryImplemented objects (like above). Also ask about 'static.
    pub fn compose(self, mut others: Vec<Box<dyn InternalHistoryOps>>) -> HistoryImplemented<CompositeHistory>
    where 
        T: 'static 
    {
        others.push(Box::new(self.0));
        HistoryImplemented::new(CompositeHistory::new(others))
    }

    pub fn earliest_time(&self) -> Option<RaphtoryTime> {
        self.0.iter().next().map(|t| RaphtoryTime::apply(t.clone()))
    }

    pub fn latest_time(&self) -> Option<RaphtoryTime> {
        self.0.iter_rev().next().map(|t| RaphtoryTime::apply(t.clone()))
    }

    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

/// Separate from CompositeHistory in that it can only hold two items. They can be nested.
/// Slightly more efficient because we are calling iter.merge() instead of iter.kmerge(). Efficiency benefits are lost if we nest these objects too much
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
}

/// Holds a vector of multiple objects implementing InternalHistoryOps
pub struct CompositeHistory {
    history_objects: Vec<Box<dyn InternalHistoryOps>>
}

impl CompositeHistory {
    pub fn new(history_objects: Vec<Box<dyn InternalHistoryOps>>) -> Self {
        Self { history_objects }
    }
}

impl InternalHistoryOps for CompositeHistory {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects.iter().map(|object| object.iter()).kmerge().into_dyn_boxed()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.history_objects.iter().map(|object| object.iter_rev()).kmerge_by(|a, b| a >= b).into_dyn_boxed()
    }
}

impl<G: TimeSemantics> InternalHistoryOps for NodeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.graph.node_history(self.node)
    }

    // Implementation is not efficient, only for testing purposes
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let mut x = self.graph.node_history(self.node).collect_vec();
        x.reverse();
        x.into_iter().into_dyn_boxed()
    }
}

impl<G: TimeSemantics + InternalLayerOps> InternalHistoryOps for EdgeView<G> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.graph.edge_history(self.edge, self.graph.layer_ids())
    }

    // Implementation is not efficient, only for testing purposes
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        let mut x = self.graph.edge_history(self.edge, self.graph.layer_ids()).collect_vec();
        x.reverse();
        x.into_iter().into_dyn_boxed()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let edge = graph.add_edge(
        3,
        "Gandalf",
        "Harry",
        [(
            "meeting",
            Prop::str("Character Co-occurrence"),
        )],
        None,
    ).unwrap();
    
    let type_prop_id = gandalf_node.get_temporal_prop_id("type");
    let meeting_prop_id = edge.get_temporal_prop_id("meeting");

    // create gandalf node history object
    let gandalf_node_history_object = HistoryImplemented::new(gandalf_node.clone());
    gandalf_node_history_object.print("Gandalf Node history: ");

    // create Harry node history object
    let harry_node_history_object = HistoryImplemented::new(harry_node.clone());
    harry_node_history_object.print("Harry Node history: ");

    // create edge history object
    let edge_history_object = HistoryImplemented::new(edge.clone());
    edge_history_object.print("Gandalf-Harry Edge history: ");
    
    // create Composite History Object
    let tmp_vector: Vec<Box<dyn InternalHistoryOps>> = vec![Box::new(harry_node), Box::new(edge)];
    let composite_history_object = gandalf_node_history_object.compose(tmp_vector);
    composite_history_object.print("Composite history: ");
    
    println!("Composite history's earliest time: {}", composite_history_object.earliest_time().unwrap().epoch());
    println!("Composite history's latest time: {}", composite_history_object.latest_time().unwrap().epoch());

    Ok(())
}
