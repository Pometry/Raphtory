#![allow(unused_imports)]
#![allow(dead_code)]

use std::iter::Peekable;
use std::slice::Iter;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use raphtory::core::entities::LayerIds;
use raphtory::core::entities::nodes::node_ref::AsNodeRef;
use raphtory::prelude::*;
use raphtory::core::storage::timeindex::{AsTime, TimeIndex, TimeIndexEntry};
use raphtory::core::utils::iter;
use raphtory::db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps};
use raphtory::db::api::view::{Base, BoxedLIter, IntoDynBoxed};
use raphtory::db::api::view::internal::{InternalLayerOps, TimeSemantics};
use raphtory::db::graph::edge::EdgeView;
use raphtory::db::graph::node::NodeView;

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

pub trait HistoryObject {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry>;
    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry>;
    fn earliest_time(&self) -> Option<RaphtoryTime>;
    fn latest_time(&self) -> Option<RaphtoryTime>;
    fn print(&self, prelude: &str);
}

// TODO: Add iterators for deletions of edges, add nodes later
pub struct HistoryImplemented<T>(T);

impl<T: InternalHistoryOps> HistoryImplemented<T> {
    pub fn new(item: T) -> Self {
        Self(item)
    }
    
    // see if theres a way to get secondary temporal information (two entries for the same time value)
    
    // Currently wont include any deletions which are available on edges but are not available on nodes
    // We will have node deletions soon, and we'll get 4 iterators in here, for insertions and deletions
    // TODO: Implement tests (filters, combinations of filters, layer filter on edges, windowing filter)
}

impl<T: InternalHistoryOps> HistoryObject for HistoryImplemented<T> {
    fn iter(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter()
    }

    fn iter_rev(&self) -> BoxedLIter<TimeIndexEntry> {
        self.0.iter_rev()
    }
    
    fn earliest_time(&self) -> Option<RaphtoryTime> {
        self.0.iter().next().map(|t| RaphtoryTime::apply(t.clone()))
    }

    fn latest_time(&self) -> Option<RaphtoryTime> {
        self.0.iter_rev().next().map(|t| RaphtoryTime::apply(t.clone()))
    }

    fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.0.iter().collect::<Vec<_>>());
    }
}

// might be a good idea to not store different HistoryObjects and instead store Vec<Box<dyn InternalHistoryOps>>
pub struct CompositeHistory {
    history_objects: Vec<Box<dyn HistoryObject>>
}

impl CompositeHistory {
    fn new(history_objects: Vec<Box<dyn HistoryObject>>) -> Self {
        Self { history_objects }
    }
    
    fn create_empty() -> Self {
        Self { history_objects: Vec::new() }
    }

    fn add_boxed_history_object(&mut self, history_object: Box<dyn HistoryObject>) {
        self.history_objects.push(history_object);
    }
    
    fn add_history_object<T: InternalHistoryOps + 'static>(&mut self, history_object: HistoryImplemented<T>) {
        self.history_objects.push(Box::new(history_object));
    }

    // FIXME: unable to get this working, can't add items directly to the composite history object
    // fn add_item(&mut self, item: Box<dyn InternalHistoryOps>) {
    //     self.history_objects.push(HistoryObject::new_boxed(item));
    // }

    // Does this implementation improve performance? It avoids calling each HistoryObject's earliest_time or latest_time implementations
    // It avoids a TimeIndexEntry clone operation and a RaphtoryTime object creation for each HistoryObject
    fn earliest_time(&self) -> Option<RaphtoryTime> {
        let mut earliest_time: Option<TimeIndexEntry> = None;
        for history_object in &self.history_objects {
            let mut tmp_iter = history_object.iter();
            if let Some(tmp_time) = tmp_iter.next() {
                if earliest_time.is_none() || tmp_time.t() < earliest_time?.t() {
                    earliest_time = Some(tmp_time);
                }
            }
        }
        earliest_time.map(|t| RaphtoryTime::apply(t.clone()))
    }

    fn latest_time(&self) -> Option<RaphtoryTime> {
        let mut latest_time: Option<TimeIndexEntry> = None;
        for history_object in &self.history_objects {
            let mut tmp_iter = history_object.iter_rev();
            if let Some(tmp_time) = tmp_iter.next() {
                if latest_time.is_none() || tmp_time.t() > latest_time?.t() {
                    latest_time = Some(tmp_time);
                }
            }
        }
        latest_time.map(|t| RaphtoryTime::apply(t.clone()))
    }
    
    fn print(&self, prelude: &str) {
        let mut count = 0;
        println!("{}", prelude);
        for history_object in &self.history_objects {
            print!("\tHistory Object #{}: ", count);
            history_object.print("");
            count = count + 1;
        }
    }
}

impl<G: TimeSemantics + InternalLayerOps> InternalHistoryOps for NodeView<G, G> {
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

impl<G: TimeSemantics + InternalLayerOps> InternalHistoryOps for EdgeView<G, G> {
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
    let gandalf_node_history_object = HistoryImplemented::new(gandalf_node);
    gandalf_node_history_object.print("Gandalf Node history: ");

    // create Harry node history object
    let harry_node_history_object = HistoryImplemented::new(harry_node);
    harry_node_history_object.print("Harry Node history: ");

    // create edge history object
    let edge_history_object = HistoryImplemented::new(edge);
    edge_history_object.print("Gandalf-Harry Edge history: ");
    
    // create Composite History Object
    let tmp_vector: Vec<Box<dyn HistoryObject>> = vec![Box::new(gandalf_node_history_object), Box::new(harry_node_history_object)];
    let mut composite_history_object = CompositeHistory::new(tmp_vector);
    composite_history_object.add_history_object(edge_history_object);
    composite_history_object.print("Composite history: ");
    
    println!("Composite history's earliest time: {}", composite_history_object.earliest_time().unwrap().epoch());
    println!("Composite history's latest time: {}", composite_history_object.latest_time().unwrap().epoch());

    Ok(())
}
