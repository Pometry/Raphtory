#![allow(unused_imports)]
#![allow(dead_code)]

use std::iter::Peekable;
use std::slice::Iter;
use chrono::{DateTime, Utc};
use raphtory::core::entities::LayerIds;
use raphtory::prelude::*;
use raphtory::core::storage::timeindex::{AsTime, TimeIndex, TimeIndexEntry};
use raphtory::core::utils::iter;
use raphtory::db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps};
use raphtory::db::api::view::BoxedLIter;
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

pub trait HistoryOps {
    fn earliest_time(&self) -> Option<RaphtoryTime>;
    fn latest_time(&self) -> Option<RaphtoryTime>;

}

// TODO: Add iterators for deletions of edges, add nodes later
pub struct HistoryObject<'a, T> {
    /// Vector of ints that holds the history data points
    front_iter: Iter<'a, TimeIndexEntry>,
    back_iter: Iter<'a, TimeIndexEntry>,
}

impl<'a> HistoryObject<'a> {
    /// TODO: Make not public once from_node and from_edge functions work
    pub fn new(front_iter: Iter<'a, TimeIndexEntry>,
           back_iter: Iter<'a, TimeIndexEntry>) -> Self {
        Self { front_iter, back_iter }
    }
    // from_node and from_edge constructors to create the iterators
    // see if theres a way to get secondary temporal information (two entries for the same time value)

    // TODO: Fix from_node and from_edge functions
    pub fn from_node<'graph, G: GraphViewOps<'graph>>(node: NodeView<G, G>) /* -> Self */ {
        let node_history = node.graph.node_history(node.node).collect::<Vec<_>>();
        let mut node_history_back = node.graph.node_history(node.node).collect::<Vec<_>>();
        node_history_back.reverse();
        // HistoryObject::new(node_history.iter(), node_history_back.iter())
    }

    pub fn from_edge<'graph, G: GraphViewOps<'graph>>(edge: EdgeView<G, G>) /*-> Self */{
        let edge_history = edge.graph.edge_history(edge.edge, edge.graph.layer_ids()).collect::<Vec<_>>();
        let mut edge_history_back = edge.graph.edge_history(edge.edge, edge.graph.layer_ids()).collect::<Vec<_>>();
        edge_history_back.reverse();
        // HistoryObject::new(edge_history.iter(), edge_history_back.iter())
    }
    
    // Currently wont include any deletions which are available on edges but are not available on nodes
    // We will have node deletions soon, and we'll get 4 iterators in here, for insertions and deletions
    
    pub fn print(&self, prelude: &str) {
        println!("{}{:?}", prelude, self.front_iter.clone().collect::<Vec<_>>());
    }
    
    // TODO: Implement tests (filters, combinations of filters, layer filter on edges, windowing filter)
}

impl<'a> InternalHistoryOps for HistoryObject<'a> {
    fn earliest_time(&self) -> Option<RaphtoryTime> {
        self.front_iter.as_slice().first().map(|t| RaphtoryTime::apply(t.clone()))
    }

    fn latest_time(&self) -> Option<RaphtoryTime> {
        self.back_iter.as_slice().first().map(|t| RaphtoryTime::apply(t.clone()))
    }
}

pub struct CompositeHistory<'a> {
    history_objects: Vec<HistoryObject<'a>>,
}

impl<'a> CompositeHistory<'a> {
    fn new(history_objects: Vec<HistoryObject<'a>>) -> Self {
        Self { history_objects }
    }

    fn add_history_object(&mut self, history_object: HistoryObject<'a>) {
        self.history_objects.push(history_object);
    }

    fn add_node<'graph, G: GraphViewOps<'graph>>(&mut self, node: NodeView<G, G>) {
        // self.history_objects.push(HistoryObject::from_node(node));
    }

    fn add_edge<'graph, G: GraphViewOps<'graph>>(&mut self, edge: EdgeView<G, G>) {
        // self.history_objects.push(HistoryObject::from_edge(edge));
    }
    
    fn print(&self, prelude: &str) {
        let mut count = 0;
        for history_object in &self.history_objects {
            print!("History Object #{}: ", count);
            history_object.print("");
            count = count + 1;
        }
    }
}

impl<'a> InternalHistoryOps for CompositeHistory<'a> {
    // Does this implementation improve performance? It avoids calling each HistoryObject's earliest_time or latest_time implementations
    // It avoids an iter.as_slice() and TimeIndexEntry clone operation for each HistoryObject
    fn earliest_time(&self) -> Option<RaphtoryTime> {
        let mut earliest_time: Option<&TimeIndexEntry> = None;
        for history_object in &self.history_objects {
            let mut peekable_iter = history_object.front_iter.clone().peekable();
            if let Some(&tmp_time) = peekable_iter.peek(){
                if earliest_time.is_none() || tmp_time.t() < earliest_time?.t() {
                    earliest_time = Some(tmp_time)
                }
            }
        }
        earliest_time.map(|t| RaphtoryTime::apply(t.clone()))
    }

    fn latest_time(&self) -> Option<RaphtoryTime> {
        let mut latest_time: Option<&TimeIndexEntry> = None;
        for history_object in self.history_objects.iter() {
            let mut peekable_iter = history_object.back_iter.clone().peekable();
            if let Some(&tmp_time) = peekable_iter.peek(){
                if latest_time.is_none() || tmp_time.t() > latest_time?.t() {
                    latest_time = Some(tmp_time)
                }
            }
        }
        latest_time.map(|t| RaphtoryTime::apply(t.clone()))
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
    let gandalf_node_history = gandalf_node.graph.node_history(gandalf_node.node).collect::<Vec<_>>();
    let mut gandalf_node_history_back = gandalf_node.graph.node_history(gandalf_node.node).collect::<Vec<_>>();
    gandalf_node_history_back.reverse();
    let gandalf_node_history_object = HistoryObject::new(gandalf_node_history.iter(), gandalf_node_history_back.iter());

    gandalf_node_history_object.print("Gandalf Node history: ");

    // create Harry node history object
    let harry_node_history = harry_node.graph.node_history(harry_node.node).collect::<Vec<_>>();
    let mut harry_node_history_back = harry_node.graph.node_history(harry_node.node).collect::<Vec<_>>();
    harry_node_history_back.reverse();
    let harry_node_history_object = HistoryObject::new(harry_node_history.iter(), harry_node_history_back.iter());

    harry_node_history_object.print("Harry Node history: ");

    // create edge history object
    let edge_history = edge.graph.edge_history(edge.edge, edge.graph.layer_ids()).collect::<Vec<_>>();
    let mut edge_history_back = edge.graph.edge_history(edge.edge, edge.graph.layer_ids()).collect::<Vec<_>>();
    edge_history_back.reverse();
    let edge_history_object = HistoryObject::new(edge_history.iter(), edge_history_back.iter());
    
    edge_history_object.print("Gandalf-Harry Edge history: ");
    
    // create Composite History Object
    let composite_history_object = CompositeHistory::new(
        vec![gandalf_node_history_object, harry_node_history_object, edge_history_object]);
    
    composite_history_object.print("Composite history: ");
    
    println!("Composite history's earliest time: {}", composite_history_object.earliest_time().unwrap().epoch());
    println!("Composite history's latest time: {}", composite_history_object.latest_time().unwrap().epoch());
    
    let nodes = graph.nodes();
    for node in nodes {
        
    }

    Ok(())
}
