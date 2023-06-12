use std::{hash::BuildHasherDefault, sync::Arc};

use dashmap::{mapref::entry::Entry, DashMap};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};

use crate::{
    core::{vertex::InputVertex, Direction, Prop},
    storage,
};

use super::{
    edge_store::EdgeStore,
    node_store::NodeStore,
    props::PropsMeta,
    timer::{MaxCounter, MinCounter, TimeCounterTrait},
    LocalID, EID, VID,
};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Serialize, Deserialize)]
pub struct TemporalGraph<const N: usize, L: lock_api::RawRwLock> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: Arc<storage::RawStorage<NodeStore<N>, L, N>>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: Arc<storage::RawStorage<EdgeStore<N>, L, N>>,

    //earliest time seen in this graph
    pub(crate) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(crate) latest_time: MaxCounter,

    // props meta data (mapping between strings and ids) TODO: this is a bottle neck
    pub(crate) props_meta: PropsMeta,
}

impl<const N: usize, L: lock_api::RawRwLock> TemporalGraph<N, L> {
    pub fn new() -> Self {
        Self {
            logical_to_physical: FxDashMap::default(),
            nodes: Arc::new(storage::RawStorage::new()),
            edges: Arc::new(storage::RawStorage::new()),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            props_meta: PropsMeta::new(),
        }
    }

    #[inline]
    fn update_time(&self, time: i64) {
        self.earliest_time.update(time);
        self.latest_time.update(time);
    }

    pub fn add_vertex<T: InputVertex>(&self, t: i64, v: T) {
        self.add_vertex_with_props(t, v, vec![])
    }

    pub fn add_vertex_with_props<T: InputVertex>(&self, t: i64, v: T, props: Vec<(String, Prop)>) {
        self.add_vertex_internal(t, v, props);
    }

    fn add_vertex_internal<T: InputVertex>(&self, t: i64, v: T, props: Vec<(String, Prop)>) -> VID {
        self.update_time(t);

        // update the logical to physical mapping if needed
        let v_id = *(self.logical_to_physical.entry(v.id()).or_insert_with(|| {
            let node_store = NodeStore::new(v.id(), t);
            self.nodes.push(node_store)
        }));

        // get the node and update the time index
        let mut node = self.nodes.entry_mut(v_id);
        node.update_time(t);

        // update the properties;
        for (prop_id, prop) in self.props_meta.resolve_prop_ids(props) {
            node.add_prop(t, prop_id, prop);
        }

        // update name
        if let Some(n) = v.name_prop() {
            for (prop_id, prop) in self
                .props_meta
                .resolve_prop_ids(vec![("_id".to_string(), n)])
            {
                node.add_prop(t, prop_id, prop);
            }
        }
        v_id.into()
    }

    pub(crate) fn add_edge_with_props<T: InputVertex>(
        &mut self,
        t: i64,
        src: T,
        dst: T,
        props: Vec<(String, Prop)>,
        layer: String,
    ) {
        let src_id = self.add_vertex_internal(t, src, vec![]);
        let dst_id = self.add_vertex_internal(t, dst, vec![]);

        // get the entries for the src and dst nodes
        let mut node_pair = self.nodes.pair_entry_mut(src_id.into(), dst_id.into());

        let src = node_pair.get_mut_i(); // first

        // find the edge_id if it exists and add the time event to the nodes
        if let Some(edge_id) = src.find_edge(dst_id) {
            src.add_edge(t, dst_id, Direction::OUT, &layer, edge_id);
            // add inbound edge for dst
            let dst = node_pair.get_mut_j(); // second
            dst.add_edge(t, src_id, Direction::IN, &layer, edge_id);
        } else {
            let mut edge = EdgeStore::new(src_id, dst_id, t);
            for (prop_id, prop) in self.props_meta.resolve_prop_ids(props) {
                edge.add_prop(t, prop_id, prop);
            }
            self.edges.push(edge);
        }

    }
}

#[cfg(test)]
mod test {}
