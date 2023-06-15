use std::{
    hash::BuildHasherDefault,
    ops::Range,
    sync::Arc,
};

use dashmap::DashMap;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};

use crate::{
    core::{vertex::InputVertex, Direction, Prop},
    storage,
};

use super::{
    edge_store::EdgeStore,
    node_store::NodeStore,
    props::Meta,
    timer::{MaxCounter, MinCounter, TimeCounterTrait},
    VID, vertex::Vertex,
};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Serialize, Deserialize, Debug)]
pub struct TGraph<const N: usize, L: lock_api::RawRwLock> {
    pub(crate) inner: Arc<InnerTemporalGraph<N, L>>,
}

impl<const N: usize, L: lock_api::RawRwLock> Clone for TGraph<N, L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct InnerTemporalGraph<const N: usize, L: lock_api::RawRwLock> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: storage::RawStorage<NodeStore<N>, L, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: storage::RawStorage<EdgeStore<N>, L, N>,

    //earliest time seen in this graph
    pub(crate) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(crate) latest_time: MaxCounter,

    // props meta data (mapping between strings and ids) TODO: this is a bottle neck
    pub(crate) props_meta: Meta,
}

impl<const N: usize> TGraph<N, parking_lot::RawRwLock> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerTemporalGraph {
                logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
                nodes: storage::RawStorage::new(),
                edges: storage::RawStorage::new(),
                earliest_time: MinCounter::new(),
                latest_time: MaxCounter::new(),
                props_meta: Meta::new(),
            }),
        }
    }
}

impl<const N: usize, L: lock_api::RawRwLock> TGraph<N, L> {
    #[inline]
    fn update_time(&self, time: i64) {
        self.inner.earliest_time.update(time);
        self.inner.latest_time.update(time);
    }

    pub fn add_vertex<T: InputVertex>(&self, t: i64, v: T) {
        self.add_vertex_with_props(t, v, vec![])
    }

    pub fn add_vertex_with_props<T: InputVertex>(&self, t: i64, v: T, props: Vec<(String, Prop)>) {
        self.add_vertex_internal(t, v, props);
    }

    fn add_vertex_internal<T: InputVertex>(&self, t: i64, v: T, props: Vec<(String, Prop)>) -> VID {
        self.update_time(t);

        // resolve the props without holding any locks
        let props = self
            .inner
            .props_meta
            .resolve_prop_ids(props)
            .collect::<Vec<_>>();
        let node_prop = v
            .name_prop()
            .map(|n| {
                self.inner
                    .props_meta
                    .resolve_prop_ids(vec![("_id".to_string(), n)])
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![]);

        // update the logical to physical mapping if needed
        let v_id = *(self
            .inner
            .logical_to_physical
            .entry(v.id())
            .or_insert_with(|| {
                let node_store = NodeStore::new(v.id(), t);
                self.inner.nodes.push(node_store)
            }));

        // get the node and update the time index
        let mut node = self.inner.nodes.entry_mut(v_id);
        node.update_time(t);

        // update the properties;
        for (prop_id, prop) in &props {
            node.add_prop(t, *prop_id, prop.clone());
        }

        // update node prop
        for (prop_id, prop) in &node_prop {
            node.add_prop(t, *prop_id, prop.clone());
        }

        v_id.into()
    }

    pub fn add_edge<T: InputVertex>(&self, t: i64, src: T, dst: T, layer: &str) {
        self.add_edge_with_props(t, src, dst, vec![], layer)
    }

    pub fn add_edge_with_props<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: Vec<(String, Prop)>,
        layer: &str,
    ) {
        let src_id = self.add_vertex_internal(t, src, vec![]);
        let dst_id = self.add_vertex_internal(t, dst, vec![]);

        let layer = self
            .inner
            .props_meta
            .get_or_create_layer_id(layer.to_owned());
        let props = self
            .inner
            .props_meta
            .resolve_prop_ids(props)
            .collect::<Vec<_>>();

        // get the entries for the src and dst nodes
        let edge_id = {
            let mut node_pair = self
                .inner
                .nodes
                .pair_entry_mut(src_id.into(), dst_id.into());

            let src = node_pair.get_mut_i();

            // find the edge_id if it exists and add the time event to the nodes
            if let Some(edge_id) = src.find_edge(dst_id) {
                src.add_edge(dst_id, Direction::OUT, layer, edge_id);
                // add inbound edge for dst
                let dst = node_pair.get_mut_j();
                dst.add_edge(src_id, Direction::IN, layer, edge_id);
                Some(edge_id)
            } else {
                let mut edge = EdgeStore::new(src_id, dst_id, t);
                for (prop_id, prop) in &props {
                    edge.add_prop(t, *prop_id, prop.clone());
                }
                let edge_id = self.inner.edges.push(edge);

                // add the edge to the nodes
                src.add_edge(dst_id, Direction::OUT, layer, edge_id.into());
                let dst = node_pair.get_mut_j(); // second
                dst.add_edge(src_id, Direction::IN, layer, edge_id.into());
                None
            }
        }; // this is to release the node locks as quick as possible

        if let Some(e_id) = edge_id {
            // update the edge with properties
            let mut edge = self.inner.edges.entry_mut(e_id.into());
            for (prop_id, prop) in &props {
                edge.add_prop(t, *prop_id, prop.clone());
            }
            // update the time event
            edge.update_time(t);
        }
    }

    pub fn has_vertex<V: InputVertex>(&self, v: V) -> bool {
        self.inner.logical_to_physical.contains_key(&v.id())
    }

    pub fn has_vertex_window<V: InputVertex>(&self, v: V, window: Range<i64>) -> bool {
        let v_id = if let Some(v_id) = self.inner.logical_to_physical.get(&v.id()) {
            *v_id
        } else {
            return false;
        };

        let node = self.inner.nodes.entry(v_id);
        if let Some(n) = node.value() {
            n.has_time_window(window)
        } else {
            false
        }
    }

    pub fn vertex_ids(&self) -> impl Iterator<Item = VID> {
        (0..self.inner.nodes.len()).map(|i| i.into())
    }

    pub fn vertices<'a>(&'a self) -> impl Iterator<Item = Vertex<'a, N, L>> {
        self.inner
            .nodes
            .iter2()
            .map(move |node| Vertex::from_ref(node, self))
    }

    pub fn find_global_id(&self, v: VID) -> Option<u64> {
        let node = self.inner.nodes.entry(v.into());
        node.value().map(|n| n.global_id())
    }

    pub(crate) fn vertex<'a>(&'a self, v: VID) -> Vertex<'a, N, L> {
        let node = self.inner.nodes.entry(v.into());
        Vertex::from_entry( node, self,)
    }
}





#[cfg(test)]
mod test {

    use itertools::Itertools;

    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let g: TGraph<4, parking_lot::RawRwLock> = TGraph::new();

        g.add_vertex(1, 9);

        assert!(g.has_vertex(9u64));
        assert!(g.has_vertex_window(9u64, 1..15));
        assert!(!g.has_vertex_window(9u64, 10..15));

        assert_eq!(
            g.vertex_ids()
                .flat_map(|v| g.find_global_id(v))
                .collect::<Vec<_>>(),
            vec![9]
        );
    }

    #[test]
    fn add_vertices_with_1_property() {
        let g: TGraph<4, parking_lot::RawRwLock> = TGraph::new();

        let v_id = 1;
        let ts = 1;
        g.add_vertex_with_props(ts, v_id, vec![("type".into(), Prop::Str("wallet".into()))]);

        assert!(g.has_vertex(v_id));
        assert!(g.has_vertex_window(v_id, 1..15));
        assert_eq!(
            g.vertex_ids()
                .flat_map(|v| g.find_global_id(v))
                .collect::<Vec<_>>(),
            vec![v_id]
        );

        let res = g
            .vertices()
            .flat_map(|v| v.temporal_properties("type").collect_vec())
            .collect_vec();

        assert_eq!(res, vec![(1i64, Prop::Str("wallet".into()))]);
    }

    #[test]
    fn add_edge_at_t1() {
        let g: TGraph<4, parking_lot::RawRwLock> = TGraph::new();

        let src = 1;
        let dst = 2;
        let ts = 1;
        g.add_vertex(ts, src);
        g.add_vertex(ts, dst);

        g.add_edge(ts, src, dst, "follows");

        let res = g
            .vertices()
            .flat_map(|v| {
                v.edges("follows", Direction::OUT)
                    .map(|e| (e.src_id(), e.dst_id()))
            })
            .collect_vec();

        assert_eq!(res, vec![(0.into(), 1.into())]);
    }

    #[test]
    fn triangle_counts_by_iterators() {
        let g: TGraph<4, parking_lot::RawRwLock> = TGraph::new();

        let ts = 1;

        g.add_edge(ts, 1, 2, "follows");
        g.add_edge(ts, 2, 3, "follows");
        g.add_edge(ts, 3, 1, "follows");

        let res = g
            .vertices()
            .flat_map(|v| {
                let v_id = v.id();
                v.edges("follows", Direction::OUT)
                    .flat_map(move |edge_1| {
                        edge_1
                            .dst()
                            .edges("follows", Direction::OUT)
                            .flat_map(move |edge_2| {
                                edge_2
                                    .dst()
                                    .edges("follows", Direction::OUT)
                                    .filter(move |e| e.dst_id() == v_id)
                                    .map(move |edge_3| {
                                        (v_id, edge_2.src_id(), edge_2.dst_id(), edge_3.dst_id())
                                    })
                            })
                    })
            })
            .collect_vec();

        assert_eq!(
            res,
            vec![
                (0.into(), 1.into(), 2.into(), 0.into()),
                (1.into(), 2.into(), 0.into(), 1.into()),
                (2.into(), 0.into(), 1.into(), 2.into()),
            ]
        );
    }
}
