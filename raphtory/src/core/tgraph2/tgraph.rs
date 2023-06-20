use std::{borrow::Borrow, fmt::Debug, hash::BuildHasherDefault, path::Path};

use dashmap::DashMap;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};

use crate::core::{
    tgraph_shard::errors::GraphError,
    time::TryIntoTime,
    vertex::InputVertex,
    vertex_ref::{LocalVertexRef, VertexRef},
    Direction, Prop, PropUnwrap,
};

use super::{
    edge_store::EdgeStore,
    node_store::NodeStore,
    props::Meta,
    tgraph_storage::GraphStorage,
    timer::{MaxCounter, MinCounter, TimeCounterTrait},
    vertex::{Vertex, ArcVertex},
    EdgeRef, EID, VID, edge::EdgeView,
};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

// #[derive(Serialize, Deserialize, Debug, Default)]
// pub struct TGraph<const N: usize> {
//     pub(crate) inner: Arc<InnerTemporalGraph<N>>,
// }

// impl<const N: usize> Clone for TGraph<N> {
//     fn clone(&self) -> Self {
//         Self {
//             inner: self.inner.clone(),
//         }
//     }
// }

pub(crate) type TGraph<const N: usize> = InnerTemporalGraph<N>;

#[derive(Serialize, Deserialize, Debug)]
pub struct InnerTemporalGraph<const N: usize> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    storage: GraphStorage<N>,

    //earliest time seen in this graph
    pub(crate) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(crate) latest_time: MaxCounter,

    // props meta data (mapping between strings and ids) TODO: this is a bottle neck
    pub(crate) props_meta: Meta,
}

impl<const N: usize> std::fmt::Display for InnerTemporalGraph<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_vertices={}, num_edges={})",
            self.storage.nodes_len(),
            self.storage.edges_len(None)
        )
    }
}

impl<const N: usize> PartialEq for InnerTemporalGraph<N> {
    fn eq(&self, other: &Self) -> bool {
        self.storage == other.storage
    }
}

impl<const N: usize> Default for InnerTemporalGraph<N> {
    fn default() -> Self {
        Self {
            logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
            storage: GraphStorage::new(),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            props_meta: Meta::new(),
        }
    }
}

impl<const N: usize> InnerTemporalGraph<N> {
    pub(crate) fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        bincode::deserialize_from(&mut reader)
    }

    pub(crate) fn global_vertex_id(&self, v: VID) -> Option<u64> {
        let node = self.storage.get_node(v.into());
        node.value().map(|n| n.global_id())
    }

    pub(crate) fn vertex_name(&self, v: VID) -> String {
        let node = self.storage.get_node(v.into());
        let name_prop_id = self.props_meta.resolve_prop_id("_id", true);
        let prop = node.static_property(name_prop_id);
        prop.cloned()
            .into_str()
            .unwrap_or_else(|| node.global_id().to_string())
    }
}

impl<const N: usize> InnerTemporalGraph<N> {
    pub fn get_layer_id<B: Borrow<str>>(&self, name: B) -> Option<usize> {
        self.props_meta.get_layer_id(name.borrow())
    }

    pub fn num_vertices(&self) -> usize {
        self.storage.nodes_len()
    }

    pub fn num_edges(&self, layer: Option<usize>) -> usize {
        self.storage.edges_len(layer)
    }

    pub fn has_edge_ref(&self, src: VID, dst: VID, layer_id: usize) -> bool {
        let node_store = self.storage.get_node(src.into());
        node_store.find_edge_on_layer(dst, layer_id).is_some()
    }

    pub fn has_vertex_ref(&self, id: VID) -> bool {
        self.storage.get_node(id.into()).value().is_some()
    }

    pub fn degree(&self, id: VID, layer_id: usize, dir: Direction) -> usize {
        let node_store = self.storage.get_node(id.into());
        node_store.edge_tuples(layer_id, dir).count()
    }

    pub fn edges<'a>(
        &'a self,
        id: VID,
        layer_id: &str,
        dir: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + 'a> {
        Box::new(
            self.vertex(id)
                .edges(layer_id, dir)
                .map(|e| EdgeRef::new(e.src_id(), e.dst_id(), e.edge_id(), 0, None)),
        )
    }

    #[inline]
    fn update_time(&self, time: i64) {
        self.earliest_time.update(time);
        self.latest_time.update(time);
    }

    pub fn add_vertex<T: InputVertex>(
        &self,
        t: i64,
        v: T,
        props: &Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        self.add_vertex_internal(t, v, props.clone())
    }

    fn add_vertex_internal<V: InputVertex, T: TryIntoTime + Debug>(
        &self,
        time: T,
        v: V,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        let t = time.try_into_time()?;
        self.update_time(t);

        // resolve the props without holding any locks
        let props = self.props_meta.resolve_prop_ids(props).collect::<Vec<_>>();
        let node_prop = v
            .name_prop()
            .map(|n| {
                self.props_meta
                    .resolve_prop_ids(vec![("_id".to_string(), n)])
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![]);

        // update the logical to physical mapping if needed
        let v_id = *(self.logical_to_physical.entry(v.id()).or_insert_with(|| {
            let node_store = NodeStore::new(v.id(), t);
            self.storage.push_node(node_store)
        }));

        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v_id);
        node.update_time(t);

        // update the properties;
        for (prop_id, prop) in &props {
            node.add_prop(t, *prop_id, prop.clone());
        }

        // update node prop
        for (prop_id, prop) in &node_prop {
            node.add_prop(t, *prop_id, prop.clone());
        }

        Ok(v_id.into())
    }

    pub fn add_edge<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.add_edge_with_props(t, src, dst, props.clone(), layer)
    }

    pub fn delete_edge<V: InputVertex, T: TryIntoTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        todo!()
    }

    pub fn add_vertex_properties<V: InputVertex>(
        &self,
        v: V,
        data: &Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        todo!()
    }

    pub fn add_edge_properties<V: InputVertex>(
        &self,
        src: V,
        dst: V,
        props: &Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        todo!()
    }

    pub fn add_static_property(&self, props: &Vec<(String, Prop)>) -> Result<(), GraphError> {
        todo!()
    }

    pub fn add_edge_with_props<T: InputVertex>(
        &self,
        t: i64,
        src: T,
        dst: T,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let src_id = self.add_vertex_internal(t, src, vec![])?;
        let dst_id = self.add_vertex_internal(t, dst, vec![])?;

        let layer = layer
            .map(|layer| self.props_meta.get_or_create_layer_id(layer.to_owned()))
            .unwrap_or(0);

        let props = self.props_meta.resolve_prop_ids(props).collect::<Vec<_>>();

        // get the entries for the src and dst nodes
        let edge_id = {
            let mut node_pair = self.storage.pair_node_mut(src_id.into(), dst_id.into());

            let src = node_pair.get_mut_i();

            // find the edge_id if it exists and add the time event to the nodes
            if let Some(edge_id) = src.find_edge(dst_id, Some(layer)) {
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
                let edge_id = self.storage.push_edge(edge);

                // add the edge to the nodes
                src.add_edge(dst_id, Direction::OUT, layer, edge_id.into());
                let dst = node_pair.get_mut_j(); // second
                dst.add_edge(src_id, Direction::IN, layer, edge_id.into());
                None
            }
        }; // this is to release the node locks as quick as possible

        if let Some(e_id) = edge_id {
            // update the edge with properties
            let mut edge = self.storage.get_edge_mut(e_id.into());
            for (prop_id, prop) in &props {
                edge.add_prop(t, *prop_id, prop.clone());
            }
            // update the time event
            edge.update_time(t);
        }
        Ok(())
    }

    pub fn has_vertex<V: InputVertex>(&self, v: V) -> bool {
        self.logical_to_physical.contains_key(&v.id())
    }

    // pub fn has_vertex_window<V: InputVertex>(&self, v: V, window: Range<i64>) -> bool {
    //     let v_id = if let Some(v_id) = self.logical_to_physical.get(&v.id()) {
    //         *v_id
    //     } else {
    //         return false;
    //     };

    //     let node = self.storage.get_node(v_id);
    //     if let Some(n) = node.value() {
    //         n.has_time_window(window)
    //     } else {
    //         false
    //     }
    // }

    pub fn vertex_ids(&self) -> impl Iterator<Item = VID> {
        (0..self.storage.nodes_len()).map(|i| i.into())
    }

    pub fn edge_ids(&self, layer: Option<usize>) -> impl Iterator<Item = EID> {
        (0..self.storage.edges_len(layer)).map(|i| i.into())
    }

    // pub fn vertex_active(&self, v: VID, t_start: i64, t_end: i64) -> bool {
    //     let node = self.storage.get_node(v.into());
    //     node.value()
    //         .map(|n| n.timestamps().active(t_start..t_end))
    //         .unwrap_or(false)
    // }

    // pub fn edge_active(&self, e: EID, t_start: i64, t_end: i64) -> bool {
    //     let edge = self.storage.get_edge(e.into());
    //     edge.value()
    //         .map(|e| e.timestamps().active(t_start..t_end))
    //         .unwrap_or(false)
    // }

    pub fn vertices<'a>(&'a self) -> impl Iterator<Item = Vertex<'a, N>> {
        self.storage
            .nodes()
            .map(move |node| Vertex::from_ref(node, self))
    }

    pub fn locked_vertices<'a>(&'a self) -> impl Iterator<Item = Vertex<'a, N>> {
        self.storage
            .locked_nodes()
            .map(move |node| Vertex::from_ge(node, self))
    }

    pub fn find_global_id(&self, v: VID) -> Option<u64> {
        let node = self.storage.get_node(v.into());
        node.value().map(|n| n.global_id())
    }

    pub(crate) fn vertex<'a>(&'a self, v: VID) -> Vertex<'a, N> {
        let node = self.storage.get_node(v.into());
        Vertex::from_entry(node, self)
    }

    pub(crate) fn vertex_arc(&self, v: VID) -> ArcVertex<N> {
        let node = self.storage.get_node_arc(v.into());
        ArcVertex::from_entry(node) 
    }

    pub(crate) fn edge<'a>(&'a self, e: EID) -> EdgeView<'a, N> {
        let edge = self.storage.get_edge(e.into());
        EdgeView::from_entry(edge, self)
    }

    pub(crate) fn find_edge(&self, src: VID, dst: VID, layer_id: usize) -> Option<EID> {
        let node = self.storage.get_node(src.into());
        node.find_edge(dst, Some(layer_id))
    }

    // pub(crate) fn find_edge<T: InputVertex>(&self, src: T, dst: T, layer: usize) -> Option<EID> {
    //     let src_id = self.logical_to_physical.get(&src.id())?;
    //     let dst_id = self.logical_to_physical.get(&dst.id())?;
    //     self.find_edge_inner((*src_id).into(), (*dst_id).into(), layer)
    // }

    pub(crate) fn resolve_vertex_ref(&self, v: &VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(LocalVertexRef { shard_id, pid }) => {
                Some((*pid * N + *shard_id).into())
            }
            VertexRef::Remote(gid) => {
                let v_id = self.logical_to_physical.get(gid)?;
                Some((*v_id).into())
            }
        }
    }

}

// #[cfg(test)]
// mod test {

//     use itertools::Itertools;

//     use crate::core::tgraph2::ops::vertex_ops::VertexListOps;

//     use super::*;

//     #[test]
//     fn add_vertex_at_time_t1() {
//         let g: TGraph<4> = TGraph::default();

//         g.add_vertex(1, 9, &vec![]);

//         assert!(g.has_vertex(9u64));
//         // assert!(g.has_vertex_window(9u64, 1..15));
//         // assert!(!g.has_vertex_window(9u64, 10..15));

//         assert_eq!(
//             g.vertex_ids()
//                 .flat_map(|v| g.find_global_id(v))
//                 .collect::<Vec<_>>(),
//             vec![9]
//         );
//     }

//     #[test]
//     fn add_vertices_with_1_property() {
//         let g: TGraph<4> = TGraph::default();

//         let v_id = 1;
//         let ts = 1;
//         g.add_vertex_with_props(ts, v_id, vec![("type".into(), Prop::Str("wallet".into()))]);

//         assert!(g.has_vertex(v_id));
//         // assert!(g.has_vertex_window(v_id, 1..15));
//         assert_eq!(
//             g.vertex_ids()
//                 .flat_map(|v| g.find_global_id(v))
//                 .collect::<Vec<_>>(),
//             vec![v_id]
//         );

//         let res = g
//             .vertices()
//             .flat_map(|v| v.temporal_properties("type").collect_vec())
//             .collect_vec();

//         assert_eq!(res, vec![(1i64, Prop::Str("wallet".into()))]);
//     }

//     #[test]
//     fn add_edge_at_t1() {
//         let g: TGraph<4> = TGraph::default();

//         let src = 1;
//         let dst = 2;
//         let ts = 1;
//         g.add_vertex(ts, src, &vec![]);
//         g.add_vertex(ts, dst, &vec![]);

//         g.add_edge(ts, src, dst, "follows");

//         let res = g
//             .vertices()
//             .flat_map(|v| {
//                 v.edges("follows", Direction::OUT)
//                     .map(|e| (e.src_id(), e.dst_id()))
//             })
//             .collect_vec();

//         assert_eq!(res, vec![(0.into(), 1.into())]);
//     }

//     #[test]
//     fn triangle_counts_by_iterators() {
//         let g: TGraph<4> = TGraph::default();

//         let ts = 1;

//         g.add_edge(ts, 1, 2, "follows");
//         g.add_edge(ts, 2, 3, "follows");
//         g.add_edge(ts, 3, 1, "follows");

//         let res = g
//             .vertices()
//             .flat_map(|v| {
//                 let v_id = v.id();
//                 v.edges("follows", Direction::OUT).flat_map(move |edge_1| {
//                     edge_1
//                         .dst()
//                         .edges("follows", Direction::OUT)
//                         .flat_map(move |edge_2| {
//                             edge_2
//                                 .dst()
//                                 .edges("follows", Direction::OUT)
//                                 .filter(move |e| e.dst_id() == v_id)
//                                 .map(move |edge_3| {
//                                     (v_id, edge_2.src_id(), edge_2.dst_id(), edge_3.dst_id())
//                                 })
//                         })
//                 })
//             })
//             .collect_vec();

//         assert_eq!(
//             res,
//             vec![
//                 (0.into(), 1.into(), 2.into(), 0.into()),
//                 (1.into(), 2.into(), 0.into(), 1.into()),
//                 (2.into(), 0.into(), 1.into(), 2.into()),
//             ]
//         );
//     }

//     #[test]
//     fn triangle_counts_by_locked_iterators() {
//         let g: TGraph<4> = TGraph::default();

//         let ts = 1;

//         g.add_edge(ts, 1, 2, "follows");
//         g.add_edge(ts, 2, 3, "follows");
//         g.add_edge(ts, 3, 1, "follows");

//         let res = g
//             .locked_vertices()
//             .flat_map(|v| {
//                 let v_id = v.id();
//                 v.edges("follows", Direction::OUT).flat_map(move |edge_1| {
//                     edge_1
//                         .dst()
//                         .edges("follows", Direction::OUT)
//                         .flat_map(move |edge_2| {
//                             edge_2
//                                 .dst()
//                                 .edges("follows", Direction::OUT)
//                                 .filter(move |e| e.dst_id() == v_id)
//                                 .map(move |edge_3| {
//                                     (v_id, edge_2.src_id(), edge_2.dst_id(), edge_3.dst_id())
//                                 })
//                         })
//                 })
//             })
//             .collect_vec();

//         assert_eq!(
//             res,
//             vec![
//                 (0.into(), 1.into(), 2.into(), 0.into()),
//                 (1.into(), 2.into(), 0.into(), 1.into()),
//                 (2.into(), 0.into(), 1.into(), 2.into()),
//             ]
//         );
//     }

//     #[test]
//     fn test_chaining_vertex_ops() {
//         let g: TGraph<4> = TGraph::default();

//         let ts = 1;

//         g.add_edge(ts, 1, 2, "follows");
//         g.add_edge(ts, 2, 3, "follows");
//         g.add_edge(ts, 3, 1, "follows");

//         let vertex = g.vertices();

//         // let hello = vertex.ids();
//         let hello_edges = vertex.neighbours().neighbours();

//         let v = g.vertex(1.into());
//         let what_are_you = v.ids();
//     }
// }
