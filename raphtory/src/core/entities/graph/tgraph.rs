use crate::core::{
    entities::{
        edges::{
            edge::EdgeView,
            edge_store::{EdgeLayer, EdgeStore},
        },
        graph::{
            tgraph_storage::{GraphStorage, LockedIter},
            timer::{MaxCounter, MinCounter, TimeCounterTrait},
        },
        properties::{graph_props::GraphProps, props::Meta, tprop::TProp},
        vertices::{
            input_vertex::InputVertex,
            vertex::{ArcEdge, ArcVertex, Vertex},
            vertex_ref::VertexRef,
            vertex_store::VertexStore,
        },
        EID, VID,
    },
    storage::{locked_view::LockedView, timeindex::TimeIndexOps, Entry},
    utils::{
        errors::{GraphError, MutateGraphError},
        time::TryIntoTime,
    },
    Direction, Prop, PropUnwrap,
};
use dashmap::DashMap;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::BuildHasherDefault, ops::Deref, path::Path, sync::Arc};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

pub(crate) type TGraph<const N: usize> = InnerTemporalGraph<N>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InnerTemporalGraph<const N: usize>(Arc<TemporalGraph<N>>);

impl<const N: usize> Deref for InnerTemporalGraph<N> {
    type Target = TemporalGraph<N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph<const N: usize> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    storage: GraphStorage<N>,

    //earliest time seen in this graph
    pub(in crate::core) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(in crate::core) latest_time: MaxCounter,

    // props meta data for vertices (mapping between strings and ids)
    pub(in crate::core) vertex_meta: Arc<Meta>,

    // props meta data for edges (mapping between strings and ids)
    pub(in crate::core) edge_meta: Arc<Meta>,

    // graph properties
    pub(in crate::core) graph_props: GraphProps,
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

impl<const N: usize> Default for InnerTemporalGraph<N> {
    fn default() -> Self {
        let tg = TemporalGraph {
            logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
            storage: GraphStorage::new(),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            vertex_meta: Arc::new(Meta::new()),
            edge_meta: Arc::new(Meta::new()),
            graph_props: GraphProps::new(),
        };

        Self(Arc::new(tg))
    }
}

impl<const N: usize> InnerTemporalGraph<N> {
    pub(crate) fn get_all_layers(&self) -> Vec<usize> {
        self.edge_meta.get_all_layers()
    }

    pub(crate) fn layer_id(&self, key: Option<&str>) -> Option<usize> {
        match key {
            Some(key) => self.edge_meta.get_layer_id(key),
            None => Some(0),
        }
    }

    pub(crate) fn get_layer_name(&self, layer: usize) -> String {
        self.edge_meta
            .get_layer_name_by_id(layer)
            .unwrap_or_else(|| panic!("layer id '{layer}' doesn't exist"))
            .to_string()
    }

    pub(crate) fn graph_earliest_time(&self) -> Option<i64> {
        Some(self.earliest_time.get()).filter(|t| *t != i64::MAX)
    }

    pub(crate) fn graph_latest_time(&self) -> Option<i64> {
        Some(self.latest_time.get()).filter(|t| *t != i64::MIN)
    }

    pub(crate) fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        bincode::deserialize_from(&mut reader)
    }

    pub(crate) fn save_to_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let f = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(f);
        bincode::serialize_into(&mut writer, self)
    }

    pub(crate) fn global_vertex_id(&self, v: VID) -> Option<u64> {
        let node = self.storage.get_node(v.into());
        node.value().map(|n| n.global_id())
    }

    pub(crate) fn vertex_name(&self, v: VID) -> String {
        let node = self.storage.get_node(v.into());
        let name_prop_id = self.vertex_meta.resolve_prop_id("_id", true);
        let prop = node.static_property(name_prop_id);
        prop.cloned()
            .into_str()
            .unwrap_or_else(|| node.global_id().to_string())
    }

    pub(crate) fn node_entry(&self, v: VID) -> Entry<'_, VertexStore<N>, N> {
        self.storage.get_node(v.into())
    }

    pub(crate) fn edge_entry(&self, e: EID) -> Entry<'_, EdgeStore<N>, N> {
        self.storage.get_edge(e.into())
    }

    pub(crate) fn vertex_reverse_prop_id(
        &self,
        prop_id: usize,
        is_static: bool,
    ) -> Option<impl Deref<Target = std::string::String> + Debug + '_> {
        self.vertex_meta.reverse_prop_id(prop_id, is_static)
    }

    pub(crate) fn edge_temp_prop_ids(&self, e: EID) -> Vec<usize> {
        let edge = self.storage.get_edge(e.into());
        edge.temp_prop_ids(None)
    }

    pub(crate) fn vertex_temp_prop_ids(&self, e: VID) -> Vec<usize> {
        let edge = self.storage.get_node(e.into());
        edge.temp_prop_ids()
    }

    pub(crate) fn edge_find_prop(&self, prop: &str, is_static: bool) -> Option<usize> {
        self.edge_meta.find_prop_id(prop, is_static)
    }

    pub(crate) fn vertex_find_prop(&self, prop: &str, is_static: bool) -> Option<usize> {
        self.vertex_meta.find_prop_id(prop, is_static)
    }

    pub(crate) fn edge_reverse_prop_id(
        &self,
        prop_id: usize,
        is_static: bool,
    ) -> Option<impl Deref<Target = std::string::String> + Debug + '_> {
        let out = self.edge_meta.reverse_prop_id(prop_id, is_static);
        if out.is_none() {
            println!("reverse_prop_id_map: {:?}", self.edge_meta);
            println!("reverse_prop_id: {} -> {:?}", prop_id, out);
        }
        out
    }
}

impl<const N: usize> InnerTemporalGraph<N> {
    pub(crate) fn internal_num_vertices(&self) -> usize {
        self.storage.nodes_len()
    }

    pub(crate) fn internal_num_edges(&self, layer: Option<usize>) -> usize {
        self.storage.edges_len(layer)
    }

    pub(crate) fn degree(&self, v: VID, dir: Direction, layer: Option<usize>) -> usize {
        let node_store = self.storage.get_node(v.into());
        node_store.neighbours(layer, dir).count()
    }

    #[inline]
    fn update_time(&self, time: i64) {
        self.earliest_time.update(time);
        self.latest_time.update(time);
    }

    pub(crate) fn add_vertex_internal(
        &self,
        time: i64,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        let t = time.try_into_time()?;
        self.update_time(t);

        // resolve the props without holding any locks
        let props = self
            .vertex_meta
            .resolve_prop_ids(props, false)
            .collect::<Vec<_>>();

        let name_prop = name
            .or_else(|| v.id_str())
            .map(|n| {
                self.vertex_meta
                    .resolve_prop_ids(vec![("_id".to_string(), Prop::Str(n.to_owned()))], true)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![]);

        // update the logical to physical mapping if needed
        let v_id = *(self.logical_to_physical.entry(v.id()).or_insert_with(|| {
            let node_store = VertexStore::new(v.id(), t);
            self.storage.push_node(node_store)
        }));

        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v_id);
        node.update_time(t);

        // update the properties;
        for (prop_id, _, prop) in &props {
            node.add_prop(t, *prop_id, prop.clone());
        }

        // update node prop
        for (prop_id, name, prop) in &name_prop {
            node.add_static_prop(*prop_id, name, prop.clone())?;
        }

        Ok(())
    }

    pub(crate) fn add_vertex_no_props(&self, t: i64, v: u64) -> Result<VID, GraphError> {
        self.update_time(t);

        // update the logical to physical mapping if needed
        let v_id = *(self.logical_to_physical.entry(v.id()).or_insert_with(|| {
            let node_store = VertexStore::new(v.id(), t);
            self.storage.push_node(node_store)
        }));

        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v_id);
        node.update_time(t);

        Ok(v_id.into())
    }

    pub(crate) fn add_vertex_properties_internal(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        if let Some(vid) = self.logical_to_physical.get(&v).map(|entry| *entry) {
            let mut node = self.storage.get_node_mut(vid);
            for (prop_name, prop) in data {
                let prop_id = self.vertex_meta.resolve_prop_id(&prop_name, true);
                node.add_static_prop(prop_id, &prop_name, prop.clone())
                    .map_err(|err| GraphError::FailedToMutateGraph { source: err })?;
            }
        }
        Ok(())
    }

    pub(crate) fn add_edge_properties_internal(
        &self,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let src_id = self
            .logical_to_physical
            .get(&src)
            .map(|entry| *entry)
            .ok_or(GraphError::FailedToMutateGraph {
                source: MutateGraphError::VertexNotFoundError { vertex_id: src },
            })?;

        let dst_id = self
            .logical_to_physical
            .get(&dst)
            .map(|entry| *entry)
            .ok_or(GraphError::FailedToMutateGraph {
                source: MutateGraphError::VertexNotFoundError { vertex_id: dst },
            })?;

        let layer_id = layer
            .map(|name| {
                self.edge_meta
                    .get_layer_id(name)
                    .ok_or(GraphError::FailedToMutateGraph {
                        source: MutateGraphError::LayerNotFoundError {
                            layer_name: name.to_string(),
                        },
                    })
            })
            .unwrap_or(Ok(0))?;

        let edge_id = self
            .storage
            .get_node(src_id)
            .find_edge(dst_id.into(), Some(layer_id))
            .ok_or(GraphError::FailedToMutateGraph {
                source: MutateGraphError::MissingEdge(src.id(), dst.id()),
            })?;

        let mut edge = self.storage.get_edge_mut(edge_id.into());

        let props = self
            .edge_meta
            .resolve_prop_ids(props.clone(), true)
            .collect::<Vec<_>>();

        let mut layer = edge.layer_mut(layer_id);
        for (prop_id, prop_name, prop) in props {
            layer
                .add_static_prop(prop_id, &prop_name, prop)
                .expect("you should not fail!");
        }
        Ok(())
    }

    pub(crate) fn add_static_property(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        for (name, prop) in props {
            self.graph_props.add_static_prop(&name, prop.clone());
        }
        Ok(())
    }

    pub(crate) fn add_property(
        &self,
        t: i64,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        for (name, prop) in props {
            self.graph_props.add_prop(t, &name, prop.clone());
        }
        Ok(())
    }

    pub(crate) fn get_static_prop(&self, name: &str) -> Option<Prop> {
        self.graph_props.get_static(name)
    }

    pub(crate) fn get_temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.graph_props.get_temporal(name)
    }

    pub(crate) fn static_property_names(
        &self,
    ) -> impl Deref<Target = Vec<std::string::String>> + '_ {
        self.graph_props.static_prop_names()
    }

    pub(crate) fn temporal_property_names(
        &self,
    ) -> impl Deref<Target = Vec<std::string::String>> + '_ {
        self.graph_props.temporal_prop_names()
    }

    pub(crate) fn delete_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.update_time(t);

        let src_id = self.add_vertex_no_props(t, src)?;
        let dst_id = self.add_vertex_no_props(t, dst)?;

        let layer = self.get_or_allocate_layer(layer);

        if let Some(e_id) = self.find_edge(src_id, dst_id, None) {
            let mut edge = self.storage.get_edge_mut(e_id.into());
            edge.layer_mut(layer).delete(t);
        } else {
            self.link_nodes(src_id, dst_id, t, layer, &vec![], |edge_layer| {
                edge_layer.delete(t)
            });
        }

        Ok(())
    }

    fn get_or_allocate_layer(&self, layer: Option<&str>) -> usize {
        layer
            .map(|layer| self.edge_meta.get_or_create_layer_id(layer.to_owned()))
            .unwrap_or(0)
    }

    fn link_nodes<F: Fn(&mut EdgeLayer)>(
        &self,
        src_id: VID,
        dst_id: VID,
        t: i64,
        layer: usize,
        props: &Vec<(usize, String, Prop)>,
        new_edge_fn: F,
    ) -> Option<EID> {
        let mut node_pair = self.storage.pair_node_mut(src_id.into(), dst_id.into());

        let src = node_pair.get_mut_i();

        // find the edge_id if it exists and add the time event to the nodes
        if let Some(edge_id) = src.find_edge(dst_id, None) {
            src.add_edge(dst_id, Direction::OUT, layer, edge_id);
            // add inbound edge for dst
            let dst = node_pair.get_mut_j();
            dst.add_edge(src_id, Direction::IN, layer, edge_id);
            Some(edge_id)
        } else {
            let mut edge = EdgeStore::new(src_id, dst_id);
            {
                let mut edge_layer = edge.layer_mut(layer);
                new_edge_fn(&mut edge_layer);
                for (prop_id, _, prop) in props {
                    edge_layer.add_prop(t, *prop_id, prop.clone());
                }
            }
            let edge_id = self.storage.push_edge(edge);

            // add the edge to the nodes
            src.add_edge(dst_id, Direction::OUT, layer, edge_id.into());
            let dst = node_pair.get_mut_j(); // second
            dst.add_edge(src_id, Direction::IN, layer, edge_id.into());
            None
        }
    }

    pub(crate) fn add_edge_internal(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let t = t.try_into_time()?;
        let src_id = self.add_vertex_no_props(t, src)?;
        let dst_id = self.add_vertex_no_props(t, dst)?;

        let layer = self.get_or_allocate_layer(layer);

        let props = self
            .edge_meta
            .resolve_prop_ids(props.clone(), false)
            .collect::<Vec<_>>();

        // get the entries for the src and dst nodes
        let edge_id = self.link_nodes(src_id, dst_id, t, layer, &props, |edge_layer| {
            edge_layer.update_time(t)
        }); // this is to release the node locks as quick as possible

        if let Some(e_id) = edge_id {
            // update the edge with properties
            let mut edge = self.storage.get_edge_mut(e_id.into());
            let mut edge_layer = edge.layer_mut(layer);
            for (prop_id, _, prop) in &props {
                edge_layer.add_prop(t, *prop_id, prop.clone());
            }
            // update the time event
            edge_layer.update_time(t);
        }
        Ok(())
    }

    pub(crate) fn vertex_ids(&self) -> impl Iterator<Item = VID> {
        (0..self.storage.nodes_len()).map(|i| i.into())
    }

    pub(crate) fn locked_edges(&self) -> LockedIter<N, EdgeStore<N>> {
        self.storage.locked_edges()
    }

    pub(crate) fn vertex<'a>(&'a self, v: VID) -> Vertex<'a, N> {
        let node = self.storage.get_node(v.into());
        Vertex::from_entry(node, self)
    }

    pub(crate) fn vertex_arc(&self, v: VID) -> ArcVertex<N> {
        let node = self.storage.get_node_arc(v.into());
        ArcVertex::from_entry(node, self.vertex_meta.clone())
    }

    pub(crate) fn edge_arc(&self, e: EID) -> ArcEdge<N> {
        let edge = self.storage.get_edge_arc(e.into());
        ArcEdge::from_entry(edge, self.edge_meta.clone())
    }

    pub(crate) fn edge<'a>(&'a self, e: EID) -> EdgeView<'a, N> {
        let edge = self.storage.get_edge(e.into());
        EdgeView::from_entry(edge, self)
    }

    pub(crate) fn find_edge(&self, src: VID, dst: VID, layer_id: Option<usize>) -> Option<EID> {
        let node = self.storage.get_node(src.into());
        node.find_edge(dst, layer_id)
    }

    pub(crate) fn resolve_vertex_ref(&self, v: &VertexRef) -> Option<VID> {
        match v {
            VertexRef::Local(vid) => Some(*vid),
            VertexRef::Remote(gid) => {
                let v_id = self.logical_to_physical.get(gid)?;
                Some((*v_id).into())
            }
        }
    }

    pub(crate) fn prop_vec_window(
        &self,
        e: EID,
        name: &str,
        t_start: i64,
        t_end: i64,
        layer: usize,
    ) -> Vec<(i64, Prop)> {
        let edge = self.storage.get_edge(e.into());
        if !edge.unsafe_layer(layer).additions().active(t_start..t_end) {
            vec![]
        } else {
            let prop_id = self.edge_meta.resolve_prop_id(name, false);

            edge.props(None)
                .flat_map(|props| props.temporal_props_window(prop_id, t_start, t_end))
                .collect()
        }
    }
}
