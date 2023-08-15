use crate::{
    core::{
        entities::{
            edges::{
                edge::EdgeView,
                edge_ref::EdgeRef,
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
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{AsTime, LockedLayeredIndex, TimeIndexEntry, TimeIndexOps},
            Entry,
        },
        utils::{
            errors::{GraphError, IllegalMutate, MutateGraphError},
            time::TryIntoTime,
        },
        Direction, Prop, PropUnwrap,
    },
    db::api::view::Layer,
};
use dashmap::DashMap;
use parking_lot::RwLockReadGuard;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    hash::BuildHasherDefault,
    ops::Deref,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;

pub(crate) type TGraph<const N: usize> = TemporalGraph<N>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InnerTemporalGraph<const N: usize>(Arc<TemporalGraph<N>>);

impl<const N: usize> InnerTemporalGraph<N> {
    #[inline]
    pub(crate) fn inner(&self) -> &TemporalGraph<N> {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph<const N: usize> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, usize>,

    pub(crate) storage: GraphStorage<N>,

    pub(crate) event_counter: AtomicUsize,

    //earliest time seen in this graph
    pub(in crate::core) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(in crate::core) latest_time: MaxCounter,

    // props meta data for vertices (mapping between strings and ids)
    pub(in crate::core) vertex_meta: Arc<Meta>,

    // props meta data for edges (mapping between strings and ids)
    pub(in crate::core) edge_meta: Arc<Meta>,

    // graph properties
    pub(crate) graph_props: GraphProps,
}

impl<const N: usize> std::fmt::Display for InnerTemporalGraph<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_vertices={}, num_edges={})",
            self.inner().storage.nodes_len(),
            self.inner().storage.edges_len(LayerIds::All)
        )
    }
}

impl<const N: usize> Default for InnerTemporalGraph<N> {
    fn default() -> Self {
        let tg = TemporalGraph {
            logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
            storage: GraphStorage::new(),
            event_counter: AtomicUsize::new(0),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            vertex_meta: Arc::new(Meta::new()),
            edge_meta: Arc::new(Meta::new()),
            graph_props: GraphProps::new(),
        };

        Self(Arc::new(tg))
    }
}

impl<const N: usize> TemporalGraph<N> {
    pub(crate) fn num_layers(&self) -> usize {
        self.edge_meta.layer_meta().len()
    }

    pub(crate) fn layer_names(&self, layer_ids: LayerIds) -> Vec<String> {
        match layer_ids {
            LayerIds::None => {
                vec![]
            }
            LayerIds::All => self.edge_meta.layer_meta().get_keys().clone(),
            LayerIds::One(id) => {
                vec![self
                    .edge_meta
                    .layer_meta()
                    .reverse_lookup(id)
                    .unwrap()
                    .clone()]
            }
            LayerIds::Multiple(ids) => ids
                .iter()
                .map(|id| {
                    self.edge_meta
                        .layer_meta()
                        .reverse_lookup(*id)
                        .unwrap()
                        .clone()
                })
                .collect(),
        }
    }

    pub(crate) fn get_all_vertex_property_names(&self, is_static: bool) -> Vec<String> {
        self.vertex_meta.get_all_property_names(is_static)
    }

    pub(crate) fn get_all_edge_property_names(&self, is_static: bool) -> Vec<String> {
        self.edge_meta.get_all_property_names(is_static)
    }

    pub(crate) fn get_all_layers(&self) -> Vec<usize> {
        self.edge_meta.get_all_layers()
    }

    pub(crate) fn layer_id(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All => LayerIds::All,
            Layer::Default => LayerIds::One(0),
            Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => LayerIds::One(id),
                None => LayerIds::None,
            },
            Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .filter_map(|id| self.edge_meta.get_layer_id(id))
                    .collect::<Vec<_>>();
                let num_layers = self.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    LayerIds::None
                } else if num_new_layers == 1 {
                    LayerIds::One(new_layers[0])
                } else if num_new_layers == num_layers {
                    LayerIds::All
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    LayerIds::Multiple(new_layers.into())
                }
            }
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

    pub(crate) fn edge_refs(&self) -> impl Iterator<Item = EdgeRef> + Send {
        self.storage.edge_refs()
    }

    pub(crate) fn edge_entry(&self, e: EID) -> Entry<'_, EdgeStore<N>, N> {
        self.storage.get_edge(e.into())
    }

    pub(crate) fn vertex_reverse_prop_id(
        &self,
        prop_id: usize,
        is_static: bool,
    ) -> Option<LockedView<String>> {
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
    ) -> Option<LockedView<String>> {
        self.edge_meta.reverse_prop_id(prop_id, is_static)
    }
}

impl<const N: usize> TemporalGraph<N> {
    pub(crate) fn internal_num_vertices(&self) -> usize {
        self.storage.nodes_len()
    }

    pub(crate) fn internal_num_edges(&self, layers: LayerIds) -> usize {
        self.storage.edges_len(layers)
    }

    pub(crate) fn degree(&self, v: VID, dir: Direction, layers: LayerIds) -> usize {
        let node_store = self.storage.get_node(v.into());
        node_store.degree(layers, dir)
    }

    #[inline]
    fn update_time(&self, time: TimeIndexEntry) {
        let t = *time.t();
        self.earliest_time.update(t);
        self.latest_time.update(t);
    }

    pub(crate) fn add_vertex_internal(
        &self,
        time: TimeIndexEntry,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        self.update_time(time);

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
            let node_store = VertexStore::new(v.id(), time);
            self.storage.push_node(node_store)
        }));

        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v_id);
        node.update_time(time);

        // update the properties;
        for (prop_id, prop) in props {
            node.add_prop(time, prop_id, prop);
        }

        // update node prop
        for (prop_id, prop) in name_prop {
            node.add_static_prop(prop_id, prop).map_err(|err| {
                MutateGraphError::IllegalVertexPropertyChange {
                    vertex_id: v,
                    source: IllegalMutate::from_source(err, "_id"),
                }
            })?;
        }

        Ok(v_id.into())
    }

    pub(crate) fn add_vertex_no_props(&self, t: TimeIndexEntry, v: u64) -> Result<VID, GraphError> {
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
                node.add_static_prop(prop_id, prop).map_err(|err| {
                    GraphError::FailedToMutateGraph {
                        source: MutateGraphError::IllegalVertexPropertyChange {
                            vertex_id: v,
                            source: IllegalMutate::from_source(err, &prop_name),
                        },
                    }
                })?;
            }
            Ok(())
        } else {
            Err(GraphError::FailedToMutateGraph {
                source: MutateGraphError::VertexNotFoundError { vertex_id: v },
            })
        }
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
            .find_edge(dst_id.into(), layer_id.into())
            .ok_or(GraphError::FailedToMutateGraph {
                source: MutateGraphError::MissingEdge(src.id(), dst.id()),
            })?;

        let mut edge = self.storage.get_edge_mut(edge_id.into());

        let props = self
            .edge_meta
            .resolve_prop_ids(props, true)
            .collect::<Vec<_>>();

        let mut layer = edge.layer_mut(layer_id);
        for (prop_id, prop) in props {
            layer.add_static_prop(prop_id, prop).map_err(|err| {
                GraphError::FailedToMutateGraph {
                    source: MutateGraphError::IllegalEdgePropertyChange {
                        src_id: src,
                        dst_id: dst,
                        source: IllegalMutate {
                            name: self
                                .edge_meta
                                .reverse_prop_id(prop_id, true)
                                .expect("resolved prop ids exist")
                                .clone(),
                            source: err,
                        },
                    },
                }
            })?;
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
        t: TimeIndexEntry,
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

    pub(crate) fn static_property_names(&self) -> RwLockReadGuard<Vec<std::string::String>> {
        self.graph_props.static_prop_names()
    }

    pub(crate) fn temporal_property_names(&self) -> RwLockReadGuard<Vec<std::string::String>> {
        self.graph_props.temporal_prop_names()
    }

    pub(crate) fn delete_edge(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.update_time(t);

        let src_id = self.add_vertex_no_props(t, src)?;
        let dst_id = self.add_vertex_no_props(t, dst)?;

        let layer = self.get_or_allocate_layer(layer);

        if let Some(e_id) = self.find_edge(src_id, dst_id, layer.into()) {
            let mut edge = self.storage.get_edge_mut(e_id.into());
            edge.deletions_mut(layer).insert(t);
        } else {
            self.link_nodes(src_id, dst_id, t, layer, |new_edge| {
                new_edge.deletions_mut(layer).insert(t);
            });
        }

        Ok(())
    }

    fn get_or_allocate_layer(&self, layer: Option<&str>) -> usize {
        layer
            .map(|layer| self.edge_meta.get_or_create_layer_id(layer.to_owned()))
            .unwrap_or(0)
    }

    fn link_nodes<F: FnOnce(&mut EdgeStore<N>)>(
        &self,
        src_id: VID,
        dst_id: VID,
        t: TimeIndexEntry,
        layer: usize,
        edge_fn: F,
    ) -> EID {
        let mut node_pair = self.storage.pair_node_mut(src_id.into(), dst_id.into());
        let src = node_pair.get_mut_i();

        let edge_id = match src.find_edge(dst_id, LayerIds::All) {
            Some(edge_id) => {
                let mut edge = self.storage.get_edge_mut(edge_id);
                edge_fn(&mut edge);
                edge_id
            }
            None => {
                let mut edge = EdgeStore::new(src_id, dst_id);
                edge_fn(&mut edge);
                self.storage.push_edge(edge)
            }
        };

        src.add_edge(dst_id, Direction::OUT, layer, edge_id);
        let dst = node_pair.get_mut_j();
        dst.add_edge(src_id, Direction::IN, layer, edge_id);
        edge_id
    }

    pub(crate) fn add_edge_internal(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<EID, GraphError> {
        let src_id = self.add_vertex_no_props(t, src)?;
        let dst_id = self.add_vertex_no_props(t, dst)?;

        let layer = self.get_or_allocate_layer(layer);

        // resolve all props ahead of time to minimise the time spent holding locks
        let props: Vec<_> = self.edge_meta.resolve_prop_ids(props, false).collect();

        // get the entries for the src and dst nodes
        let edge_id = self.link_nodes(src_id, dst_id, t, layer, move |edge| {
            edge.additions_mut(layer).insert(t);
            let mut edge_layer = edge.layer_mut(layer);
            for (prop_id, prop_value) in props {
                edge_layer.add_prop(t, prop_id, prop_value);
            }
        }); // this is to release the node locks as quick as possible
        Ok(edge_id)
    }

    pub(crate) fn vertex_ids(&self) -> impl Iterator<Item = VID> {
        (0..self.storage.nodes_len()).map(|i| i.into())
    }

    pub(crate) fn locked_edges(&self) -> LockedIter<N, EdgeStore<N>> {
        self.storage.locked_edges()
    }

    pub(crate) fn find_edge(&self, src: VID, dst: VID, layer_id: LayerIds) -> Option<EID> {
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
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        // FIXME: this is not ideal as we get the edge twice just to check if it's active
        let edge = self.storage.get_edge(e.into());
        let active = {
            let t_index = edge.map(|entry| entry.additions());
            LockedLayeredIndex::new(layer_ids, t_index).active(t_start..t_end)
        };

        if !active {
            vec![]
        } else {
            let edge = self.storage.get_edge(e.into());
            let prop_id = self.edge_meta.resolve_prop_id(name, false);

            edge.props(None)
                .flat_map(|props| props.temporal_props_window(prop_id, t_start, t_end))
                .collect()
        }
    }

    pub(crate) fn vertex(&self, v: VID) -> Vertex<N> {
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

    pub(crate) fn edge(&self, e: EID) -> EdgeView<N> {
        let edge = self.storage.get_edge(e.into());
        EdgeView::from_entry(edge, self)
    }
}

#[cfg(test)]
mod test_additions {
    use crate::prelude::*;
    use rayon::{join, prelude::*};
    #[test]
    fn add_edge_and_read_props_concurrent() {
        let g = Graph::new();
        for t in 0..1000 {
            join(
                || g.add_edge(t, 1, 2, [("test", true)], None),
                || {
                    // if the edge exists already, it should have the property set
                    g.window(t, t + 1)
                        .edge(1, 2)
                        .map(|e| assert!(e.properties().get("test").is_some()))
                },
            );
        }
    }
}
