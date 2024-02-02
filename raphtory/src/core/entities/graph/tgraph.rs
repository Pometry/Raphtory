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
            nodes::{
                input_node::InputNode,
                node::{ArcEdge, ArcNode, Node},
                node_ref::NodeRef,
                node_store::NodeStore,
            },
            properties::{
                graph_props::GraphProps,
                props::{ArcReadLockedVec, Meta},
                tprop::TProp,
            },
            LayerIds, EID, VID,
        },
        storage::{
            lazy_vec::IllegalSet,
            locked_view::LockedView,
            timeindex::{AsTime, LayeredIndex, TimeIndexEntry, TimeIndexOps},
            ArcEntry, Entry, EntryMut,
        },
        utils::{
            errors::{GraphError, IllegalMutate, MutateGraphError},
            time::TryIntoTime,
        },
        ArcStr, Direction, Prop, PropUnwrap,
    },
    db::api::view::{internal::EdgeFilter, BoxedIter, Layer},
    prelude::DeletionOps,
};
use dashmap::{DashMap, DashSet};
use itertools::Itertools;
use parking_lot::RwLockReadGuard;
use rayon::prelude::*;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    hash::BuildHasherDefault,
    iter,
    ops::{Deref, Range},
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

pub(crate) type FxDashMap<K, V> = DashMap<K, V, BuildHasherDefault<FxHasher>>;
pub(crate) type FxDashSet<K> = DashSet<K, BuildHasherDefault<FxHasher>>;

pub(crate) type TGraph<const N: usize> = TemporalGraph<N>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InnerTemporalGraph<const N: usize>(Arc<TemporalGraph<N>>);

impl<const N: usize> DeletionOps for InnerTemporalGraph<N> {}

impl<const N: usize> InnerTemporalGraph<N> {
    #[inline]
    pub(crate) fn inner(&self) -> &TemporalGraph<N> {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph<const N: usize> {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, VID>,
    string_pool: FxDashSet<ArcStr>,

    pub(crate) storage: GraphStorage<N>,

    pub(crate) event_counter: AtomicUsize,

    //earliest time seen in this graph
    pub(in crate::core) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(in crate::core) latest_time: MaxCounter,

    // props meta data for nodes (mapping between strings and ids)
    pub(crate) node_meta: Arc<Meta>,

    // props meta data for edges (mapping between strings and ids)
    pub(crate) edge_meta: Arc<Meta>,

    // graph properties
    pub(crate) graph_props: GraphProps,
}

impl<const N: usize> std::fmt::Display for InnerTemporalGraph<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_nodes={}, num_edges={})",
            self.inner().storage.nodes.len(),
            self.inner().storage.edges.len()
        )
    }
}

impl<const N: usize> Default for InnerTemporalGraph<N> {
    fn default() -> Self {
        let tg = TemporalGraph {
            logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
            string_pool: Default::default(),
            storage: GraphStorage::new(),
            event_counter: AtomicUsize::new(0),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            node_meta: Arc::new(Meta::new()),
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

    pub(crate) fn layer_names(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        match layer_ids {
            LayerIds::None => Box::new(iter::empty()),
            LayerIds::All => Box::new(self.edge_meta.layer_meta().get_keys().into_iter()),
            LayerIds::One(id) => {
                let name = self.edge_meta.layer_meta().get_name(id).clone();
                Box::new(iter::once(name))
            }
            LayerIds::Multiple(ids) => {
                let keys = self.edge_meta.layer_meta().get_keys();
                Box::new((0..ids.len()).map(move |index| {
                    let id = ids[index];
                    keys[id].clone()
                }))
            }
        }
    }

    fn as_local_node(&self, v: NodeRef) -> Result<VID, GraphError> {
        match v {
            NodeRef::Internal(vid) => Ok(vid),
            NodeRef::External(gid) => self
                .logical_to_physical
                .get(&gid)
                .map(|entry| *entry)
                .ok_or(GraphError::FailedToMutateGraph {
                    source: MutateGraphError::NodeNotFoundError { node_id: gid },
                }),
        }
    }

    pub(crate) fn get_all_node_property_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        self.node_meta.get_all_property_names(is_static)
    }

    pub(crate) fn get_all_edge_property_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        self.edge_meta.get_all_property_names(is_static)
    }

    pub(crate) fn get_all_layers(&self) -> Vec<usize> {
        self.edge_meta.get_all_layers()
    }

    pub(crate) fn layer_ids(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match key {
            Layer::None => Ok(LayerIds::None),
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => Ok(LayerIds::One(id)),
                None => Err(GraphError::InvalidLayer(id.to_string())),
            },
            Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .map(|id| {
                        self.edge_meta
                            .get_layer_id(id)
                            .ok_or_else(|| GraphError::InvalidLayer(id.to_string()))
                    })
                    .collect::<Result<Vec<_>, GraphError>>()?;
                let num_layers = self.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    Ok(LayerIds::None)
                } else if num_new_layers == 1 {
                    Ok(LayerIds::One(new_layers[0]))
                } else if num_new_layers == num_layers {
                    Ok(LayerIds::All)
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    Ok(LayerIds::Multiple(new_layers.into()))
                }
            }
        }
    }

    pub(crate) fn valid_layer_ids(&self, key: Layer) -> LayerIds {
        match key {
            Layer::None => LayerIds::None,
            Layer::All => LayerIds::All,
            Layer::Default => LayerIds::One(0),
            Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => LayerIds::One(id),
                None => LayerIds::None,
            },
            Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .flat_map(|id| self.edge_meta.get_layer_id(id))
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

    pub(crate) fn get_layer_name(&self, layer: usize) -> ArcStr {
        self.edge_meta.get_layer_name_by_id(layer)
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

    #[inline]
    pub(crate) fn global_node_id(&self, v: VID) -> u64 {
        let node = self.storage.get_node(v);
        node.global_id()
    }

    pub(crate) fn node_name(&self, v: VID) -> String {
        let node = self.storage.get_node(v);
        node.name
            .clone()
            .unwrap_or_else(|| node.global_id().to_string())
    }

    pub(crate) fn node_type(&self, v: VID) -> String {
        let node = self.storage.get_node(v);
        self.node_meta
            .get_node_type_name_by_id(node.node_type)
            .clone()
            .0
            .to_string()
    }

    #[inline]
    pub(crate) fn node_entry(&self, v: VID) -> Entry<'_, NodeStore, N> {
        self.storage.get_node(v.into())
    }

    pub(crate) fn edge_refs(&self) -> impl Iterator<Item = EdgeRef> + Send {
        self.storage.edge_refs()
    }

    #[inline]
    pub(crate) fn edge_entry(&self, e: EID) -> Entry<'_, EdgeStore, N> {
        self.storage.get_edge(e.into())
    }
}

impl<const N: usize> TemporalGraph<N> {
    pub(crate) fn internal_num_nodes(&self) -> usize {
        self.storage.nodes.len()
    }
    #[inline]
    pub(crate) fn num_edges(&self, layers: &LayerIds, filter: Option<&EdgeFilter>) -> usize {
        match filter {
            None => match layers {
                LayerIds::All => self.storage.edges.len(),
                _ => {
                    let guard = self.storage.edges.read_lock();
                    guard.par_iter().filter(|e| e.has_layer(layers)).count()
                }
            },
            Some(filter) => {
                let guard = self.storage.edges.read_lock();
                guard.par_iter().filter(|&e| filter(e, layers)).count()
            }
        }
    }

    #[inline]
    pub(crate) fn degree(
        &self,
        v: VID,
        dir: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        let node_store = self.storage.get_node(v);
        match filter {
            None => node_store.degree(layers, dir),
            Some(filter) => {
                let edges_locked = self.storage.edges.read_lock();
                node_store
                    .edge_tuples(layers, dir)
                    .filter(|e| filter(edges_locked.get(e.pid().into()), layers))
                    .dedup_by(|e1, e2| e1.remote() == e2.remote())
                    .count()
            }
        }
    }

    #[inline]
    fn update_time(&self, time: TimeIndexEntry) {
        let t = *time.t();
        self.earliest_time.update(t);
        self.latest_time.update(t);
    }

    /// return local id for node, initialising storage if node does not exist yet
    pub(crate) fn resolve_node(&self, id: u64, name: Option<&str>) -> VID {
        *(self.logical_to_physical.entry(id).or_insert_with(|| {
            let name = name.map(|s| s.to_owned());
            let node_store = NodeStore::empty(id, name);
            self.storage.push_node(node_store)
        }))
    }

    pub(crate) fn resolve_node_type(&self, v_id: VID, node_type: Option<&str>) -> usize {
        match node_type {
            None => self.node_meta.get_default_node_type_id(),
            Some(node_type) => {
                let node_type_id = self.node_meta.get_or_create_node_type_id(node_type);
                let mut node = self.storage.get_node_mut(v_id);
                node.update_node_type(node_type_id);
                node_type_id
            }
        }
    }

    #[inline]
    pub(crate) fn add_node_no_props(
        &self,
        time: TimeIndexEntry,
        v_id: VID,
        node_type_id: usize,
    ) -> EntryMut<NodeStore> {
        self.update_time(time);
        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v_id);
        node.update_time(time);
        node
    }

    pub(crate) fn add_node_internal(
        &self,
        time: TimeIndexEntry,
        v_id: VID,
        props: Vec<(usize, Prop)>,
        node_type_id: usize,
    ) -> Result<(), GraphError> {
        let mut node = self.add_node_no_props(time, v_id, node_type_id);
        for (id, prop) in props {
            node.add_prop(time, id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn add_edge_properties_internal(
        &self,
        edge_id: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), IllegalMutate> {
        let mut edge = self.storage.get_edge_mut(edge_id.into());

        let mut layer = edge.layer_mut(layer);
        for (prop_id, prop) in props {
            layer.add_constant_prop(prop_id, prop).map_err(|err| {
                IllegalMutate::from_source(err, &self.edge_meta.get_prop_name(prop_id, true))
            })?;
        }
        Ok(())
    }

    pub(crate) fn add_constant_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (id, prop) in props {
            self.graph_props.add_constant_prop(id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn update_constant_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (id, prop) in props {
            self.graph_props.update_constant_prop(id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (prop_id, prop) in props {
            self.graph_props.add_prop(t, prop_id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn get_constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph_props.get_constant(id)
    }

    pub(crate) fn get_temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph_props.get_temporal_prop(id)
    }

    pub(crate) fn const_prop_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.graph_props.constant_names()
    }

    pub(crate) fn temporal_property_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.graph_props.temporal_names()
    }

    pub(crate) fn delete_edge(
        &self,
        t: TimeIndexEntry,
        src_id: VID,
        dst_id: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.link_nodes(src_id, dst_id, t, layer, |new_edge| {
            new_edge.deletions_mut(layer).insert(t);
            Ok(())
        })?;
        Ok(())
    }

    fn get_or_allocate_layer(&self, layer: Option<&str>) -> usize {
        layer
            .map(|layer| self.edge_meta.get_or_create_layer_id(layer))
            .unwrap_or(0)
    }

    fn get_or_allocate_node_type(&self, layer: Option<&str>) -> usize {
        layer
            .map(|layer| self.node_meta.get_or_create_node_type_id(layer))
            .unwrap_or(0)
    }

    fn link_nodes<F: FnOnce(&mut EdgeStore) -> Result<(), GraphError>>(
        &self,
        src_id: VID,
        dst_id: VID,
        t: TimeIndexEntry,
        layer: usize,
        edge_fn: F,
    ) -> Result<EID, GraphError> {
        let mut node_pair = self.storage.pair_node_mut(src_id.into(), dst_id.into());
        self.update_time(t);
        let src = node_pair.get_mut_i();

        let edge_id = match src.find_edge(dst_id, &LayerIds::All) {
            Some(edge_id) => {
                let mut edge = self.storage.get_edge_mut(edge_id);
                edge_fn(&mut edge)?;
                edge_id
            }
            None => {
                let mut edge = EdgeStore::new(src_id, dst_id);
                edge_fn(&mut edge)?;
                self.storage.push_edge(edge)
            }
        };

        src.add_edge(dst_id, Direction::OUT, layer, edge_id);
        src.update_time(t);
        let dst = node_pair.get_mut_j();
        dst.add_edge(src_id, Direction::IN, layer, edge_id);
        dst.update_time(t);
        Ok(edge_id)
    }

    pub(crate) fn add_edge_internal(
        &self,
        t: TimeIndexEntry,
        src_id: VID,
        dst_id: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        // get the entries for the src and dst nodes
        self.link_nodes(src_id, dst_id, t, layer, move |edge| {
            edge.additions_mut(layer).insert(t);
            let mut edge_layer = edge.layer_mut(layer);
            for (prop_id, prop_value) in props {
                edge_layer.add_prop(t, prop_id, prop_value)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub(crate) fn node_ids(&self) -> impl Iterator<Item = VID> {
        (0..self.storage.nodes.len()).map(|i| i.into())
    }

    pub(crate) fn locked_edges(&self) -> impl Iterator<Item = ArcEntry<EdgeStore>> {
        self.storage.locked_edges()
    }

    pub(crate) fn find_edge(&self, src: VID, dst: VID, layer_id: &LayerIds) -> Option<EID> {
        let node = self.storage.get_node(src.into());
        node.find_edge(dst, layer_id)
    }

    pub(crate) fn resolve_node_ref(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(gid) => {
                let v_id = self.logical_to_physical.get(&gid)?;
                Some((*v_id).into())
            }
        }
    }

    pub(crate) fn node(&self, v: VID) -> Node<N> {
        let node = self.storage.get_node(v.into());
        Node::from_entry(node, self)
    }

    pub(crate) fn node_arc(&self, v: VID) -> ArcNode {
        let node = self.storage.get_node_arc(v.into());
        ArcNode::from_entry(node, self.node_meta.clone())
    }

    pub(crate) fn edge_arc(&self, e: EID) -> ArcEdge {
        let edge = self.storage.get_edge_arc(e.into());
        ArcEdge::from_entry(edge, self.edge_meta.clone())
    }

    #[inline]
    pub(crate) fn edge(&self, e: EID) -> EdgeView<N> {
        let edge = self.storage.get_edge(e.into());
        EdgeView::from_entry(edge, self)
    }

    /// Checks if the same string value already exists and returns a pointer to the same existing value if it exists,
    /// otherwise adds the string to the pool.
    pub(crate) fn resolve_str(&self, value: ArcStr) -> ArcStr {
        match self.string_pool.get(&value) {
            Some(value) => value.clone(),
            None => {
                if self.string_pool.insert(value.clone()) {
                    value
                } else {
                    self.string_pool
                        .get(&value)
                        .expect("value exists due to insert above returning false")
                        .clone()
                }
            }
        }
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
