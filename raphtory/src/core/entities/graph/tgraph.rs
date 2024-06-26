use crate::{
    core::{
        entities::{
            edges::edge_store::EdgeStore,
            graph::{
                tgraph_storage::GraphStorage,
                timer::{MaxCounter, MinCounter, TimeCounterTrait},
            },
            nodes::{node_ref::NodeRef, node_store::NodeStore},
            properties::{graph_meta::GraphMeta, props::Meta, tprop::TProp},
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{AsTime, TimeIndexEntry},
            Entry, EntryMut,
        },
        utils::errors::GraphError,
        Direction, Prop,
    },
    db::api::{
        storage::locked::LockedGraph,
        view::{BoxedIter, Layer},
    },
    prelude::DeletionOps,
};
use dashmap::DashSet;
use raphtory_api::core::{
    input::input_node::InputNode,
    storage::{arc_str::ArcStr, locked_vec::ArcReadLockedVec, FxDashMap},
};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    hash::BuildHasherDefault,
    iter,
    sync::{atomic::AtomicUsize, Arc},
};

pub(crate) type FxDashSet<K> = DashSet<K, BuildHasherDefault<FxHasher>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InternalGraph(Arc<TemporalGraph>);

impl DeletionOps for InternalGraph {}

impl InternalGraph {
    #[inline]
    pub(crate) fn inner(&self) -> &TemporalGraph {
        &self.0
    }

    pub(crate) fn lock(&self) -> LockedGraph {
        let nodes = Arc::new(self.inner().storage.nodes.read_lock());
        let edges = Arc::new(self.inner().storage.edges.read_lock());
        LockedGraph { nodes, edges }
    }

    pub(crate) fn new(num_locks: usize) -> Self {
        let tg = TemporalGraph {
            logical_to_physical: FxDashMap::default(), // TODO: could use DictMapper here
            string_pool: Default::default(),
            storage: GraphStorage::new(num_locks),
            event_counter: AtomicUsize::new(0),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            node_meta: Arc::new(Meta::new()),
            edge_meta: Arc::new(Meta::new()),
            graph_meta: GraphMeta::new(),
        };

        Self(Arc::new(tg))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph {
    // mapping between logical and physical ids
    logical_to_physical: FxDashMap<u64, VID>,
    string_pool: FxDashSet<ArcStr>,

    pub(crate) storage: GraphStorage,

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
    pub(crate) graph_meta: GraphMeta,
}

impl std::fmt::Display for InternalGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_nodes={}, num_edges={})",
            self.inner().storage.nodes.len(),
            self.inner().storage.edges.len()
        )
    }
}

impl Default for InternalGraph {
    fn default() -> Self {
        Self::new(rayon::current_num_threads())
    }
}

impl TemporalGraph {
    fn get_valid_layers(edge_meta: &Arc<Meta>) -> Vec<String> {
        edge_meta
            .layer_meta()
            .get_keys()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    }

    pub(crate) fn num_layers(&self) -> usize {
        self.edge_meta.layer_meta().len()
    }

    pub(crate) fn layer_names(&self, layer_ids: &LayerIds) -> BoxedIter<ArcStr> {
        let layer_ids = layer_ids.clone();
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

    pub(crate) fn layer_ids(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match key {
            Layer::None => Ok(LayerIds::None),
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => Ok(LayerIds::One(id)),
                None => Err(GraphError::invalid_layer(
                    id.to_string(),
                    Self::get_valid_layers(&self.edge_meta),
                )),
            },
            Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .map(|id| {
                        self.edge_meta.get_layer_id(id).ok_or_else(|| {
                            GraphError::invalid_layer(
                                id.to_string(),
                                Self::get_valid_layers(&self.edge_meta),
                            )
                        })
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

    pub(crate) fn node_type(&self, v: VID) -> Option<ArcStr> {
        let node = self.storage.get_node(v);
        self.node_meta.get_node_type_name_by_id(node.node_type)
    }

    pub(crate) fn node_type_id(&self, v: VID) -> usize {
        let node = self.storage.get_node(v);
        node.node_type
    }

    pub(crate) fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.node_meta.get_all_node_types()
    }

    #[inline]
    pub(crate) fn node_entry(&self, v: VID) -> Entry<'_, NodeStore> {
        self.storage.get_node(v)
    }

    #[inline]
    pub(crate) fn edge_entry(&self, e: EID) -> Entry<'_, EdgeStore> {
        self.storage.get_edge(e)
    }
}

impl TemporalGraph {
    pub(crate) fn internal_num_nodes(&self) -> usize {
        self.storage.nodes.len()
    }

    #[inline]
    fn update_time(&self, time: TimeIndexEntry) {
        let t = time.t();
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

    pub(crate) fn resolve_node_type(
        &self,
        v_id: VID,
        node_type: Option<&str>,
    ) -> Result<usize, GraphError> {
        let mut node = self.storage.get_node_mut(v_id);
        match node_type {
            None => Ok(node.node_type),
            Some(node_type) => {
                if node_type == "_default" {
                    return Err(GraphError::NodeTypeError(
                        "_default type is not allowed to be used on nodes"
                            .parse()
                            .unwrap(),
                    ));
                }
                match node.node_type {
                    0 => {
                        let node_type_id = self.node_meta.get_or_create_node_type_id(node_type);
                        node.update_node_type(node_type_id);
                        Ok(node_type_id)
                    }
                    _ => {
                        let new_node_type_id =
                            self.node_meta.get_node_type_id(node_type).unwrap_or(0);
                        if node.node_type != new_node_type_id {
                            return Err(GraphError::NodeTypeError(
                                "Node already has a non-default type".parse().unwrap(),
                            ));
                        }
                        // Returns the original node type to prevent type being changed
                        Ok(node.node_type)
                    }
                }
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
        node.update_node_type(node_type_id);
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

    pub(crate) fn add_constant_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (id, prop) in props {
            self.graph_meta.add_constant_prop(id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn update_constant_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (id, prop) in props {
            self.graph_meta.update_constant_prop(id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        for (prop_id, prop) in props {
            self.graph_meta.add_prop(t, prop_id, prop)?;
        }
        Ok(())
    }

    pub(crate) fn get_constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph_meta.get_constant(id)
    }

    pub(crate) fn get_temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph_meta.get_temporal_prop(id)
    }

    pub(crate) fn const_prop_names(&self) -> ArcReadLockedVec<ArcStr> {
        self.graph_meta.constant_names()
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

    fn link_nodes<F: FnOnce(&mut EdgeStore) -> Result<(), GraphError>>(
        &self,
        src_id: VID,
        dst_id: VID,
        t: TimeIndexEntry,
        layer: usize,
        edge_fn: F,
    ) -> Result<EID, GraphError> {
        let mut node_pair = self.storage.pair_node_mut(src_id, dst_id);
        self.update_time(t);
        let src = node_pair.get_mut_i();

        let edge_id = match src.find_edge_eid(dst_id, &LayerIds::All) {
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

    pub(crate) fn resolve_node_ref(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(gid) => {
                let v_id = self.logical_to_physical.get(&gid)?;
                Some(*v_id)
            }
            NodeRef::ExternalStr(string) => self.resolve_node_ref(NodeRef::External(string.id())),
        }
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
    use rayon::join;

    use crate::prelude::*;

    #[test]
    fn add_edge_and_read_props_concurrent() {
        let g = Graph::new();
        for t in 0..1000 {
            join(
                || g.add_edge(t, 1, 2, [("test", true)], None).unwrap(),
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
