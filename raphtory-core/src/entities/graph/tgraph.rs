use super::logical_to_physical::{InvalidNodeId, Mapping};
use crate::{
    entities::{
        edges::edge_store::EdgeStore,
        graph::{
            tgraph_storage::GraphStorage,
            timer::{MaxCounter, MinCounter, TimeCounterTrait},
        },
        nodes::{
            node_ref::{AsNodeRef, NodeRef},
            node_store::NodeStore,
        },
        properties::graph_meta::GraphMeta,
        LayerIds, EID, VID,
    },
    storage::{
        raw_edges::EdgeWGuard,
        timeindex::{AsTime, TimeIndexEntry},
        PairEntryMut,
    },
};
use dashmap::DashSet;
use either::Either;
use raphtory_api::core::{
    entities::{
        properties::{meta::Meta, prop::Prop},
        GidRef, Layer,
    },
    input::input_node::InputNode,
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew},
    Direction,
};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::BuildHasherDefault, sync::atomic::AtomicUsize};
use thiserror::Error;

pub(crate) type FxDashSet<K> = DashSet<K, BuildHasherDefault<FxHasher>>;

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph {
    pub storage: GraphStorage,
    // mapping between logical and physical ids
    pub logical_to_physical: Mapping,
    string_pool: FxDashSet<ArcStr>,
    pub event_counter: AtomicUsize,
    //earliest time seen in this graph
    pub earliest_time: MinCounter,
    //latest time seen in this graph
    pub latest_time: MaxCounter,
    // props meta data for nodes (mapping between strings and ids)
    pub node_meta: Meta,
    // props meta data for edges (mapping between strings and ids)
    pub edge_meta: Meta,
    // graph properties
    pub graph_meta: GraphMeta,
}

#[derive(Error, Debug)]
#[error("Invalid layer: {invalid_layer}. Valid layers: {valid_layers}")]
pub struct InvalidLayer {
    invalid_layer: ArcStr,
    valid_layers: String,
}

impl InvalidLayer {
    pub fn new(invalid_layer: ArcStr, valid: Vec<String>) -> Self {
        let valid_layers = valid.join(", ");
        Self {
            invalid_layer,
            valid_layers,
        }
    }
}

impl std::fmt::Display for TemporalGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Graph(num_nodes={}, num_edges={})",
            self.storage.nodes_len(),
            self.storage.edges_len()
        )
    }
}

impl Default for TemporalGraph {
    fn default() -> Self {
        Self::new(rayon::current_num_threads())
    }
}

impl TemporalGraph {
    pub fn new(num_locks: usize) -> Self {
        TemporalGraph {
            logical_to_physical: Mapping::new(),
            string_pool: Default::default(),
            storage: GraphStorage::new(num_locks),
            event_counter: AtomicUsize::new(0),
            earliest_time: MinCounter::new(),
            latest_time: MaxCounter::new(),
            node_meta: Meta::new(),
            edge_meta: Meta::new(),
            graph_meta: GraphMeta::new(),
        }
    }

    pub fn process_prop_value(&self, prop: &Prop) -> Prop {
        match prop {
            Prop::Str(value) => Prop::Str(self.resolve_str(value)),
            _ => prop.clone(),
        }
    }

    fn get_valid_layers(edge_meta: &Meta) -> Vec<String> {
        edge_meta
            .layer_meta()
            .get_keys()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    }

    pub fn num_layers(&self) -> usize {
        self.edge_meta.layer_meta().len()
    }

    pub fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, InvalidNodeId> {
        match id.as_gid_ref() {
            Either::Left(id) => self.logical_to_physical.get_or_init_node(id, || {
                let node_store = NodeStore::empty(id.into());
                self.storage.push_node(node_store)
            }),
            Either::Right(id) => Ok(MaybeNew::Existing(id)),
        }
    }

    pub fn layer_ids(&self, key: Layer) -> Result<LayerIds, InvalidLayer> {
        match key {
            Layer::None => Ok(LayerIds::None),
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(id) => match self.edge_meta.get_layer_id(&id) {
                Some(id) => Ok(LayerIds::One(id)),
                None => Err(InvalidLayer::new(
                    id,
                    Self::get_valid_layers(&self.edge_meta),
                )),
            },
            Layer::Multiple(ids) => {
                let mut new_layers = ids
                    .iter()
                    .map(|id| {
                        self.edge_meta.get_layer_id(id).ok_or_else(|| {
                            InvalidLayer::new(id.clone(), Self::get_valid_layers(&self.edge_meta))
                        })
                    })
                    .collect::<Result<Vec<_>, InvalidLayer>>()?;
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

    pub fn valid_layer_ids(&self, key: Layer) -> LayerIds {
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

    pub fn get_layer_name(&self, layer: usize) -> ArcStr {
        self.edge_meta.get_layer_name_by_id(layer)
    }

    #[inline]
    pub fn graph_earliest_time(&self) -> Option<i64> {
        Some(self.earliest_time.get()).filter(|t| *t != i64::MAX)
    }

    #[inline]
    pub fn graph_latest_time(&self) -> Option<i64> {
        Some(self.latest_time.get()).filter(|t| *t != i64::MIN)
    }

    #[inline]
    pub fn internal_num_nodes(&self) -> usize {
        self.storage.nodes.len()
    }

    #[inline]
    pub fn update_time(&self, time: TimeIndexEntry) {
        let t = time.t();
        self.earliest_time.update(t);
        self.latest_time.update(t);
    }

    pub(crate) fn link_nodes_inner(
        &self,
        node_pair: &mut PairEntryMut,
        edge_id: EID,
        t: TimeIndexEntry,
        layer: usize,
        is_deletion: bool,
    ) {
        self.update_time(t);
        let src_id = node_pair.get_i().vid;
        let dst_id = node_pair.get_j().vid;
        let src = node_pair.get_mut_i();
        let elid = if is_deletion {
            edge_id.with_layer_deletion(layer)
        } else {
            edge_id.with_layer(layer)
        };
        src.add_edge(dst_id, Direction::OUT, layer, edge_id);
        src.update_time(t, elid);
        let dst = node_pair.get_mut_j();
        dst.add_edge(src_id, Direction::IN, layer, edge_id);
        dst.update_time(t, elid);
    }

    pub fn link_edge(
        &self,
        eid: EID,
        t: TimeIndexEntry,
        layer: usize,
        is_deletion: bool,
    ) -> EdgeWGuard {
        let (src, dst) = {
            let edge_r = self.storage.edges.get_edge(eid);
            let edge_r = edge_r.as_mem_edge().edge_store();
            (edge_r.src, edge_r.dst)
        };
        // need to get the node pair first to avoid deadlocks with link_nodes
        let mut node_pair = self.storage.pair_node_mut(src, dst);
        self.link_nodes_inner(&mut node_pair, eid, t, layer, is_deletion);
        self.storage.edges.get_edge_mut(eid)
    }

    pub fn link_nodes(
        &self,
        src_id: VID,
        dst_id: VID,
        t: TimeIndexEntry,
        layer: usize,
        is_deletion: bool,
    ) -> MaybeNew<EdgeWGuard> {
        let edge = {
            let mut node_pair = self.storage.pair_node_mut(src_id, dst_id);
            let src = node_pair.get_i();
            let mut edge = match src.find_edge_eid(dst_id, &LayerIds::All) {
                Some(edge_id) => Either::Left(self.storage.get_edge_mut(edge_id)),
                None => Either::Right(self.storage.push_edge(EdgeStore::new(src_id, dst_id))),
            };
            let eid = match edge.as_mut() {
                Either::Left(edge) => edge.as_ref().eid(),
                Either::Right(edge) => edge.value().eid,
            };
            self.link_nodes_inner(&mut node_pair, eid, t, layer, is_deletion);
            edge
        };

        match edge {
            Either::Left(edge) => MaybeNew::Existing(edge),
            Either::Right(edge) => {
                let edge = edge.init();
                MaybeNew::New(edge)
            }
        }
    }

    #[inline]
    pub fn resolve_node_ref(&self, v: NodeRef) -> Option<VID> {
        match v {
            NodeRef::Internal(vid) => Some(vid),
            NodeRef::External(GidRef::U64(gid)) => self.logical_to_physical.get_u64(gid),
            NodeRef::External(GidRef::Str(string)) => self
                .logical_to_physical
                .get_str(string)
                .or_else(|| self.logical_to_physical.get_u64(string.id())),
        }
    }

    /// Checks if the same string value already exists and returns a pointer to the same existing value if it exists,
    /// otherwise adds the string to the pool.
    fn resolve_str(&self, value: &ArcStr) -> ArcStr {
        match self.string_pool.get(value) {
            Some(value) => value.clone(),
            None => {
                self.string_pool.insert(value.clone());
                self.string_pool
                    .get(value)
                    .expect("value should exist as inserted above")
                    .clone()
            }
        }
    }
}
