use super::logical_to_physical::Mapping;
use crate::{
    core::{
        entities::{
            edges::edge_store::{EdgeDataLike, EdgeStore},
            graph::{
                tgraph_storage::GraphStorage,
                timer::{MaxCounter, MinCounter, TimeCounterTrait},
            },
            nodes::{node_ref::NodeRef, node_store::NodeStore},
            properties::{graph_meta::GraphMeta, props::Meta},
            LayerIds, EID, VID,
        },
        storage::{
            raw_edges::MutEdge,
            timeindex::{AsTime, TimeIndexEntry},
            PairEntryMut,
        },
        utils::{errors::GraphError, iter::GenLockedIter},
        Direction, Prop,
    },
    db::api::{
        storage::graph::edges::edge_storage_ops::EdgeStorageOps,
        view::{IntoDynBoxed, Layer},
    },
};
use dashmap::DashSet;
use either::Either;
use itertools::Itertools;
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, GidRef},
    input::input_node::InputNode,
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew},
};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow, collections::HashMap, fmt::Debug, hash::BuildHasherDefault, iter,
    sync::atomic::AtomicUsize,
};

pub(crate) type FxDashSet<K> = DashSet<K, BuildHasherDefault<FxHasher>>;

#[derive(Serialize, Deserialize, Debug)]
pub struct TemporalGraph {
    // mapping between logical and physical ids
    pub(crate) logical_to_physical: Mapping,
    string_pool: FxDashSet<ArcStr>,

    pub(crate) storage: GraphStorage,

    pub(crate) event_counter: AtomicUsize,

    //earliest time seen in this graph
    pub(in crate::core) earliest_time: MinCounter,

    //latest time seen in this graph
    pub(in crate::core) latest_time: MaxCounter,

    // props meta data for nodes (mapping between strings and ids)
    pub(crate) node_meta: Meta,

    // props meta data for edges (mapping between strings and ids)
    pub(crate) edge_meta: Meta,

    // graph properties
    pub(crate) graph_meta: GraphMeta,
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

    pub(crate) fn process_prop_value(&self, prop: &Prop) -> Prop {
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

    pub(crate) fn num_layers(&self) -> usize {
        self.edge_meta.layer_meta().len()
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

    pub(crate) fn core_temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let entry = self.storage.edge_entry(e.pid());
        let layer_ids = layer_ids.constrain_from_edge(e).into_owned();
        GenLockedIter::from((entry, layer_ids), |(entry, layer_ids)| {
            let iter: Box<dyn Iterator<Item = usize> + Send> = match layer_ids {
                LayerIds::None => Box::new(iter::empty()),
                LayerIds::All => entry.temp_prop_ids(None),
                LayerIds::One(id) => entry.temp_prop_ids(Some(*id)),
                LayerIds::Multiple(ids) => Box::new(
                    ids.iter()
                        .map(|id| entry.temp_prop_ids(Some(*id)))
                        .kmerge()
                        .dedup(),
                ),
            };
            iter
        })
        .into_dyn_boxed()
    }

    pub(crate) fn core_const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let entry = self.storage.edge_entry(e.pid());
        GenLockedIter::from((entry, layer_ids), |(entry, layer_ids)| {
            let layer_ids = layer_ids.constrain_from_edge(e);
            match layer_ids.as_ref() {
                LayerIds::None => Box::new(iter::empty()),
                LayerIds::All => entry
                    .layer_iter()
                    .map(|(_, data)| data.const_prop_ids())
                    .kmerge()
                    .dedup()
                    .into_dyn_boxed(),
                LayerIds::One(id) => match entry.layer(*id) {
                    Some(l) => l.const_prop_ids().into_dyn_boxed(),
                    None => Box::new(iter::empty()),
                },
                LayerIds::Multiple(ids) => ids
                    .iter()
                    .flat_map(|id| entry.layer(*id).map(|l| l.const_prop_ids()))
                    .kmerge()
                    .dedup()
                    .into_dyn_boxed(),
            }
        })
        .into_dyn_boxed()
    }

    pub(crate) fn core_get_const_edge_prop(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let entry = self.storage.edge_entry(e.pid());
        match layer_ids.borrow() {
            LayerIds::None => None,
            LayerIds::All => {
                if self.num_layers() == 1 {
                    // iterator has at most 1 element
                    entry
                        .layer_iter()
                        .next()
                        .and_then(|(_, data)| data.const_prop(prop_id).cloned())
                } else {
                    let prop_map: HashMap<_, _> = entry
                        .layer_iter()
                        .flat_map(|(id, data)| {
                            data.const_prop(prop_id)
                                .map(|p| (self.get_layer_name(id), p.clone()))
                        })
                        .collect();
                    if prop_map.is_empty() {
                        None
                    } else {
                        Some(prop_map.into())
                    }
                }
            }
            LayerIds::One(id) => entry
                .layer(*id)
                .and_then(|l| l.const_prop(prop_id).cloned()),
            LayerIds::Multiple(ids) => {
                let prop_map: HashMap<_, _> = ids
                    .iter()
                    .flat_map(|&id| {
                        entry.layer(id).and_then(|data| {
                            data.const_prop(prop_id)
                                .map(|p| (self.get_layer_name(id), p.clone()))
                        })
                    })
                    .collect();
                if prop_map.is_empty() {
                    None
                } else {
                    Some(prop_map.into())
                }
            }
        }
    }

    #[inline]
    pub(crate) fn internal_num_nodes(&self) -> usize {
        self.storage.nodes.len()
    }

    #[inline]
    pub(crate) fn update_time(&self, time: TimeIndexEntry) {
        let t = time.t();
        self.earliest_time.update(t);
        self.latest_time.update(t);
    }

    pub(crate) fn link_nodes_inner(
        &self,
        node_pair: &mut PairEntryMut<NodeStore>,
        edge_id: EID,
        t: TimeIndexEntry,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.update_time(t);
        let src_id = node_pair.get_i().vid;
        let dst_id = node_pair.get_j().vid;
        let src = node_pair.get_mut_i();
        src.add_edge(dst_id, Direction::OUT, layer, edge_id);
        src.update_time(t);
        let dst = node_pair.get_mut_j();
        dst.add_edge(src_id, Direction::IN, layer, edge_id);
        dst.update_time(t);
        Ok(())
    }

    pub(crate) fn link_edge(
        &self,
        eid: EID,
        t: TimeIndexEntry,
        layer: usize,
        edge_fn: impl FnOnce(MutEdge) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        let (src, dst) = {
            let edge_r = self.storage.edges.get_edge(eid);
            let edge_r = edge_r.as_mem_edge();
            (edge_r.src(), edge_r.dst())
        };
        // need to get the node pair first to avoid deadlocks with link_nodes
        {
            let mut node_pair = self.storage.pair_node_mut(src, dst);
            self.link_nodes_inner(&mut node_pair, eid, t, layer)?;
        }
        let mut edge_w = self.storage.edges.get_edge_mut(eid);
        edge_fn(edge_w.as_mut())
    }

    pub(crate) fn link_nodes<F: FnOnce(MutEdge) -> Result<(), GraphError>>(
        &self,
        src_id: VID,
        dst_id: VID,
        t: TimeIndexEntry,
        layer: usize,
        edge_fn: F,
    ) -> Result<MaybeNew<EID>, GraphError> {
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
            self.link_nodes_inner(&mut node_pair, eid, t, layer)?;
            edge
        };

        match edge {
            Either::Left(mut edge) => {
                edge_fn(edge.as_mut())?;
                Ok(MaybeNew::Existing(edge.as_ref().eid()))
            }
            Either::Right(edge) => {
                let mut edge = edge.init();
                edge_fn(edge.as_mut())?;
                Ok(MaybeNew::New(edge.as_ref().eid()))
            }
        }
    }

    pub(crate) fn resolve_node_ref(&self, v: NodeRef) -> Option<VID> {
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
