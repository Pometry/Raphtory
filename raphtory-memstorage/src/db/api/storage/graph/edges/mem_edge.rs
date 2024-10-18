use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::core::{
    entities::{
        edges::edge_store::EdgeStore,
        properties::{props::Props, tprop::TProp},
    },
    storage::{raw_edges::EdgeShard, timeindex::TimeIndex},
};

#[derive(Clone, Copy, Debug)]
pub struct MemEdge<'a> {
    edges: &'a EdgeShard,
    offset: usize,
}

impl<'a> MemEdge<'a> {
    pub fn new(edges: &'a EdgeShard, offset: usize) -> Self {
        MemEdge { edges, offset }
    }

    pub fn edge_store(&self) -> &EdgeStore {
        self.edges.edge_store(self.offset)
    }

    #[inline]
    pub fn props(&self, layer_id: usize) -> Option<&Props> {
        self.edges
            .props(self.offset, layer_id)
            .and_then(|el| el.props())
    }

    pub fn eid(self) -> EID {
        self.edge_store().eid
    }

    pub fn src(self) -> VID {
        self.edge_store().src
    }

    pub fn dst(self) -> VID {
        self.edge_store().dst
    }

    pub fn as_edge_ref(&self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn internal_num_layers(self) -> usize {
        self.edges.internal_num_layers()
    }

    pub fn get_additions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.additions(self.offset, layer_id)
    }

    pub fn get_deletions(self, layer_id: usize) -> Option<&'a TimeIndex<TimeIndexEntry>> {
        self.edges.deletions(self.offset, layer_id)
    }

    pub fn has_layer_inner(self, layer_id: usize) -> bool {
        self.get_additions(layer_id)
            .filter(|t_index| !t_index.is_empty())
            .is_some()
            || self
                .get_deletions(layer_id)
                .filter(|t_index| !t_index.is_empty())
                .is_some()
    }

    pub fn has_layers(self, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => true,
            LayerIds::One(id) => self.has_layer_inner(*id),
            LayerIds::Multiple(ids) => ids.iter().any(|id| self.has_layer_inner(*id)),
        }
    }

    pub fn temporal_prop_layer_inner(self, layer_id: usize, prop_id: usize) -> Option<&'a TProp> {
        let layer = self.edges.props(self.offset, layer_id)?;
        layer.temporal_property(prop_id)
    }
}
