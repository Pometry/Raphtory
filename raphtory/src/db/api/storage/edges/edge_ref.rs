use crate::{
    core::entities::{
        edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
        LayerIds, EID, VID,
    },
    db::api::{storage::arrow::edges::ArrowEdge, view::internal::EdgeLike},
    prelude::Layer,
};

#[derive(Copy, Clone, Debug)]
pub enum EdgeStorageRef<'a> {
    Mem(&'a EdgeStore),
    #[cfg(feature = "arrow")]
    Arrow(ArrowEdge<'a>),
}

impl<'a> EdgeStorageRef<'a> {
    #[inline]
    pub fn eid(&self) -> EID {
        match self {
            EdgeStorageRef::Mem(e) => e.eid,
            #[cfg(feature = "arrow")]
            EdgeStorageRef::Arrow(e) => e.eid(),
        }
    }

    #[inline]
    pub fn src(&self) -> VID {
        match self {
            EdgeStorageRef::Mem(e) => e.src,
            #[cfg(feature = "arrow")]
            EdgeStorageRef::Arrow(e) => e.src(),
        }
    }

    #[inline]
    pub fn dst(&self) -> VID {
        match self {
            EdgeStorageRef::Mem(e) => e.dst,
            #[cfg(feature = "arrow")]
            EdgeStorageRef::Arrow(e) => e.dst(),
        }
    }

    #[inline]
    pub fn in_ref(self) -> EdgeRef {
        EdgeRef::new_incoming(self.eid(), self.src(), self.dst())
    }

    #[inline]
    pub fn out_ref(self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn has_layer(&self, layer_ids: &LayerIds) -> bool {
        match self {
            EdgeStorageRef::Mem(edge) => edge.has_layer(layer_ids),
            EdgeStorageRef::Arrow(edge) => edge.has_layer(layer_ids),
        }
    }
}
