use raphtory_api::core::entities::{edges::edge_ref::EdgeRef, LayerIds, EID, VID};

use super::mem_edge::MemEdge;

#[derive(Copy, Clone, Debug)]
pub enum EdgeStorageRef<'a> {
    Mem(MemEdge<'a>),
    #[cfg(feature = "storage")]
    Disk(DiskEdge<'a>),
}

impl <'a> EdgeStorageRef<'a> {

    pub fn src(self) -> VID {
        match self {
            EdgeStorageRef::Mem(edge) => edge.src(),
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk(edge) => edge.src(),
        }
    }

    pub fn dst(self) -> VID {
        match self {
            EdgeStorageRef::Mem(edge) => edge.dst(),
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk(edge) => edge.dst(),
        }
    }

    pub fn eid(self) -> EID {
        match self {
            EdgeStorageRef::Mem(edge) => edge.eid(),
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk(edge) => edge.eid(),
        }
    }

    pub fn out_ref(self) -> EdgeRef {
        EdgeRef::new_outgoing(self.eid(), self.src(), self.dst())
    }

    pub fn has_layers(self, layer_id: &LayerIds) -> bool {
        match self {
            EdgeStorageRef::Mem(edge) => edge.has_layers(layer_id),
            #[cfg(feature = "storage")]
            EdgeStorageRef::Disk(edge) => edge.has_layer(layer_id),
        }
    }

}
