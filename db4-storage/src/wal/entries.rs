use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WalEntry {
    AddEdge(AddEdge),
    AddNodeID(AddNodeID),
    AddConstPropIDs(Vec<AddConstPropID>),
    AddTemporalPropIDs(Vec<AddTemporalPropID>),
    AddLayerID(AddLayerID),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddEdge {
    pub t: TimeIndexEntry,
    pub src: VID,
    pub dst: VID,
    pub eid: EID,
    pub layer_id: u64,
    pub t_props: Vec<(usize, Prop)>,
    pub c_props: Vec<(usize, Prop)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeID {
    pub gid: GID,
    pub vid: VID,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeTypeID {
    pub name: String,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddConstPropID {
    pub name: String,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddTemporalPropID {
    pub name: String,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddLayerID {
    pub name: String,
    pub id: u64,
}

// Constructors
impl WalEntry {
    pub fn add_edge(
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: u64,
        t_props: Vec<(usize, Prop)>,
        c_props: Vec<(usize, Prop)>,
    ) -> Self {
        Self::AddEdge(AddEdge {
            t,
            src,
            dst,
            eid,
            layer_id,
            t_props,
            c_props,
        })
    }

    pub fn add_node_id(gid: GID, vid: VID) -> Self {
        Self::AddNodeID(AddNodeID { gid, vid })
    }

    pub fn add_const_prop_ids(props: Vec<(String, u64)>) -> Self {
        Self::AddConstPropIDs(
            props.into_iter()
                .map(|(name, id)| AddConstPropID { name, id })
                .collect(),
        )
    }

    pub fn add_temporal_prop_ids(props: Vec<(String, u64)>) -> Self {
        Self::AddTemporalPropIDs(
            props.into_iter()
                .map(|(name, id)| AddTemporalPropID { name, id })
                .collect(),
        )
    }

    pub fn add_layer_id(name: String, id: u64) -> Self {
        Self::AddLayerID(AddLayerID { name, id })
    }
}

impl WalEntry {
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}
