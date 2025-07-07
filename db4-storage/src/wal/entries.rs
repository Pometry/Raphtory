use raphtory_core::{
    entities::{VID, EID, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_api::core::entities::properties::prop::Prop;
use serde::{Serialize, Deserialize};
use std::borrow::Cow;

#[derive(Debug, Serialize, Deserialize)]
pub enum WalEntry<'a> {
    AddEdge(AddEdge<'a>),
    AddNodeID(AddNodeID),
    AddConstPropIDs(Vec<AddConstPropID<'a>>),
    AddTemporalPropIDs(Vec<AddTemporalPropID<'a>>),
    AddLayerID(AddLayerID),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddEdge<'a> {
    pub t: TimeIndexEntry,
    pub src: VID,
    pub dst: VID,
    pub eid: EID,
    pub layer_id: usize,
    pub t_props: Cow<'a, Vec<(usize, Prop)>>,
    pub c_props: Cow<'a, Vec<(usize, Prop)>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeID {
    pub gid: GID,
    pub vid: VID,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeTypeID {
    pub name: String,
    pub id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddConstPropID<'a> {
    pub name: Cow<'a, str>,
    pub id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddTemporalPropID<'a> {
    pub name: Cow<'a, str>,
    pub id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddLayerID {
    pub name: String,
    pub id: usize,
}

// Constructors
impl<'a> WalEntry<'a> {
    pub fn add_edge(
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        eid: EID,
        layer_id: usize,
        t_props: Cow<'a, Vec<(usize, Prop)>>,
        c_props: Cow<'a, Vec<(usize, Prop)>>,
    ) -> WalEntry<'a> {
        WalEntry::AddEdge(AddEdge {
            t,
            src,
            dst,
            eid,
            layer_id,
            t_props,
            c_props,
        })
    }

    pub fn add_node_id(gid: GID, vid: VID) -> WalEntry<'static> {
        WalEntry::AddNodeID(AddNodeID { gid, vid })
    }

    pub fn add_const_prop_ids(props: Vec<(Cow<'a, str>, usize)>) -> WalEntry<'a> {
        WalEntry::AddConstPropIDs(
            props.into_iter()
                .map(|(name, id)| AddConstPropID { name, id })
                .collect(),
        )
    }

    pub fn add_temporal_prop_ids(props: Vec<(Cow<'a, str>, usize)>) -> WalEntry<'a> {
        WalEntry::AddTemporalPropIDs(
            props.into_iter()
                .map(|(name, id)| AddTemporalPropID { name, id })
                .collect(),
        )
    }

    pub fn add_layer_id(name: String, id: usize) -> WalEntry<'static> {
        WalEntry::AddLayerID(AddLayerID { name, id })
    }
}

impl<'a> WalEntry<'a> {
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<WalEntry<'static>, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}
