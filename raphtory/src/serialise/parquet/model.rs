use super::{
    Prop, DST_COL, LAYER_COL, NODE_ID_COL, SECONDARY_INDEX_COL, SRC_COL, TIME_COL, TYPE_COL,
};
use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};
use arrow::datatypes::DataType;
use raphtory_api::core::{
    entities::{properties::prop::SerdeArrowProp, GidType},
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};
use raphtory_storage::graph::graph::GraphStorage;
use serde::{
    ser::{Error, SerializeMap},
    Serialize,
};

#[derive(Debug)]
struct ParquetGID(GID);

impl Serialize for ParquetGID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0 {
            GID::U64(id) => serializer.serialize_u64(*id),
            GID::Str(id) => serializer.serialize_str(id),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ParquetTEdge<'a, G: StaticGraphViewOps>(pub(crate) EdgeView<&'a G>);

impl<'a, G: StaticGraphViewOps> Serialize for ParquetTEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.0;
        let mut state = serializer.serialize_map(None)?;
        let t = edge
            .edge
            .time()
            .ok_or(S::Error::custom("Edge has no time"))?;
        let layer = edge
            .layer_name()
            .map_err(|_| S::Error::custom("Edge has no layer"))?;

        state.serialize_entry(TIME_COL, &t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &t.1)?;
        state.serialize_entry(SRC_COL, &ParquetGID(edge.src().id()))?;
        state.serialize_entry(DST_COL, &ParquetGID(edge.dst().id()))?;
        state.serialize_entry(LAYER_COL, &layer)?;

        for (name, prop) in edge.properties().temporal().iter_latest() {
            state.serialize_entry(&name, &SerdeArrowProp(&prop))?;
        }

        state.end()
    }
}

#[derive(Debug)]
pub(crate) struct ParquetCEdge<'a, G: StaticGraphViewOps>(pub(crate) EdgeView<&'a G>);

impl<'a, G: StaticGraphViewOps> Serialize for ParquetCEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.0;
        let mut state = serializer.serialize_map(None)?;
        let layer = edge
            .layer_name()
            .map_err(|_| S::Error::custom("Edge has no layer"))?;

        state.serialize_entry(SRC_COL, &ParquetGID(edge.src().id()))?;
        state.serialize_entry(DST_COL, &ParquetGID(edge.dst().id()))?;
        state.serialize_entry(LAYER_COL, &layer)?;

        for (name, prop) in edge.metadata().iter_filtered() {
            state.serialize_entry(&name, &SerdeArrowProp(&prop))?;
        }

        state.end()
    }
}

pub(crate) struct ParquetDelEdge<'a, G> {
    pub layer: &'a str,
    pub edge: EdgeView<&'a G>,
    pub del: TimeIndexEntry,
}

impl<'a, G: StaticGraphViewOps> Serialize for ParquetDelEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.edge;
        let mut state = serializer.serialize_map(None)?;

        state.serialize_entry(TIME_COL, &self.del.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.del.1)?;
        state.serialize_entry(SRC_COL, &ParquetGID(edge.src().id()))?;
        state.serialize_entry(DST_COL, &ParquetGID(edge.dst().id()))?;
        state.serialize_entry(LAYER_COL, &self.layer)?;

        state.end()
    }
}

pub(crate) struct ParquetTNode<'a> {
    pub node: NodeView<'a, &'a GraphStorage>,
    pub cols: &'a [ArcStr],
    pub t: TimeIndexEntry,
    pub props: Vec<(usize, Prop)>,
}

impl<'a> Serialize for ParquetTNode<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        state.serialize_entry(NODE_ID_COL, &ParquetGID(self.node.id()))?;
        state.serialize_entry(TIME_COL, &self.t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.t.1)?;
        state.serialize_entry(TYPE_COL, &self.node.node_type())?;

        for (name, prop) in self.props.iter() {
            state.serialize_entry(&self.cols[*name], &SerdeArrowProp(prop))?;
        }

        state.end()
    }
}

pub(crate) struct ParquetCNode<'a> {
    pub node: NodeView<'a, &'a GraphStorage>,
}

impl<'a> Serialize for ParquetCNode<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        state.serialize_entry(NODE_ID_COL, &ParquetGID(self.node.id()))?;
        state.serialize_entry(TYPE_COL, &self.node.node_type())?;

        for (name, prop) in self.node.metadata().iter_filtered() {
            state.serialize_entry(&name, &SerdeArrowProp(&prop))?;
        }

        state.end()
    }
}

pub(crate) fn get_id_type(id_type: Option<GidType>) -> Result<DataType, DataType> {
    match id_type {
        Some(GidType::Str) => Ok(DataType::Utf8),
        Some(GidType::U64) => Ok(DataType::UInt64),
        None => Err(DataType::UInt64), // The graph is empty what now?
    }
}
