use super::{Prop, DST_COL, LAYER_COL, NODE_ID, SRC_COL, TIME_COL, TYPE_COL};
use crate::{
    db::{
        api::{storage::graph::storage_ops::GraphStorage, view::StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};
use arrow_schema::DataType;
use raphtory_api::core::{
    entities::GidType,
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};
use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serialize,
};

pub(crate) struct ParquetProp<'a>(pub &'a Prop);

impl<'a> Serialize for ParquetProp<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Prop::I32(i) => serializer.serialize_i32(*i),
            Prop::I64(i) => serializer.serialize_i64(*i),
            Prop::F32(f) => serializer.serialize_f32(*f),
            Prop::F64(f) => serializer.serialize_f64(*f),
            Prop::U8(u) => serializer.serialize_u8(*u),
            Prop::U16(u) => serializer.serialize_u16(*u),
            Prop::U32(u) => serializer.serialize_u32(*u),
            Prop::U64(u) => serializer.serialize_u64(*u),
            Prop::Str(s) => serializer.serialize_str(s),
            Prop::Bool(b) => serializer.serialize_bool(*b),
            Prop::DTime(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            Prop::NDTime(dt) => serializer.serialize_i64(dt.and_utc().timestamp_millis()),
            Prop::List(l) => {
                let mut state = serializer.serialize_seq(Some(l.len()))?;
                for prop in l.iter() {
                    state.serialize_element(&ParquetProp(prop))?;
                }
                state.end()
            }
            Prop::Map(m) => {
                let mut state = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    state.serialize_entry(k, &ParquetProp(v))?;
                }
                state.end()
            }
            _ => todo!(),
        }
    }
}

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
        state.serialize_entry(SRC_COL, &ParquetGID(edge.src().id()))?;
        state.serialize_entry(DST_COL, &ParquetGID(edge.dst().id()))?;
        state.serialize_entry(LAYER_COL, &layer)?;

        for (name, prop) in edge.properties().temporal().iter_latest() {
            state.serialize_entry(&name, &ParquetProp(&prop))?;
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

        for (name, prop) in edge.properties().constant().iter() {
            state.serialize_entry(&name, &ParquetProp(&prop))?;
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

        state.serialize_entry(NODE_ID, &ParquetGID(self.node.id()))?;
        state.serialize_entry(TIME_COL, &self.t.0)?;
        state.serialize_entry(TYPE_COL, &self.node.node_type())?;

        for (name, prop) in self.props.iter() {
            state.serialize_entry(&self.cols[*name], &ParquetProp(&prop))?;
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

        state.serialize_entry(NODE_ID, &ParquetGID(self.node.id()))?;
        state.serialize_entry(TYPE_COL, &self.node.node_type())?;

        for (name, prop) in self.node.properties().constant().iter() {
            state.serialize_entry(&name, &ParquetProp(&prop))?;
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
