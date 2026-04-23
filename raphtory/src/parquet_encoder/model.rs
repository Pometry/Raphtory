use super::{
    Prop, DST_COL_ID, LAYER_COL, NODE_ID_COL, SECONDARY_INDEX_COL, SRC_COL_ID, TIME_COL, TYPE_COL,
};
use crate::{
    db::{
        api::view::internal::GraphView,
        graph::{edge::EdgeView, node::NodeView},
    },
    parquet_encoder::{
        DST_COL_VID, EDGE_COL_ID, LAYER_ID_COL, NODE_VID_COL, SRC_COL_VID, TYPE_ID_COL,
    },
    prelude::*,
};
use arrow::datatypes::DataType;
use raphtory_api::core::{
    entities::{
        properties::{prop::SerdeArrowProp, tprop::TPropOps},
        GidType, LayerId, EID,
    },
    storage::{
        arc_str::ArcStr,
        timeindex::{EventTime, TimeIndexOps},
    },
};
use raphtory_storage::graph::edges::edge_storage_ops::EdgeStorageOps;
use serde::{
    ser::{Error, SerializeMap},
    Serialize,
};

#[derive(Debug)]
struct ParquetGID<'a>(&'a GID);

impl Serialize for ParquetGID<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            GID::U64(id) => serializer.serialize_u64(*id),
            GID::Str(id) => serializer.serialize_str(id),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ParquetTEdge<'a, G: GraphView> {
    pub(crate) edge: EdgeView<&'a G>,
    pub(crate) export_src_vid: usize,
    pub(crate) export_src_id: GID,
    pub(crate) export_dst_vid: usize,
    pub(crate) export_dst_id: GID,
    pub(crate) export_eid: EID,
    pub(crate) export_layer_id: Option<usize>,
}

impl<'a, G: GraphView> Serialize for ParquetTEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.edge;
        let mut state = serializer.serialize_map(None)?;
        let t = edge
            .edge
            .time()
            .ok_or(S::Error::custom("Edge has no time"))?;
        let layer = edge
            .layer_name()
            .map_err(|_| S::Error::custom("Edge has no layer"))?;

        let layer_id = self
            .export_layer_id
            .ok_or_else(|| S::Error::custom("Edge has no layer"))?;

        state.serialize_entry(TIME_COL, &t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &t.1)?;
        state.serialize_entry(SRC_COL_VID, &self.export_src_vid)?;
        state.serialize_entry(SRC_COL_ID, &ParquetGID(&self.export_src_id))?;
        state.serialize_entry(DST_COL_VID, &self.export_dst_vid)?;
        state.serialize_entry(DST_COL_ID, &ParquetGID(&self.export_dst_id))?;
        state.serialize_entry(EDGE_COL_ID, &self.export_eid)?;
        state.serialize_entry(LAYER_COL, &layer)?;
        state.serialize_entry(LAYER_ID_COL, &layer_id)?;

        // Emit temporal props for this exploded event. Two cases:
        //   * real addition at exactly `t` in storage — use `at(t)` so we only
        //     emit props actually set at that time (additions without new prop
        //     values should not inherit persisted values, otherwise later
        //     reloads see spurious duplicates).
        //   * synthetic event (windowed persistent graph emits an event at
        //     `w.start` for edges that persist into the window) — fall back to
        //     the view's persistent semantics via `latest` to emit the
        //     persisted prop values.
        let core = edge.graph.core_edge(edge.edge.pid());
        let core_ref = core.as_ref();
        let layer = LayerId(layer_id);
        let t_next = EventTime::start(t.0.saturating_add(1));
        let has_real_addition = core_ref.additions(layer).active(t..t_next);
        let mapper = edge.graph.edge_meta().temporal_prop_mapper();
        let temporal = edge.properties().temporal();
        for prop_id in mapper.ids() {
            let prop = if has_real_addition {
                core_ref.temporal_prop_layer(layer, prop_id).at(&t)
            } else {
                temporal.get_by_id(prop_id).and_then(|v| v.latest())
            };
            if let Some(prop) = prop {
                let name = mapper.get_name(prop_id);
                state.serialize_entry(&name, &SerdeArrowProp(&prop))?;
            }
        }

        state.end()
    }
}

#[derive(Debug)]
pub(crate) struct ParquetCEdge<'a, G: GraphView> {
    pub(crate) edge: EdgeView<&'a G>,
    pub(crate) export_src_vid: usize,
    pub(crate) export_src_id: GID,
    pub(crate) export_dst_vid: usize,
    pub(crate) export_dst_id: GID,
    pub(crate) export_eid: usize,
}

impl<'a, G: GraphView> Serialize for ParquetCEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let edge = &self.edge;
        let mut state = serializer.serialize_map(None)?;
        let layer = edge
            .layer_name()
            .map_err(|_| S::Error::custom("Edge has no layer"))?;

        state.serialize_entry(SRC_COL_VID, &self.export_src_vid)?;
        state.serialize_entry(SRC_COL_ID, &ParquetGID(&self.export_src_id))?;
        state.serialize_entry(DST_COL_VID, &self.export_dst_vid)?;
        state.serialize_entry(DST_COL_ID, &ParquetGID(&self.export_dst_id))?;
        state.serialize_entry(EDGE_COL_ID, &self.export_eid)?;
        state.serialize_entry(LAYER_COL, &layer)?;

        for (name, prop) in edge.metadata().iter_filtered() {
            state.serialize_entry(&name, &SerdeArrowProp(&prop))?;
        }

        state.end()
    }
}

pub(crate) struct ParquetDelEdge<'a, G: GraphView> {
    pub(crate) edge: EdgeView<&'a G>,
    pub(crate) del: EventTime,
    pub(crate) export_src_vid: usize,
    pub(crate) export_src_id: GID,
    pub(crate) export_dst_vid: usize,
    pub(crate) export_dst_id: GID,
    pub(crate) export_eid: usize,
    pub(crate) export_layer_id: Option<usize>,
}

impl<'a, G: GraphView> Serialize for ParquetDelEdge<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        let layer_id = self
            .export_layer_id
            .ok_or_else(|| S::Error::custom("Edge has no layer"))?;
        let layer = self
            .edge
            .layer_name()
            .map_err(|_| S::Error::custom("Edge has no layer"))?;

        state.serialize_entry(TIME_COL, &self.del.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.del.1)?;
        state.serialize_entry(SRC_COL_VID, &self.export_src_vid)?;
        state.serialize_entry(SRC_COL_ID, &ParquetGID(&self.export_src_id))?;
        state.serialize_entry(DST_COL_VID, &self.export_dst_vid)?;
        state.serialize_entry(DST_COL_ID, &ParquetGID(&self.export_dst_id))?;
        state.serialize_entry(EDGE_COL_ID, &self.export_eid)?;
        state.serialize_entry(LAYER_COL, &layer)?;
        state.serialize_entry(LAYER_ID_COL, &layer_id)?;

        state.end()
    }
}

pub(crate) struct ParquetTNode<'a> {
    pub export_id: GID,
    pub export_vid: usize,
    pub export_node_type: Option<ArcStr>,
    pub cols: &'a [ArcStr],
    pub t: EventTime,
    pub props: Vec<(usize, Prop)>,
}

impl<'a> Serialize for ParquetTNode<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        state.serialize_entry(NODE_ID_COL, &ParquetGID(&self.export_id))?;
        state.serialize_entry(NODE_VID_COL, &self.export_vid)?;
        state.serialize_entry(TYPE_COL, &self.export_node_type)?;
        state.serialize_entry(TIME_COL, &self.t.0)?;
        state.serialize_entry(SECONDARY_INDEX_COL, &self.t.1)?;

        for (name, prop) in self.props.iter() {
            state.serialize_entry(&self.cols[*name], &SerdeArrowProp(prop))?;
        }

        state.end()
    }
}

pub(crate) struct ParquetCNode<'a, G: GraphView> {
    pub node: NodeView<'a, &'a G>,
    pub export_vid: usize,
    pub export_node_type_id: usize,
}

impl<'a, G: GraphView> Serialize for ParquetCNode<'a, G> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        state.serialize_entry(NODE_ID_COL, &ParquetGID(&self.node.id()))?;
        state.serialize_entry(NODE_VID_COL, &self.export_vid)?;
        state.serialize_entry(TYPE_COL, &self.node.node_type())?;
        state.serialize_entry(TYPE_ID_COL, &self.export_node_type_id)?;

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
