use crate::{
    core::{utils::errors::GraphError, DocumentInput, Lifespan, Prop, PropType},
    db::graph::views::deletion_graph::PersistentGraph,
    prelude::{Graph, StableDecode, StableEncode},
    serialise::{
        proto,
        proto::{
            graph_update::{
                DelEdge, PropPair, Update, UpdateEdgeCProps, UpdateEdgeTProps, UpdateGraphCProps,
                UpdateGraphTProps, UpdateNodeCProps, UpdateNodeTProps, UpdateNodeType,
            },
            new_meta::{
                Meta, NewEdgeCProp, NewEdgeTProp, NewGraphCProp, NewGraphTProp, NewLayer,
                NewNodeCProp, NewNodeTProp, NewNodeType,
            },
            new_node, prop,
            prop_type::PropType as SPropType,
            GraphUpdate, NewEdge, NewMeta, NewNode,
        },
    },
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use raphtory_api::core::{
    entities::{GidRef, EID, VID},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, TimeIndexEntry},
    },
};
use std::{borrow::Borrow, sync::Arc};

fn as_proto_prop_type(p_type: &PropType) -> SPropType {
    match p_type {
        PropType::Str => SPropType::Str,
        PropType::U8 => SPropType::U8,
        PropType::U16 => SPropType::U16,
        PropType::U32 => SPropType::U32,
        PropType::I32 => SPropType::I32,
        PropType::I64 => SPropType::I64,
        PropType::U64 => SPropType::U64,
        PropType::F32 => SPropType::F32,
        PropType::F64 => SPropType::F64,
        PropType::Bool => SPropType::Bool,
        PropType::List => SPropType::List,
        PropType::Map => SPropType::Map,
        PropType::NDTime => SPropType::NdTime,
        PropType::DTime => SPropType::DTime,
        PropType::Graph => SPropType::Graph,
        PropType::PersistentGraph => SPropType::PersistentGraph,
        PropType::Document => SPropType::Document,
        _ => unimplemented!("Empty prop types not supported!"),
    }
}

pub fn as_prop_type(p_type: SPropType) -> PropType {
    match p_type {
        SPropType::Str => PropType::Str,
        SPropType::U8 => PropType::U8,
        SPropType::U16 => PropType::U16,
        SPropType::U32 => PropType::U32,
        SPropType::I32 => PropType::I32,
        SPropType::I64 => PropType::I64,
        SPropType::U64 => PropType::U64,
        SPropType::F32 => PropType::F32,
        SPropType::F64 => PropType::F64,
        SPropType::Bool => PropType::Bool,
        SPropType::List => PropType::List,
        SPropType::Map => PropType::Map,
        SPropType::NdTime => PropType::NDTime,
        SPropType::DTime => PropType::DTime,
        SPropType::Graph => PropType::Graph,
        SPropType::PersistentGraph => PropType::PersistentGraph,
        SPropType::Document => PropType::Document,
    }
}

impl NewEdge {
    pub fn src(&self) -> VID {
        VID(self.src as usize)
    }

    pub fn dst(&self) -> VID {
        VID(self.dst as usize)
    }

    pub fn eid(&self) -> EID {
        EID(self.eid as usize)
    }
}

impl DelEdge {
    pub fn eid(&self) -> EID {
        EID(self.eid as usize)
    }

    pub fn layer_id(&self) -> usize {
        self.layer_id as usize
    }

    pub fn time(&self) -> TimeIndexEntry {
        TimeIndexEntry(self.time, self.secondary as usize)
    }
}

impl UpdateEdgeCProps {
    pub fn eid(&self) -> EID {
        EID(self.eid as usize)
    }

    pub fn layer_id(&self) -> usize {
        self.layer_id as usize
    }

    pub fn props(&self) -> impl Iterator<Item = Result<(usize, Prop), GraphError>> + '_ {
        self.properties.iter().map(as_prop)
    }
}

impl UpdateEdgeTProps {
    pub fn eid(&self) -> EID {
        EID(self.eid as usize)
    }

    pub fn layer_id(&self) -> usize {
        self.layer_id as usize
    }

    pub fn time(&self) -> TimeIndexEntry {
        TimeIndexEntry(self.time, self.secondary as usize)
    }

    pub fn has_props(&self) -> bool {
        !self.properties.is_empty()
    }

    pub fn props(&self) -> impl Iterator<Item = Result<(usize, Prop), GraphError>> + '_ {
        self.properties.iter().map(as_prop)
    }
}

impl UpdateNodeType {
    pub fn vid(&self) -> VID {
        VID(self.id as usize)
    }

    pub fn type_id(&self) -> usize {
        self.type_id as usize
    }
}

impl UpdateNodeCProps {
    pub fn vid(&self) -> VID {
        VID(self.id as usize)
    }

    pub fn props(&self) -> impl Iterator<Item = Result<(usize, Prop), GraphError>> + '_ {
        self.properties.iter().map(as_prop)
    }
}

impl UpdateNodeTProps {
    pub fn vid(&self) -> VID {
        VID(self.id as usize)
    }

    pub fn time(&self) -> TimeIndexEntry {
        TimeIndexEntry(self.time, self.secondary as usize)
    }

    pub fn props(&self) -> impl Iterator<Item = Result<(usize, Prop), GraphError>> + '_ {
        self.properties.iter().map(as_prop)
    }
}

impl NewMeta {
    fn new(new_meta: Meta) -> Self {
        Self {
            meta: Some(new_meta),
        }
    }

    fn new_graph_cprop(key: &str, id: usize) -> Self {
        let inner = NewGraphCProp {
            name: key.to_string(),
            id: id as u64,
        };
        Self::new(Meta::NewGraphCprop(inner))
    }

    fn new_graph_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewGraphTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewGraphTprop(inner))
    }

    fn new_node_cprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewNodeCProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewNodeCprop(inner))
    }

    fn new_node_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewNodeTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewNodeTprop(inner))
    }

    fn new_edge_cprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewEdgeCProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewEdgeCprop(inner))
    }

    fn new_edge_tprop(key: &str, id: usize, dtype: &PropType) -> Self {
        let mut inner = NewEdgeTProp::default();
        inner.name = key.to_string();
        inner.id = id as u64;
        inner.set_p_type(as_proto_prop_type(dtype));
        Self::new(Meta::NewEdgeTprop(inner))
    }

    fn new_layer(layer: &str, id: usize) -> Self {
        let mut inner = NewLayer::default();
        inner.name = layer.to_string();
        inner.id = id as u64;
        Self::new(Meta::NewLayer(inner))
    }

    fn new_node_type(node_type: &str, id: usize) -> Self {
        let mut inner = NewNodeType::default();
        inner.name = node_type.to_string();
        inner.id = id as u64;
        Self::new(Meta::NewNodeType(inner))
    }
}

impl GraphUpdate {
    fn new(update: Update) -> Self {
        Self {
            update: Some(update),
        }
    }

    fn update_graph_cprops(values: impl Iterator<Item = (usize, impl Borrow<Prop>)>) -> Self {
        let inner = UpdateGraphCProps::new(values);
        Self::new(Update::UpdateGraphCprops(inner))
    }

    fn update_graph_tprops(
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let inner = UpdateGraphTProps::new(time, values);
        Self::new(Update::UpdateGraphTprops(inner))
    }

    fn update_node_type(node_id: VID, type_id: usize) -> Self {
        let inner = UpdateNodeType {
            id: node_id.as_u64(),
            type_id: type_id as u64,
        };
        Self::new(Update::UpdateNodeType(inner))
    }

    fn update_node_cprops(
        node_id: VID,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateNodeCProps {
            id: node_id.as_u64(),
            properties,
        };
        Self::new(Update::UpdateNodeCprops(inner))
    }

    fn update_node_tprops(
        node_id: VID,
        time: TimeIndexEntry,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateNodeTProps {
            id: node_id.as_u64(),
            time: time.t(),
            secondary: time.i() as u64,
            properties,
        };
        Self::new(Update::UpdateNodeTprops(inner))
    }

    fn update_edge_tprops(
        eid: EID,
        time: TimeIndexEntry,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateEdgeTProps {
            eid: eid.0 as u64,
            time: time.t(),
            secondary: time.i() as u64,
            layer_id: layer_id as u64,
            properties,
        };
        Self::new(Update::UpdateEdgeTprops(inner))
    }

    fn update_edge_cprops(
        eid: EID,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(properties);
        let inner = UpdateEdgeCProps {
            eid: eid.0 as u64,
            layer_id: layer_id as u64,
            properties,
        };
        Self::new(Update::UpdateEdgeCprops(inner))
    }

    fn del_edge(eid: EID, layer_id: usize, time: TimeIndexEntry) -> Self {
        let inner = DelEdge {
            eid: eid.as_u64(),
            time: time.t(),
            secondary: time.i() as u64,
            layer_id: layer_id as u64,
        };
        Self::new(Update::DelEdge(inner))
    }
}

impl UpdateGraphCProps {
    fn new(values: impl Iterator<Item = (usize, impl Borrow<Prop>)>) -> Self {
        let properties = collect_proto_props(values);
        UpdateGraphCProps { properties }
    }
}

impl UpdateGraphTProps {
    fn new(
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) -> Self {
        let properties = collect_proto_props(values);
        UpdateGraphTProps {
            time: time.t(),
            secondary: time.i() as u64,
            properties,
        }
    }
}

impl PropPair {
    fn new(key: usize, value: &Prop) -> Self {
        PropPair {
            key: key as u64,
            value: Some(as_proto_prop(value)),
        }
    }
}

impl proto::Graph {
    pub fn new_edge(&mut self, src: VID, dst: VID, eid: EID) {
        let edge = NewEdge {
            src: src.as_u64(),
            dst: dst.as_u64(),
            eid: eid.as_u64(),
        };
        self.edges.push(edge);
    }

    pub fn new_node(&mut self, gid: GidRef, vid: VID, type_id: usize) {
        let type_id = type_id as u64;
        let gid = match gid {
            GidRef::U64(id) => new_node::Gid::GidU64(id),
            GidRef::Str(name) => new_node::Gid::GidStr(name.to_string()),
        };
        let node = NewNode {
            type_id,
            gid: Some(gid),
            vid: vid.as_u64(),
        };
        self.nodes.push(node);
    }

    pub fn new_graph_cprop(&mut self, key: &str, id: usize) {
        self.metas.push(NewMeta::new_graph_cprop(key, id));
    }

    pub fn new_graph_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_graph_tprop(key, id, dtype));
    }

    pub fn new_node_cprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_node_cprop(key, id, dtype));
    }

    pub fn new_node_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_node_tprop(key, id, dtype));
    }

    pub fn new_edge_cprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_edge_cprop(key, id, dtype));
    }

    pub fn new_edge_tprop(&mut self, key: &str, id: usize, dtype: &PropType) {
        self.metas.push(NewMeta::new_edge_tprop(key, id, dtype))
    }

    pub fn new_layer(&mut self, layer: &str, id: usize) {
        self.metas.push(NewMeta::new_layer(layer, id));
    }

    pub fn new_node_type(&mut self, node_type: &str, id: usize) {
        self.metas.push(NewMeta::new_node_type(node_type, id));
    }

    pub fn update_graph_cprops(
        &mut self,
        values: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates.push(GraphUpdate::update_graph_cprops(values));
    }

    pub fn update_graph_tprops(
        &mut self,
        time: TimeIndexEntry,
        values: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_graph_tprops(time, values));
    }

    pub fn update_node_type(&mut self, node_id: VID, type_id: usize) {
        self.updates
            .push(GraphUpdate::update_node_type(node_id, type_id))
    }
    pub fn update_node_cprops(
        &mut self,
        node_id: VID,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_node_cprops(node_id, properties));
    }

    pub fn update_node_tprops(
        &mut self,
        node_id: VID,
        time: TimeIndexEntry,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_node_tprops(node_id, time, properties));
    }

    pub fn update_edge_tprops(
        &mut self,
        eid: EID,
        time: TimeIndexEntry,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates.push(GraphUpdate::update_edge_tprops(
            eid, time, layer_id, properties,
        ));
    }

    pub fn update_edge_cprops(
        &mut self,
        eid: EID,
        layer_id: usize,
        properties: impl Iterator<Item = (usize, impl Borrow<Prop>)>,
    ) {
        self.updates
            .push(GraphUpdate::update_edge_cprops(eid, layer_id, properties));
    }

    pub fn del_edge(&mut self, eid: EID, layer_id: usize, time: TimeIndexEntry) {
        self.updates
            .push(GraphUpdate::del_edge(eid, layer_id, time))
    }
}

fn as_prop(prop_pair: &PropPair) -> Result<(usize, Prop), GraphError> {
    let PropPair { key, value } = prop_pair;
    let value = value.as_ref().expect("Missing prop value");
    let value = value.value.as_ref();
    let value = as_prop_value(value)?;

    Ok((*key as usize, value))
}

fn as_prop_value(value: Option<&prop::Value>) -> Result<Prop, GraphError> {
    let value = match value.expect("Missing prop value") {
        prop::Value::BoolValue(b) => Prop::Bool(*b),
        prop::Value::U8(u) => Prop::U8((*u).try_into().unwrap()),
        prop::Value::U16(u) => Prop::U16((*u).try_into().unwrap()),
        prop::Value::U32(u) => Prop::U32(*u),
        prop::Value::I32(i) => Prop::I32(*i),
        prop::Value::I64(i) => Prop::I64(*i),
        prop::Value::U64(u) => Prop::U64(*u),
        prop::Value::F32(f) => Prop::F32(*f),
        prop::Value::F64(f) => Prop::F64(*f),
        prop::Value::Str(s) => Prop::Str(ArcStr::from(s.as_str())),
        prop::Value::Prop(props) => Prop::List(Arc::new(
            props
                .properties
                .iter()
                .map(|prop| as_prop_value(prop.value.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        prop::Value::Map(dict) => Prop::Map(Arc::new(
            dict.map
                .iter()
                .map(|(k, v)| Ok((ArcStr::from(k.as_str()), as_prop_value(v.value.as_ref())?)))
                .collect::<Result<_, GraphError>>()?,
        )),
        prop::Value::NdTime(ndt) => {
            let prop::NdTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanos,
            } = ndt;
            let ndt = NaiveDateTime::new(
                NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32).unwrap(),
                NaiveTime::from_hms_nano_opt(
                    *hour as u32,
                    *minute as u32,
                    *second as u32,
                    *nanos as u32,
                )
                .unwrap(),
            );
            Prop::NDTime(ndt)
        }
        prop::Value::DTime(dt) => Prop::DTime(DateTime::parse_from_rfc3339(dt).unwrap().into()),
        prop::Value::Graph(graph_proto) => Prop::Graph(Graph::decode_from_proto(graph_proto)?),
        prop::Value::PersistentGraph(graph_proto) => {
            Prop::PersistentGraph(PersistentGraph::decode_from_proto(graph_proto)?)
        }
        prop::Value::DocumentInput(doc) => Prop::Document(DocumentInput {
            content: doc.content.clone(),
            life: doc
                .life
                .as_ref()
                .map(|l| match l.l_type {
                    Some(prop::lifespan::LType::Interval(prop::lifespan::Interval {
                        start,
                        end,
                    })) => Lifespan::Interval { start, end },
                    Some(prop::lifespan::LType::Event(prop::lifespan::Event { time })) => {
                        Lifespan::Event { time }
                    }
                    None => Lifespan::Inherited,
                })
                .unwrap_or(Lifespan::Inherited),
        }),
    };
    Ok(value)
}

fn collect_proto_props(
    iter: impl IntoIterator<Item = (usize, impl Borrow<Prop>)>,
) -> Vec<PropPair> {
    iter.into_iter()
        .map(|(key, value)| PropPair::new(key, value.borrow()))
        .collect()
}

pub fn collect_props<'a>(
    iter: impl IntoIterator<Item = &'a PropPair>,
) -> Result<Vec<(usize, Prop)>, GraphError> {
    iter.into_iter().map(as_prop).collect()
}

fn as_proto_prop(prop: &Prop) -> proto::Prop {
    let value: prop::Value = match prop {
        Prop::Bool(b) => prop::Value::BoolValue(*b),
        Prop::U8(u) => prop::Value::U8((*u).into()),
        Prop::U16(u) => prop::Value::U16((*u).into()),
        Prop::U32(u) => prop::Value::U32(*u),
        Prop::I32(i) => prop::Value::I32(*i),
        Prop::I64(i) => prop::Value::I64(*i),
        Prop::U64(u) => prop::Value::U64(*u),
        Prop::F32(f) => prop::Value::F32(*f),
        Prop::F64(f) => prop::Value::F64(*f),
        Prop::Str(s) => prop::Value::Str(s.to_string()),
        Prop::List(list) => {
            let properties = list.iter().map(as_proto_prop).collect();
            prop::Value::Prop(prop::Props { properties })
        }
        Prop::Map(map) => {
            let map = map
                .iter()
                .map(|(k, v)| (k.to_string(), as_proto_prop(v)))
                .collect();
            prop::Value::Map(prop::Dict { map })
        }
        Prop::NDTime(ndt) => {
            let (year, month, day) = (ndt.date().year(), ndt.date().month(), ndt.date().day());
            let (hour, minute, second, nanos) = (
                ndt.time().hour(),
                ndt.time().minute(),
                ndt.time().second(),
                ndt.time().nanosecond(),
            );

            let proto_ndt = prop::NdTime {
                year: year as u32,
                month: month as u32,
                day: day as u32,
                hour: hour as u32,
                minute: minute as u32,
                second: second as u32,
                nanos: nanos as u32,
            };
            prop::Value::NdTime(proto_ndt)
        }
        Prop::DTime(dt) => {
            prop::Value::DTime(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Prop::Graph(g) => prop::Value::Graph(g.encode_to_proto()),
        Prop::PersistentGraph(g) => prop::Value::PersistentGraph(g.encode_to_proto()),
        Prop::Document(doc) => {
            let life = match doc.life {
                Lifespan::Interval { start, end } => {
                    Some(prop::lifespan::LType::Interval(prop::lifespan::Interval {
                        start,
                        end,
                    }))
                }
                Lifespan::Event { time } => {
                    Some(prop::lifespan::LType::Event(prop::lifespan::Event { time }))
                }
                Lifespan::Inherited => None,
            };
            prop::Value::DocumentInput(prop::DocumentInput {
                content: doc.content.clone(),
                life: Some(prop::Lifespan { l_type: life }),
            })
        }
    };

    proto::Prop { value: Some(value) }
}
