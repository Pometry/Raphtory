use std::{fs::File, io::Write, path::Path, sync::Arc};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use prost::Message;
use raphtory_api::core::{
    entities::VID,
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};

use crate::{
    core::{
        entities::{properties::props::PropMapper, LayerIds},
        utils::errors::GraphError,
        DocumentInput, Lifespan, PropType,
    },
    db::{
        api::{
            mutation::internal::{
                DelegatePropertyAdditionOps, InternalAdditionOps, InternalPropertyAdditionOps,
            },
            storage::nodes::node_storage_ops::NodeStorageOps,
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    prelude::*,
    serialise::{
        self,
        graph::{
            properties_meta::{self, PropName},
            AddEdge, AddNode, GraphConstProps, Node, PropPair, UpdateEdgeConstProps,
        },
        lifespan, prop, Dict, NdTime,
    },
};

use super::GraphViewOps;

pub trait StableEncoder {
    fn encode_to_vec(&self) -> Result<Vec<u8>, GraphError>;

    fn stable_serialise(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let mut file = File::create(path)?;
        let bytes = self.encode_to_vec()?;
        file.write_all(&bytes)?;

        Ok(())
    }
}

pub trait StableDecode {
    fn decode_from_bytes(bytes: &[u8], g: &Self) -> Result<(), GraphError>;
    fn decode(path: impl AsRef<Path>, g: &Self) -> Result<(), GraphError> {
        let file = File::open(path)?;
        let buf = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let bytes = buf.as_ref();
        Self::decode_from_bytes(bytes, g)
    }
}

fn as_proto_prop_type(p_type: &PropType) -> properties_meta::PropType {
    match p_type {
        PropType::Str => properties_meta::PropType::Str,
        PropType::U8 => properties_meta::PropType::U8,
        PropType::U16 => properties_meta::PropType::U16,
        PropType::U32 => properties_meta::PropType::U32,
        PropType::I32 => properties_meta::PropType::I32,
        PropType::I64 => properties_meta::PropType::I64,
        PropType::U64 => properties_meta::PropType::U64,
        PropType::F32 => properties_meta::PropType::F32,
        PropType::F64 => properties_meta::PropType::F64,
        PropType::Bool => properties_meta::PropType::Bool,
        PropType::List => properties_meta::PropType::List,
        PropType::Map => properties_meta::PropType::Map,
        PropType::NDTime => properties_meta::PropType::NdTime,
        PropType::DTime => properties_meta::PropType::DTime,
        PropType::Graph => properties_meta::PropType::Graph,
        PropType::PersistentGraph => properties_meta::PropType::PersistentGraph,
        PropType::Document => properties_meta::PropType::Document,
        _ => unimplemented!("Empty prop types not supported!"),
    }
}

fn as_prop_type(p_type: properties_meta::PropType) -> PropType {
    match p_type {
        properties_meta::PropType::Str => PropType::Str,
        properties_meta::PropType::U8 => PropType::U8,
        properties_meta::PropType::U16 => PropType::U16,
        properties_meta::PropType::U32 => PropType::U32,
        properties_meta::PropType::I32 => PropType::I32,
        properties_meta::PropType::I64 => PropType::I64,
        properties_meta::PropType::U64 => PropType::U64,
        properties_meta::PropType::F32 => PropType::F32,
        properties_meta::PropType::F64 => PropType::F64,
        properties_meta::PropType::Bool => PropType::Bool,
        properties_meta::PropType::List => PropType::List,
        properties_meta::PropType::Map => PropType::Map,
        properties_meta::PropType::NdTime => PropType::NDTime,
        properties_meta::PropType::DTime => PropType::DTime,
        properties_meta::PropType::Graph => PropType::Graph,
        properties_meta::PropType::PersistentGraph => PropType::PersistentGraph,
        properties_meta::PropType::Document => PropType::Document,
    }
}

fn collect_prop_names<'a>(
    names: impl Iterator<Item = &'a ArcStr>,
    prop_mapper: &'a PropMapper,
) -> Vec<PropName> {
    names
        .enumerate()
        .map(|(prop_id, name)| {
            let prop_type = prop_mapper
                .get_dtype(prop_id)
                .expect("Failed to get prop type");
            PropName {
                name: name.to_string(),
                p_type: as_proto_prop_type(&prop_type).into(),
            }
        })
        .collect()
}

impl<'graph, G: GraphViewOps<'graph>> StableEncoder for G {
    fn encode_to_vec(&self) -> Result<Vec<u8>, GraphError> {
        let mut graph = serialise::Graph::default();

        // const graph properties
        let (names, properties): (Vec<_>, Vec<_>) = self
            .const_prop_ids()
            .filter_map(|id| {
                let prop = self.get_const_prop(id)?;
                let prop_name = self.get_const_prop_name(id);
                Some((
                    prop_name.to_string(),
                    PropPair {
                        key: id as u64,
                        value: Some(as_proto_prop(&prop).expect("Failed to convert prop")),
                    },
                ))
            })
            .unzip();

        graph.const_properties = Some(GraphConstProps { names, properties });

        // temporal graph properties
        let prop_names = self
            .temporal_prop_keys()
            .into_iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>();

        let (ts, props): (Vec<_>, Vec<_>) = self
            .temporal_prop_ids()
            .flat_map(|id| {
                let prop_t = self.temporal_history(id);
                let props = self.temporal_values(id);
                props.into_iter().zip(prop_t).map(move |(prop, t)| {
                    let prop = as_proto_prop(&prop).expect("Failed to convert prop");
                    (
                        t,
                        PropPair {
                            key: id as u64,
                            value: Some(prop),
                        },
                    )
                })
            })
            .unzip();

        graph.temp_properties = Some(serialise::graph::GraphTempProps {
            names: prop_names,
            times: ts,
            properties: props,
        });

        graph.layers = self
            .unique_layers()
            .map(|l_name| l_name.to_string())
            .collect();
        graph.node_types = self
            .get_all_node_types()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let n_const_meta = &self.node_meta().const_prop_meta();
        let n_temporal_meta = &self.node_meta().temporal_prop_meta();
        let e_const_meta = &self.edge_meta().const_prop_meta();
        let e_temporal_meta = &self.edge_meta().temporal_prop_meta();

        graph.meta = Some(serialise::graph::PropertiesMeta {
            nodes: Some(properties_meta::PropNames {
                constant: collect_prop_names(n_const_meta.get_keys().iter(), n_const_meta),
                temporal: collect_prop_names(n_temporal_meta.get_keys().iter(), n_temporal_meta),
            }),
            edges: Some(properties_meta::PropNames {
                constant: collect_prop_names(e_const_meta.get_keys().iter(), e_const_meta),
                temporal: collect_prop_names(e_temporal_meta.get_keys().iter(), e_temporal_meta),
            }),
        });

        let serialise::Graph {
            ref mut add_nodes,
            ref mut const_nodes_props,
            ref mut const_edges_props,
            ref mut nodes,
            ref mut edges,
            ..
        } = graph;

        *nodes = self
            .nodes()
            .into_iter()
            .map(|n| {
                let gid = n.id();
                let vid = n.node;
                let node = self.core_node_entry(vid);
                let name = node.as_ref().name().map(|n| n.to_string());
                Node {
                    gid,
                    vid: vid.0 as u64,
                    name,
                }
            })
            .collect::<Vec<_>>();

        for v in self.nodes().iter() {
            let type_id = Some(v.node_type_id() as u64);
            let id = v.node.0 as u64;

            for time in v.history() {
                add_nodes.push(AddNode {
                    id,
                    properties: None,
                    type_id,
                    time,
                });
            }

            for (prop_name, prop_view) in v.properties().temporal().iter() {
                for (time, prop) in prop_view.iter() {
                    let key = self
                        .node_meta()
                        .temporal_prop_meta()
                        .get_id(&prop_name)
                        .unwrap();
                    add_nodes.push(AddNode {
                        id,
                        properties: Some(as_prop_pair(key as u64, &prop)?),
                        type_id,
                        time,
                    });
                }
            }

            for (prop_name, prop) in v.properties().constant() {
                let key = self
                    .node_meta()
                    .const_prop_meta()
                    .get_id(&prop_name)
                    .unwrap();
                const_nodes_props.push(serialise::graph::UpdateNodeConstProps {
                    id,
                    properties: Some(as_prop_pair(key as u64, &prop)?),
                });
            }
        }

        for e in self.edges() {
            let src = e.src().node.0 as u64;
            let dst = e.dst().node.0 as u64;
            // FIXME: this needs to be verified
            for ee in e.explode_layers() {
                let layer_id = *ee.edge.layer().expect("exploded layers");

                for (prop_name, prop) in ee.properties().constant() {
                    let key = self
                        .edge_meta()
                        .const_prop_meta()
                        .get_id(&prop_name)
                        .unwrap();
                    const_edges_props.push(serialise::graph::UpdateEdgeConstProps {
                        src,
                        dst,
                        layer_id: layer_id as u64,
                        properties: Some(as_prop_pair(key as u64, &prop)?),
                    });
                }

                for ee in ee.explode() {
                    edges.push(AddEdge {
                        src,
                        dst,
                        properties: None,
                        time: ee.time().expect("exploded edge"),
                        layer_id: Some(layer_id as u64),
                    });

                    for (prop_name, prop_view) in ee.properties().temporal() {
                        for (time, prop) in prop_view.iter() {
                            let key = self
                                .edge_meta()
                                .temporal_prop_meta()
                                .get_id(&prop_name)
                                .unwrap();
                            edges.push(AddEdge {
                                src,
                                dst,
                                properties: Some(as_prop_pair(key as u64, &prop)?),
                                time,
                                layer_id: Some(layer_id as u64),
                            });
                        }
                    }
                }
            }
        }

        Ok(graph.encode_to_vec())
    }
}

impl<'graph, G: InternalAdditionOps + GraphViewOps<'graph> + DelegatePropertyAdditionOps>
    StableDecode for G
{
    fn decode_from_bytes(buf: &[u8], graph: &Self) -> Result<(), GraphError> {
        let g = serialise::Graph::decode(&buf[..]).expect("Failed to decode graph");

        // constant graph properties
        if let Some(meta) = g.const_properties.as_ref() {
            for (name, prop_pair) in meta.names.iter().zip(meta.properties.iter()) {
                let id = graph.graph_meta().resolve_property(name, true);
                assert_eq!(id, prop_pair.key as usize);

                let prop = prop_pair.value.as_ref().and_then(|p| p.value.as_ref());
                let prop = as_prop_value(prop);
                graph.graph_meta().add_constant_prop(id, prop)?;
            }
        }

        if let Some(meta) = g.temp_properties.as_ref() {
            for name in meta.names.iter() {
                graph.graph_meta().resolve_property(name, false);
            }

            for (time, prop_pair) in meta.times.iter().zip(meta.properties.iter()) {
                let id = prop_pair.key as usize;
                let prop = prop_pair.value.as_ref().and_then(|p| p.value.as_ref());
                let prop = as_prop_value(prop);
                graph
                    .graph_meta()
                    .add_prop(TimeIndexEntry::from(*time), id, prop)?;
            }
        }

        // align the nodes
        for node in g.nodes {
            let l_vid = graph.resolve_node(node.gid, node.name.as_deref());
            assert_eq!(l_vid, VID(node.vid as usize));
        }

        // align the node types
        for (type_id, type_name) in g.node_types.iter().enumerate() {
            let n_id = graph.node_meta().get_or_create_node_type_id(type_name);
            assert_eq!(n_id, type_id);
        }

        // alight the edge layers
        for (layer_id, layer) in g.layers.iter().enumerate() {
            let l_id = graph.resolve_layer(Some(layer));
            assert_eq!(l_id, layer_id);
        }

        // align the node properties
        if let Some(meta) = g.meta.as_ref().and_then(|m| m.nodes.as_ref()) {
            for PropName { name, p_type } in &meta.constant {
                let p_type = properties_meta::PropType::try_from(*p_type).unwrap();
                graph
                    .node_meta()
                    .resolve_prop_id(&name, as_prop_type(p_type), true)?;
            }

            for PropName { name, p_type } in &meta.temporal {
                let p_type = properties_meta::PropType::try_from(*p_type).unwrap();
                graph
                    .node_meta()
                    .resolve_prop_id(&name, as_prop_type(p_type), false)?;
            }
        }

        // align the edge properties

        if let Some(meta) = g.meta.as_ref().and_then(|m| m.edges.as_ref()) {
            for PropName { name, p_type } in &meta.constant {
                let p_type = properties_meta::PropType::try_from(*p_type).unwrap();
                graph
                    .edge_meta()
                    .resolve_prop_id(&name, as_prop_type(p_type), true)?;
            }

            for PropName { name, p_type } in &meta.temporal {
                let p_type = properties_meta::PropType::try_from(*p_type).unwrap();
                graph
                    .edge_meta()
                    .resolve_prop_id(&name, as_prop_type(p_type), false)?;
            }
        }

        for AddNode {
            id,
            properties,
            time,
            type_id,
        } in &g.add_nodes
        {
            let v = VID(*id as usize);
            let props = properties.as_ref().map(as_prop).into_iter().collect();
            graph.internal_add_node(
                TimeIndexEntry::from(*time),
                v,
                props,
                type_id.map(|id| id as usize).unwrap(),
            )?;
        }

        for update_node_const_props in &g.const_nodes_props {
            let vid = VID(update_node_const_props.id as usize);
            let props = update_node_const_props
                .properties
                .iter()
                .map(|prop| as_prop(prop))
                .collect();
            graph.internal_update_constant_node_properties(vid, props)?;
        }

        for AddEdge {
            src,
            dst,
            properties,
            time,
            layer_id,
        } in &g.edges
        {
            let src = VID(*src as usize);
            let dst = VID(*dst as usize);
            let props = properties.as_ref().map(as_prop).into_iter().collect();
            graph.internal_add_edge(
                TimeIndexEntry::from(*time),
                src,
                dst,
                props,
                layer_id.map(|id| id as usize).unwrap(),
            )?;
        }

        for UpdateEdgeConstProps {
            src,
            dst,
            properties,
            layer_id,
        } in &g.const_edges_props
        {
            let src = VID(*src as usize);
            let dst = VID(*dst as usize);
            let eid = graph
                .core_node_entry(src)
                .find_edge(dst, &LayerIds::All)
                .map(|e| e.pid())
                .unwrap();
            let props = properties.iter().map(|prop| as_prop(prop)).collect();
            graph.internal_update_constant_edge_properties(eid, *layer_id as usize, props)?;
        }

        Ok(())
    }
}

fn as_prop(prop_pair: &PropPair) -> (usize, Prop) {
    let PropPair { key, value } = prop_pair;
    let value = value.as_ref().expect("Missing prop value");
    let value = value.value.as_ref();
    let value = as_prop_value(value);

    (*key as usize, value)
}

fn as_prop_value(value: Option<&prop::Value>) -> Prop {
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
        prop::Value::Str(s) => Prop::Str(ArcStr::from(s.clone())),
        prop::Value::Prop(props) => Prop::List(Arc::new(
            props
                .properties
                .iter()
                .map(|prop| as_prop_value(prop.value.as_ref()))
                .collect(),
        )),
        prop::Value::Map(dict) => Prop::Map(Arc::new(
            dict.map
                .iter()
                .map(|(k, v)| (ArcStr::from(k.as_str()), as_prop_value(v.value.as_ref())))
                .collect(),
        )),
        serialise::prop::Value::NdTime(ndt) => {
            let NdTime {
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
        serialise::prop::Value::DTime(dt) => {
            Prop::DTime(DateTime::parse_from_rfc3339(dt).unwrap().into())
        }
        serialise::prop::Value::Graph(graph) => {
            let g = Graph::new();
            Graph::decode_from_bytes(&graph, &g).expect("Failed to decode graph");
            Prop::Graph(g)
        }
        serialise::prop::Value::PersistentGraph(graph) => {
            let g = Graph::new().persistent_graph();
            PersistentGraph::decode_from_bytes(&graph, &g).expect("Failed to decode graph");
            Prop::PersistentGraph(g)
        }
        serialise::prop::Value::DocumentInput(doc) => Prop::Document(DocumentInput {
            content: doc.content.clone(),
            life: doc
                .life
                .as_ref()
                .map(|l| match l.l_type {
                    Some(lifespan::LType::Interval(lifespan::Interval { start, end })) => {
                        Lifespan::Interval { start, end }
                    }
                    Some(lifespan::LType::Event(lifespan::Event { time })) => {
                        Lifespan::Event { time }
                    }
                    None => Lifespan::Inherited,
                })
                .unwrap_or(Lifespan::Inherited),
        }),
    };
    value
}

fn as_prop_pair(key: u64, prop: &Prop) -> Result<PropPair, GraphError> {
    Ok(PropPair {
        key,
        value: Some(as_proto_prop(prop)?),
    })
}

fn as_proto_prop(prop: &Prop) -> Result<serialise::Prop, GraphError> {
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
            let properties = list.iter().map(as_proto_prop).collect::<Result<_, _>>()?;
            prop::Value::Prop(serialise::Props { properties })
        }
        Prop::Map(map) => {
            let map = map
                .iter()
                .map(|(k, v)| as_proto_prop(v).map(|v| (k.to_string(), v)))
                .collect::<Result<_, _>>()?;
            prop::Value::Map(Dict { map })
        }
        Prop::NDTime(ndt) => {
            let (year, month, day) = (ndt.date().year(), ndt.date().month(), ndt.date().day());
            let (hour, minute, second, nanos) = (
                ndt.time().hour(),
                ndt.time().minute(),
                ndt.time().second(),
                ndt.time().nanosecond(),
            );

            let proto_ndt = NdTime {
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
        Prop::Graph(g) => {
            let bytes = g.encode_to_vec()?;
            prop::Value::Graph(bytes)
        }
        Prop::PersistentGraph(g) => {
            let bytes = g.encode_to_vec()?;
            prop::Value::PersistentGraph(bytes)
        }
        Prop::Document(doc) => {
            let life = match doc.life {
                Lifespan::Interval { start, end } => {
                    Some(lifespan::LType::Interval(lifespan::Interval { start, end }))
                }
                Lifespan::Event { time } => Some(lifespan::LType::Event(lifespan::Event { time })),
                Lifespan::Inherited => None,
            };
            prop::Value::DocumentInput(serialise::DocumentInput {
                content: doc.content.clone(),
                life: Some(serialise::Lifespan { l_type: life }),
            })
        }
    };

    Ok(serialise::Prop { value: Some(value) })
}

#[cfg(test)]
mod proto_test {
    use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

    use crate::{core::DocumentInput, db::api::properties::internal::ConstPropertiesOps};

    use super::*;

    #[test]
    fn node_no_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn node_with_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn node_with_const_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        let n1 = g1
            .add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();

        n1.update_constant_properties([("name", Prop::Str("Bob".into()))])
            .expect("Failed to update constant properties");

        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn edge_no_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn edge_t_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", [("kind", "friends")], None)
            .unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn edge_const_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        let e1 = g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        e1.update_constant_properties([("friends", true)], None)
            .expect("Failed to update constant properties");
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn edge_layers() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g1.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);
    }

    #[test]
    fn test_all_the_t_props_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", props.clone(), None).unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.properties()
                .temporal()
                .get(name)
                .filter(|prop_view| {
                    let (t, prop) = prop_view.iter().next().expect("Failed to get prop");
                    prop == expected && t == 1
                })
                .is_some()
        }))
    }

    #[test]
    fn test_all_the_const_props_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        let n = g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_constant_properties(props.clone())
            .expect("Failed to update constant properties");
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();
        assert_eq!(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.properties()
                .constant()
                .get(name)
                .filter(|prop| prop == &expected)
                .is_some()
        }))
    }

    #[test]
    fn graph_const_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        g1.add_constant_properties(props.clone())
            .expect("Failed to add constant properties");

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();

        props.into_iter().for_each(|(name, prop)| {
            let id = g2.get_const_prop_id(name).expect("Failed to get prop id");
            assert_eq!(prop, g2.get_const_prop(id).expect("Failed to get prop"));
        });
    }

    #[test]
    fn graph_temp_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        for t in 0..props.len() {
            g1.add_properties(t as i64, (&props[t..t + 1]).to_vec())
                .expect("Failed to add constant properties");
        }

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::new();
        Graph::decode(&temp_file, &g2).unwrap();

        props
            .into_iter()
            .enumerate()
            .for_each(|(expected_t, (name, expected))| {
                for (t, prop) in g2
                    .properties()
                    .temporal()
                    .get(name)
                    .expect("Failed to get prop view")
                {
                    assert_eq!(prop, expected);
                    assert_eq!(t, expected_t as i64);
                }
            });
    }

    fn write_props_to_vec(props: &mut Vec<(&str, Prop)>) {
        props.push(("name", Prop::Str("Alice".into())));
        props.push(("age", Prop::U32(47)));
        props.push(("score", Prop::I32(27)));
        props.push(("is_adult", Prop::Bool(true)));
        props.push(("height", Prop::F32(1.75)));
        props.push(("weight", Prop::F64(75.5)));
        props.push((
            "children",
            Prop::List(Arc::new(vec![
                Prop::Str("Bob".into()),
                Prop::Str("Charlie".into()),
            ])),
        ));
        props.push((
            "properties",
            Prop::Map(Arc::new(
                props
                    .iter()
                    .map(|(k, v)| (ArcStr::from(*k), v.clone()))
                    .collect(),
            )),
        ));
        let fmt = "%Y-%m-%d %H:%M:%S";
        props.push((
            "time",
            Prop::NDTime(
                NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", fmt)
                    .expect("Failed to parse time"),
            ),
        ));

        props.push((
            "dtime",
            Prop::DTime(
                DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                    .unwrap()
                    .into(),
            ),
        ));

        props.push((
            "doc",
            Prop::Document(DocumentInput {
                content: "Hello, World!".into(),
                life: Lifespan::Interval {
                    start: -11i64,
                    end: 100i64,
                },
            }),
        ));
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        props.push(("graph", Prop::Graph(graph)));

        let graph = Graph::new().persistent_graph();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.delete_edge(2, "a", "b", None).unwrap();
        props.push(("p_graph", Prop::PersistentGraph(graph)));
    }
}
