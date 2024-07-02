use std::{fs::File, io::Write, path::Path};

use prost::Message;
use raphtory_api::core::storage::arc_str::ArcStr;

use crate::{
    core::utils::errors::GraphError,
    prelude::*,
    serialise::{
        self,
        graph::{AddNode, PropPair},
    },
};

use super::GraphViewOps;

pub trait StableSerialise {
    fn stable_serialise(&self, path: impl AsRef<Path>) -> Result<(), GraphError>;
    fn stable_deserialise(path: impl AsRef<Path>) -> Result<Graph, GraphError>;
}

impl<'graph, G: GraphViewOps<'graph>> StableSerialise for G {
    fn stable_serialise(&self, path: impl AsRef<Path>) -> Result<(), GraphError> {
        let mut graph = serialise::Graph::default();

        let serialise::Graph {
            ref mut nodes,
            ref mut edges,
            ..
        } = graph;

        for v in self.nodes().iter() {
            let node_type = v.node_type().map(|s| s.to_string());
            let id = v.node.0 as u64;
            let name = v.name();

            for time in v.history() {
                nodes.push(AddNode {
                    id,
                    name: name.clone(),
                    properties: None,
                    node_type: node_type.clone(),
                    time,
                });
            }

            for (prop_name, prop_view) in v.properties().temporal().iter() {
                for (time, prop) in prop_view.iter() {
                    nodes.push(AddNode {
                        id,
                        name: name.clone(),
                        properties: Some(as_prop_pair(&prop_name, &prop)),
                        node_type: node_type.clone(),
                        time,
                    });
                }
            }

            // node.add_constant_properties(v.properties().constant())?;
        }

        let mut file = File::create(path)?;
        let bytes = graph.encode_to_vec();
        file.write_all(&bytes)?;

        Ok(())
    }

    fn stable_deserialise(path: impl AsRef<Path>) -> Result<Graph, GraphError> {
        let file = File::open(path)?;
        let buf = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let g = serialise::Graph::decode(&buf[..]).expect("Failed to decode graph");
        let graph = Graph::new();

        for add_node in &g.nodes {
            graph.add_node(
                add_node.time,
                add_node.name.as_ref(),
                add_node.properties.as_ref().map(as_prop),
                add_node.node_type.as_deref(),
            )?;
        }
        Ok(graph)
    }
}

fn as_prop(prop_pair: &PropPair) -> (String, Prop) {
    let PropPair { key, value } = prop_pair;
    let value = value.as_ref().expect("Missing prop value");
    let value = match value.value.as_ref().expect("Missing prop value") {
        serialise::prop::Value::BoolValue(b) => Prop::Bool(*b),
        serialise::prop::Value::U8(u) => Prop::U8((*u).try_into().unwrap()),
        serialise::prop::Value::U16(u) => Prop::U16((*u).try_into().unwrap()),
        serialise::prop::Value::U32(u) => Prop::U32(*u),
        serialise::prop::Value::I32(i) => Prop::I32(*i),
        serialise::prop::Value::I64(i) => Prop::I64(*i),
        serialise::prop::Value::U64(u) => Prop::U64(*u),
        serialise::prop::Value::F32(f) => Prop::F32(*f),
        serialise::prop::Value::F64(f) => Prop::F64(*f),
        serialise::prop::Value::Str(s) => Prop::Str(ArcStr::from(s.clone())),
        _ => todo!(),
        // serialise::prop::Value::List(_) => todo!(),
        // serialise::prop::Value::Map(_) => todo!(),
        // serialise::prop::Value::NDTime(_) => todo!(),
        // serialise::prop::Value::DTime(_) => todo!(),
        // serialise::prop::Value::Graph(_) => todo!(),
        // serialise::prop::Value::PersistentGraph(_) => todo!(),
        // serialise::prop::Value::Document(_) => todo!(),
    };

    (key.clone(), value)
}

fn as_prop_pair(prop_name: &str, prop: &Prop) -> PropPair {
    PropPair {
        key: prop_name.to_string(),
        value: Some(as_proto_prop(prop)),
    }
}

fn as_proto_prop(prop: &Prop) -> serialise::Prop {
    let value: serialise::prop::Value = match prop {
        Prop::Bool(b) => serialise::prop::Value::BoolValue(*b),
        Prop::U8(u) => serialise::prop::Value::U8((*u).into()),
        Prop::U16(u) => serialise::prop::Value::U16((*u).into()),
        Prop::U32(u) => serialise::prop::Value::U32(*u),
        Prop::I32(i) => serialise::prop::Value::I32(*i),
        Prop::I64(i) => serialise::prop::Value::I64(*i),
        Prop::U64(u) => serialise::prop::Value::U64(*u),
        Prop::F32(f) => serialise::prop::Value::F32(*f),
        Prop::F64(f) => serialise::prop::Value::F64(*f),
        Prop::Str(s) => serialise::prop::Value::Str(s.to_string()),
        Prop::List(_) => todo!(),
        Prop::Map(_) => todo!(),
        Prop::NDTime(_) => todo!(),
        Prop::DTime(_) => todo!(),
        Prop::Graph(_) => todo!(),
        Prop::PersistentGraph(_) => todo!(),
        Prop::Document(_) => todo!(),
    };

    serialise::Prop { value: Some(value) }
}

#[cfg(test)]
mod proto_test {
    use super::*;

    #[test]
    fn node_no_props() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.stable_serialise(&temp_file).unwrap();
        let g2 = Graph::stable_deserialise(&temp_file).unwrap();
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
        let g2 = Graph::stable_deserialise(&temp_file).unwrap();
        assert_eq!(&g1, &g2);
    }
}
