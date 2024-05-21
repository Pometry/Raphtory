use crate::{
    arrow2::{
        array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
        datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
    },
    core::{
        entities::{properties::props::PropMapper, VID},
        PropType,
    },
    db::api::{storage::tprop_storage_ops::TPropOps, view::internal::CoreGraphOps},
    prelude::{Graph, Prop, PropUnwrap},
};
use itertools::Itertools;
use raphtory_arrow::{
    properties::{node_ts, NodePropsBuilder, Properties},
    RAError,
};
use std::path::Path;

pub fn make_node_properties_from_graph(
    graph: &Graph,
    graph_dir: impl AsRef<Path>,
) -> Result<Option<Properties<raphtory_arrow::interop::VID>>, RAError> {
    let graph_dir = graph_dir.as_ref();
    let n = graph.unfiltered_num_nodes();

    let temporal_meta = graph.node_meta().temporal_prop_meta();
    let constant_meta = graph.node_meta().const_prop_meta();
    if temporal_meta.is_empty() && constant_meta.is_empty() {
        return Ok(None);
    }

    let nodes = graph.0.inner().storage.nodes.read_lock();

    let temporal_prop_keys = temporal_meta
        .get_keys()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let const_prop_keys = constant_meta
        .get_keys()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let builder = NodePropsBuilder::new(n, graph_dir)
        .with_timestamps(|vid| {
            let node = nodes.get(VID(vid.0));
            let ts: Vec<_> = node
                .props
                .iter()
                .flat_map(|props| {
                    props
                        .temporal_props
                        .filled_values()
                        .map(|tprop| tprop.iter().map(|(t, _)| t))
                })
                .kmerge()
                .dedup()
                .collect();
            ts
        })
        .with_temporal_props(temporal_prop_keys, |prop_id, prop_key, ts, offsets| {
            let prop_type = temporal_meta.get_dtype(prop_id).unwrap();
            let col = arrow_array_from_props(
                (0..n).flat_map(|vid| {
                    let ts = node_ts(raphtory_arrow::interop::VID(vid), offsets, ts);
                    let node = nodes.get(VID(vid));
                    ts.iter()
                        .map(move |t| node.temporal_property(prop_id).and_then(|prop| prop.at(t)))
                }),
                prop_type,
            );
            col.map(|col| {
                let dtype = col.data_type().clone();
                (Field::new(prop_key, dtype, true), col)
            })
        })
        .with_const_props(const_prop_keys, |prop_id, prop_key| {
            let prop_type = constant_meta.get_dtype(prop_id).unwrap();
            let col = arrow_array_from_props(
                (0..n).map(|vid| {
                    let node = nodes.get(VID(vid));
                    node.const_prop(prop_id).cloned()
                }),
                prop_type,
            );
            col.map(|col| {
                let dtype = col.data_type().clone();
                (Field::new(prop_key, dtype, true), col)
            })
        });
    let props = builder.build().map(Some)?;
    Ok(props)
}
pub fn arrow_dtype_from_prop_type(prop_type: PropType) -> DataType {
    match prop_type {
        PropType::Str => DataType::LargeUtf8,
        PropType::U8 => DataType::UInt8,
        PropType::U16 => DataType::UInt16,
        PropType::I32 => DataType::Int32,
        PropType::I64 => DataType::Int64,
        PropType::U32 => DataType::UInt32,
        PropType::U64 => DataType::UInt64,
        PropType::F32 => DataType::Float32,
        PropType::F64 => DataType::Float64,
        PropType::Bool => DataType::Boolean,
        PropType::Empty
        | PropType::List
        | PropType::Map
        | PropType::NDTime
        | PropType::Graph
        | PropType::PersistentGraph
        | PropType::Document
        | PropType::DTime => panic!("{prop_type:?} not supported as arrow property"),
    }
}

/// Map iterator of prop values to array (returns None if all the props are None)
pub fn arrow_array_from_props(
    props: impl Iterator<Item = Option<Prop>>,
    prop_type: PropType,
) -> Option<Box<dyn Array>> {
    match prop_type {
        PropType::Str => {
            let array: Utf8Array<i64> = props.map(|prop| prop.into_str()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::U8 => {
            let array: PrimitiveArray<u8> = props.map(|prop| prop.into_u8()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::U16 => {
            let array: PrimitiveArray<u16> = props.map(|prop| prop.into_u16()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::I32 => {
            let array: PrimitiveArray<i32> = props.map(|prop| prop.into_i32()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::I64 => {
            let array: PrimitiveArray<i64> = props.map(|prop| prop.into_i64()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::U32 => {
            let array: PrimitiveArray<u32> = props.map(|prop| prop.into_u32()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::U64 => {
            let array: PrimitiveArray<u64> = props.map(|prop| prop.into_u64()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::F32 => {
            let array: PrimitiveArray<f32> = props.map(|prop| prop.into_f32()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::F64 => {
            let array: PrimitiveArray<f64> = props.map(|prop| prop.into_f64()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::Bool => {
            let array: BooleanArray = props.map(|prop| prop.into_bool()).collect();
            array.iter().any(|v| v.is_some()).then_some(array.boxed())
        }
        PropType::Empty
        | PropType::List
        | PropType::Map
        | PropType::NDTime
        | PropType::Graph
        | PropType::PersistentGraph
        | PropType::Document
        | PropType::DTime => panic!("{prop_type:?} not supported as arrow property"),
    }
}

pub fn schema_from_prop_meta(prop_map: &PropMapper) -> Schema {
    let time_field = Field::new("time", DataType::Int64, false);
    let mut schema = vec![time_field];

    for (id, key) in prop_map.get_keys().iter().enumerate() {
        match prop_map.get_dtype(id).unwrap() {
            PropType::Str => {
                schema.push(Field::new(key, DataType::LargeUtf8, true));
            }
            PropType::U8 => {
                schema.push(Field::new(key, DataType::UInt8, true));
            }
            PropType::U16 => {
                schema.push(Field::new(key, DataType::UInt16, true));
            }
            PropType::I32 => {
                schema.push(Field::new(key, DataType::Int32, true));
            }
            PropType::I64 => {
                schema.push(Field::new(key, DataType::Int64, true));
            }
            PropType::U32 => {
                schema.push(Field::new(key, DataType::UInt32, true));
            }
            PropType::U64 => {
                schema.push(Field::new(key, DataType::UInt64, true));
            }
            PropType::F32 => {
                schema.push(Field::new(key, DataType::Float32, true));
            }
            PropType::F64 => {
                schema.push(Field::new(key, DataType::Float64, true));
            }
            PropType::Bool => {
                schema.push(Field::new(key, DataType::Boolean, true));
            }
            prop_type @ (PropType::Empty
            | PropType::List
            | PropType::Map
            | PropType::NDTime
            | PropType::Graph
            | PropType::PersistentGraph
            | PropType::Document
            | PropType::DTime) => panic!("{:?} not supported as arrow property", prop_type),
        }
    }

    let schema = Schema::from(schema);
    schema
}
