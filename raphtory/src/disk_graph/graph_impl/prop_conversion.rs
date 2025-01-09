use crate::{
    arrow2::{
        array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
        datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
    },
    core::{
        entities::{properties::props::PropMapper, VID},
        utils::iter::GenLockedIter,
        PropType,
    },
    db::api::{
        storage::graph::{nodes::node_storage_ops::NodeStorageOps, tprop_storage_ops::TPropOps},
        view::internal::CoreGraphOps,
    },
    prelude::{Graph, Prop, PropUnwrap},
};
use itertools::Itertools;
use pometry_storage::{
    properties::{node_ts, NodePropsBuilder, Properties},
    RAError,
};
use std::path::Path;

pub fn make_node_properties_from_graph(
    graph: &Graph,
    graph_dir: impl AsRef<Path>,
) -> Result<Properties<VID>, RAError> {
    let graph_dir = graph_dir.as_ref();
    let n = graph.unfiltered_num_nodes();

    let temporal_meta = graph.node_meta().temporal_prop_meta();
    let constant_meta = graph.node_meta().const_prop_meta();
    if temporal_meta.is_empty() && constant_meta.is_empty() {
        return Ok(Properties::default());
    }

    let gs = graph.core_graph();

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
            let node = gs.node_entry(vid);
            node.as_ref().temp_prop_rows().map(|(ts, _)| ts).collect()
        })
        .with_temporal_props(temporal_prop_keys, |prop_id, prop_key, ts, offsets| {
            let prop_type = temporal_meta.get_dtype(prop_id).unwrap();
            let col = arrow_array_from_props(
                (0..n).flat_map(|vid| {
                    let ts = node_ts(VID(vid), offsets, ts);
                    let node = gs.node_entry(VID(vid));
                    let iter =
                        GenLockedIter::from(node, |node| Box::new(node.tprop(prop_id).iter()));
                    iter.merge_join_by(ts, |(t2, _), &t1| t2.cmp(t1))
                        .map(|result| match result {
                            itertools::EitherOrBoth::Both((_, t_prop), _) => Some(t_prop),
                            _ => None,
                        })
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
                    let node = gs.node_entry(VID(vid));
                    node.prop(prop_id)
                }),
                prop_type,
            );
            col.map(|col| {
                let dtype = col.data_type().clone();
                (Field::new(prop_key, dtype, true), col)
            })
        });
    let props = builder.build()?;
    Ok(props)
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
        | PropType::Array(_)
        | PropType::Document
        | PropType::DTime => panic!("{prop_type:?} not supported as disk_graph property"),
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
            | PropType::Array(_)
            | PropType::Document
            | PropType::DTime) => panic!("{:?} not supported as disk_graph property", prop_type),
        }
    }

    Schema::from(schema)
}
