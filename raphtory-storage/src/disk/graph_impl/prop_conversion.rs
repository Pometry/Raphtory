use crate::{core_ops::CoreGraphOps, graph::nodes::node_storage_ops::NodeStorageOps};
use itertools::Itertools;
use num_traits::ToPrimitive;
use polars_arrow::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    datatypes::{ArrowDataType, ArrowSchema, Field},
};
use pometry_storage::{
    properties::{node_ts, NodePropsBuilder, Properties},
    RAError,
};
use raphtory_api::core::entities::{
    properties::{
        meta::PropMapper,
        prop::{Prop, PropType, PropUnwrap},
        tprop::TPropOps,
    },
    VID,
};
use raphtory_core::utils::iter::GenLockedIter;
use std::path::Path;

pub fn make_node_properties_from_graph<G: CoreGraphOps>(
    graph: &G,
    graph_dir: impl AsRef<Path>,
) -> Result<Properties<VID>, RAError> {
    let graph_dir = graph_dir.as_ref();
    let n = graph.unfiltered_num_nodes();

    let temporal_meta = graph.node_meta().temporal_prop_meta();
    let constant_meta = graph.node_meta().const_prop_meta();

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
            let node = gs.core_node(vid);
            node.as_ref().temp_prop_rows().map(|(ts, _)| ts).collect()
        })
        .with_const_props(const_prop_keys, |prop_id, prop_key| {
            let prop_type = constant_meta.get_dtype(prop_id).unwrap();
            let col = arrow_array_from_props(
                (0..n).map(|vid| {
                    let node = gs.core_node(VID(vid));
                    node.prop(prop_id)
                }),
                prop_type,
            );
            col.map(|col| {
                let dtype = col.data_type().clone();
                (Field::new(prop_key, dtype, true), col)
            })
        })
        .with_temporal_props(temporal_prop_keys, |prop_id, prop_key, ts, offsets| {
            let prop_type = temporal_meta.get_dtype(prop_id).unwrap();
            let col = arrow_array_from_props(
                (0..n).flat_map(|vid| {
                    let ts = node_ts(VID(vid), offsets, ts);
                    let node = gs.core_node(VID(vid));
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
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::U8 => {
            let array: PrimitiveArray<u8> = props.map(|prop| prop.into_u8()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::U16 => {
            let array: PrimitiveArray<u16> = props.map(|prop| prop.into_u16()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::I32 => {
            let array: PrimitiveArray<i32> = props.map(|prop| prop.into_i32()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::I64 => {
            let array: PrimitiveArray<i64> = props.map(|prop| prop.into_i64()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::U32 => {
            let array: PrimitiveArray<u32> = props.map(|prop| prop.into_u32()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::U64 => {
            let array: PrimitiveArray<u64> = props.map(|prop| prop.into_u64()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::F32 => {
            let array: PrimitiveArray<f32> = props.map(|prop| prop.into_f32()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::F64 => {
            let array: PrimitiveArray<f64> = props.map(|prop| prop.into_f64()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::Bool => {
            let array: BooleanArray = props.map(|prop| prop.into_bool()).collect();
            (array.null_count() != array.len()).then_some(array.boxed())
        }
        PropType::Decimal { scale } => {
            let array: PrimitiveArray<i128> = props
                .map(|prop| {
                    prop.into_decimal().and_then(|d| {
                        let (int, _) = d.as_bigint_and_exponent();
                        int.to_i128()
                    })
                })
                .collect();
            (array.null_count() != array.len())
                .then_some(array.to(ArrowDataType::Decimal(38, scale as usize)).boxed())
        }
        PropType::Empty
        | PropType::List(_)
        | PropType::Map(_)
        | PropType::NDTime
        | PropType::Array(_)
        | PropType::DTime => panic!("{prop_type:?} not supported as disk_graph property"),
    }
}

pub fn schema_from_prop_meta(prop_map: &PropMapper) -> ArrowSchema {
    let time_field = Field::new("time", ArrowDataType::Int64, false);
    let mut schema = vec![time_field];

    for (id, key) in prop_map.get_keys().iter().enumerate() {
        match prop_map.get_dtype(id).unwrap() {
            PropType::Str => {
                schema.push(Field::new(key, ArrowDataType::LargeUtf8, true));
            }
            PropType::U8 => {
                schema.push(Field::new(key, ArrowDataType::UInt8, true));
            }
            PropType::U16 => {
                schema.push(Field::new(key, ArrowDataType::UInt16, true));
            }
            PropType::I32 => {
                schema.push(Field::new(key, ArrowDataType::Int32, true));
            }
            PropType::I64 => {
                schema.push(Field::new(key, ArrowDataType::Int64, true));
            }
            PropType::U32 => {
                schema.push(Field::new(key, ArrowDataType::UInt32, true));
            }
            PropType::U64 => {
                schema.push(Field::new(key, ArrowDataType::UInt64, true));
            }
            PropType::F32 => {
                schema.push(Field::new(key, ArrowDataType::Float32, true));
            }
            PropType::F64 => {
                schema.push(Field::new(key, ArrowDataType::Float64, true));
            }
            PropType::Bool => {
                schema.push(Field::new(key, ArrowDataType::Boolean, true));
            }
            PropType::Decimal { scale } => {
                schema.push(Field::new(
                    key,
                    ArrowDataType::Decimal(38, scale as usize),
                    true,
                ));
            }
            prop_type @ (PropType::Empty
            | PropType::List(_)
            | PropType::Map(_)
            | PropType::NDTime
            | PropType::Array(_)
            | PropType::DTime) => panic!("{:?} not supported as disk_graph property", prop_type),
        }
    }

    ArrowSchema::from(schema)
}
