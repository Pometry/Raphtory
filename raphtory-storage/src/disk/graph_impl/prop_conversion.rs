use crate::{core_ops::CoreGraphOps, graph::nodes::node_storage_ops::NodeStorageOps};
use arrow_array::{
    builder::BooleanBuilder, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, DECIMAL128_MAX_PRECISION};
use itertools::Itertools;
use num_traits::ToPrimitive;
use pometry_storage::{
    chunked_array::array_like::BaseArrayLike,
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

    let temporal_mapper = graph.node_meta().temporal_prop_mapper();
    let metadata_mapper = graph.node_meta().metadata_mapper();

    let gs = graph.core_graph();

    let temporal_prop_keys = temporal_mapper
        .get_keys()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let metadata_keys = metadata_mapper
        .get_keys()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let builder = NodePropsBuilder::new(n, graph_dir)
        .with_timestamps(|vid| {
            let node = gs.core_node(vid);
            node.as_ref().temp_prop_rows().map(|(ts, _)| ts).collect()
        })
        .with_metadata(metadata_keys, |prop_id, prop_key| {
            let prop_type = metadata_mapper.get_dtype(prop_id).unwrap();
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
        .with_properties(temporal_prop_keys, |prop_id, prop_key, ts, offsets| {
            let prop_type = temporal_mapper.get_dtype(prop_id).unwrap();
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
) -> Option<ArrayRef> {
    match prop_type {
        PropType::Str => {
            let array: LargeStringArray = props.map(|prop| prop.into_str()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::U8 => {
            let array: UInt8Array = props.map(|prop| prop.into_u8()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::U16 => {
            let array: UInt16Array = props.map(|prop| prop.into_u16()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::I32 => {
            let array: Int32Array = props.map(|prop| prop.into_i32()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::I64 => {
            let array: Int64Array = props.map(|prop| prop.into_i64()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::U32 => {
            let array: UInt32Array = props.map(|prop| prop.into_u32()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::U64 => {
            let array: UInt64Array = props.map(|prop| prop.into_u64()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::F32 => {
            let array: Float32Array = props.map(|prop| prop.into_f32()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::F64 => {
            let array: Float64Array = props.map(|prop| prop.into_f64()).collect();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::Bool => {
            // direct collect requires known size for the iterator which we do not have
            let mut builder = BooleanBuilder::new();
            builder.extend(props.map(|prop| prop.into_bool()));
            let array = builder.finish();
            (array.null_count() != array.len()).then_some(array.as_array_ref())
        }
        PropType::Decimal { scale } => {
            let array: Decimal128Array = props
                .map(|prop| {
                    prop.into_decimal().and_then(|d| {
                        let (int, _) = d.as_bigint_and_exponent();
                        int.to_i128()
                    })
                })
                .collect();
            (array.null_count() != array.len()).then_some(
                array
                    .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale as i8)
                    .expect("valid decimal")
                    .as_array_ref(),
            )
        }
        PropType::Empty
        | PropType::List(_)
        | PropType::Map(_)
        | PropType::NDTime
        | PropType::Array(_)
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
            PropType::Decimal { scale } => {
                schema.push(Field::new(key, DataType::Decimal128(38, scale as i8), true));
            }
            prop_type @ (PropType::Empty
            | PropType::List(_)
            | PropType::Map(_)
            | PropType::NDTime
            | PropType::Array(_)
            | PropType::DTime) => panic!("{:?} not supported as disk_graph property", prop_type),
        }
    }

    Schema::new(schema)
}
