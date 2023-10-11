mod edge_frame_builder;
mod edge_overflow_builder;

use arrow2::{
    array::*,
    datatypes::{DataType, Field},
    types::{f16, NativeType, Offset},
};
pub(crate) use edge_frame_builder::*;
pub(crate) use edge_overflow_builder::EdgeOverflowChunk;

use super::Time;

// pub(crate) fn extend_time_slice(edge_timestamps: &mut Vec<Time>, arr: &PrimitiveArray<i64>) {
//     let time_slice = arr.values();
//     edge_timestamps.extend_from_slice(time_slice);
// }

pub(crate) fn extend_tprops_slice(
    maybe_t_props: &mut Option<MutableStructArray>,
    copy_from: &StructArray,
) {
    if let Some(mut t_props) = maybe_t_props.take() {
        let values = t_props.mut_values();
        assert!(
            values.len() > 0,
            "if they exist properties mut have at least 1 column"
        );
        copy_from
            .values()
            .iter()
            .enumerate()
            .for_each(|(i, from_col)| {
                let into_col = &mut values[i];
                dynamic_extend_array(into_col, from_col);
            });
        // put it back
        *maybe_t_props = Some(t_props);
    } else {
        let mut_arrays = as_mut_arrays(copy_from);
        let t_props =
            MutableStructArray::new(remap_data_type(copy_from.data_type().clone()), mut_arrays);
        *maybe_t_props = Some(t_props);
        extend_tprops_slice(maybe_t_props, copy_from);
    }
}

fn dynamic_extend_array(
    into_col: &mut Box<dyn MutableArray>,
    from_col: &Box<dyn Array>,
) -> Option<()> {
    match into_col.data_type() {
        DataType::Boolean => {
            let into_col = into_col
                .as_mut_any()
                .downcast_mut::<MutableBooleanArray>()?;
            let from_col = from_col
                .as_any()
                .downcast_ref::<BooleanArray>()?
                .into_iter();

            for val in from_col {
                into_col.push(val);
            }
        }
        DataType::Int8 => extend_primitive_array::<i8>(into_col, from_col)?,
        DataType::Int16 => extend_primitive_array::<i64>(into_col, from_col)?,
        DataType::Int32 => extend_primitive_array::<i32>(into_col, from_col)?,
        DataType::Int64 => extend_primitive_array::<i64>(into_col, from_col)?,
        DataType::UInt8 => extend_primitive_array::<u8>(into_col, from_col)?,
        DataType::UInt16 => extend_primitive_array::<u16>(into_col, from_col)?,
        DataType::UInt32 => extend_primitive_array::<u32>(into_col, from_col)?,
        DataType::UInt64 => extend_primitive_array::<u64>(into_col, from_col)?,
        DataType::Float16 => extend_primitive_array::<f16>(into_col, from_col)?,
        DataType::Float32 => extend_primitive_array::<f32>(into_col, from_col)?,
        DataType::Float64 => extend_primitive_array::<f64>(into_col, from_col)?,
        DataType::Utf8 => extend_utf8_array::<i64>(into_col, from_col)?,
        DataType::LargeUtf8 => extend_utf8_array::<i64>(into_col, from_col)?,
        what => {
            println!("Skipping data type {:?}", what)
        }
    }
    Some(())
}

fn extend_primitive_array<T: NativeType + Copy>(
    into_col: &mut Box<dyn MutableArray>,
    from_col: &Box<dyn Array>,
) -> Option<()> {
    let into_col = into_col
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<T>>()?;

    let from_col = from_col.as_any().downcast_ref::<PrimitiveArray<T>>()?;

    // happy fast path
    if into_col.validity().is_none() && from_col.validity().is_none() {
        let values = from_col.values();
        into_col.extend_from_slice(values);
    } else {
        for val in from_col.into_iter() {
            into_col.push(val.copied());
        }
    }
    Some(())
}

fn extend_utf8_array<I: Offset>(
    into_col: &mut Box<dyn MutableArray>,
    from_col: &Box<dyn Array>,
) -> Option<()> {
    let into_col = into_col
        .as_mut_any()
        .downcast_mut::<MutableUtf8Array<I>>()?;

    match from_col.data_type() {
        DataType::LargeUtf8 => {
            let arr = from_col.as_any().downcast_ref::<Utf8Array<i64>>()?;
            for val in arr.into_iter() {
                into_col.push(val);
            }
        }
        DataType::Utf8 => {
            let arr = from_col.as_any().downcast_ref::<Utf8Array<i32>>()?;
            for val in arr.into_iter() {
                into_col.push(val);
            }
        }
        _ => {
            println!("Skipping data type {:?}", from_col.data_type());
            return None;
        }
    }

    Some(())
}

fn remap_data_type(dt: DataType) -> DataType {
    match dt {
        DataType::Utf8 => DataType::LargeUtf8,
        DataType::Struct(fields) => DataType::Struct(
            fields
                .into_iter()
                .map(|f| Field::new(f.name, remap_data_type(f.data_type), f.is_nullable))
                .collect(),
        ),
        _ => dt,
    }
}

fn as_mut_arrays(copy_from: &StructArray) -> Vec<Box<dyn MutableArray>> {
    copy_from
        .values()
        .iter()
        .map(|arr| match arr.data_type() {
            DataType::Boolean => {
                let arr: Box<dyn MutableArray> = Box::new(MutableBooleanArray::new());
                arr
            }
            DataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::new()),
            DataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::new()),
            DataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::new()),
            DataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::new()),
            DataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::new()),
            DataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::new()),
            DataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::new()),
            DataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::new()),
            DataType::Float16 => Box::new(MutablePrimitiveArray::<f16>::new()),
            DataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::new()),
            DataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::new()),
            DataType::Utf8 => Box::new(MutableUtf8Array::<i64>::new()),
            DataType::LargeUtf8 => Box::new(MutableUtf8Array::<i64>::new()),
            _ => panic!("Unsupported data type"),
        })
        .collect()
}
