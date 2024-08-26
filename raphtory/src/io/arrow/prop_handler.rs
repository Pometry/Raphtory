use crate::{
    core::{
        utils::errors::{DataTypeError, GraphError},
        IntoPropList, PropType,
    },
    io::arrow::dataframe::DFChunk,
    prelude::Prop,
};
use chrono::{DateTime, Utc};
use polars_arrow::{
    array::{
        Array, BooleanArray, FixedSizeListArray, ListArray, PrimitiveArray, StaticArray, Utf8Array,
    },
    datatypes::{ArrowDataType as DataType, TimeUnit},
    offset::Offset,
};
use raphtory_api::core::storage::dict_mapper::MaybeNew;
use rayon::prelude::*;

pub struct PropCols {
    prop_ids: Vec<usize>,
    cols: Vec<Box<dyn PropCol>>,
    len: usize,
}

impl PropCols {
    fn iter_row(&self, i: usize) -> impl Iterator<Item = (usize, Prop)> + '_ {
        self.prop_ids
            .iter()
            .zip(self.cols.iter())
            .filter_map(move |(id, col)| col.get(i).map(|v| (*id, v)))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn par_rows(
        &self,
    ) -> impl IndexedParallelIterator<Item = impl Iterator<Item = (usize, Prop)> + '_> + '_ {
        (0..self.len()).into_par_iter().map(|i| self.iter_row(i))
    }
}

pub(crate) fn combine_properties(
    props: &[&str],
    indices: &[usize],
    df: &DFChunk,
    prop_id_resolver: impl Fn(&str, PropType) -> Result<MaybeNew<usize>, GraphError>,
) -> Result<PropCols, GraphError> {
    let dtypes = indices
        .iter()
        .map(|idx| data_type_as_prop_type(df.chunk[*idx].data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let cols = indices
        .iter()
        .map(|idx| lift_property_col(df.chunk[*idx].as_ref()))
        .collect::<Vec<_>>();
    let prop_ids = props
        .iter()
        .zip(dtypes.into_iter())
        .map(|(name, dtype)| Ok(prop_id_resolver(name, dtype)?.inner()))
        .collect::<Result<Vec<_>, GraphError>>()?;

    Ok(PropCols {
        prop_ids,
        cols,
        len: df.len(),
    })
}

fn arr_as_prop(arr: Box<dyn Array>) -> Prop {
    match arr.data_type() {
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            arr.iter().flatten().copied().into_prop_list()
        }
        DataType::Utf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::List(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        DataType::LargeList(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| arr_as_prop(elem))
                .into_prop_list()
        }
        _ => panic!("Data type not recognized"),
    }
}

fn data_type_as_prop_type(dt: &DataType) -> Result<PropType, GraphError> {
    match dt {
        DataType::Boolean => Ok(PropType::Bool),
        DataType::Int32 => Ok(PropType::I32),
        DataType::Int64 => Ok(PropType::I64),
        DataType::UInt8 => Ok(PropType::U8),
        DataType::UInt16 => Ok(PropType::U16),
        DataType::UInt32 => Ok(PropType::U32),
        DataType::UInt64 => Ok(PropType::U64),
        DataType::Float32 => Ok(PropType::F32),
        DataType::Float64 => Ok(PropType::F64),
        DataType::Utf8 => Ok(PropType::Str),
        DataType::LargeUtf8 => Ok(PropType::Str),
        DataType::List(v) => is_data_type_supported(v.data_type()).map(|_| PropType::List),
        DataType::FixedSizeList(v, _) => {
            is_data_type_supported(v.data_type()).map(|_| PropType::List)
        }
        DataType::LargeList(v) => is_data_type_supported(v.data_type()).map(|_| PropType::List),
        DataType::Timestamp(_, v) => match v {
            None => Ok(PropType::NDTime),
            Some(_) => Ok(PropType::DTime),
        },
        _ => Err(DataTypeError::InvalidPropertyType(dt.clone()).into()),
    }
}

fn is_data_type_supported(dt: &DataType) -> Result<(), GraphError> {
    match dt {
        DataType::Boolean => {}
        DataType::Int32 => {}
        DataType::Int64 => {}
        DataType::UInt8 => {}
        DataType::UInt16 => {}
        DataType::UInt32 => {}
        DataType::UInt64 => {}
        DataType::Float32 => {}
        DataType::Float64 => {}
        DataType::Utf8 => {}
        DataType::LargeUtf8 => {}
        DataType::List(v) => is_data_type_supported(v.data_type())?,
        DataType::FixedSizeList(v, _) => is_data_type_supported(v.data_type())?,
        DataType::LargeList(v) => is_data_type_supported(v.data_type())?,
        DataType::Timestamp(_, _) => {}
        _ => return Err(DataTypeError::InvalidPropertyType(dt.clone()).into()),
    }
    Ok(())
}

trait PropCol: Send + Sync {
    fn get(&self, i: usize) -> Option<Prop>;

    fn len(&self) -> usize;
}

impl<A> PropCol for A
where
    A: StaticArray,
    for<'a> A::ValueT<'a>: Into<Prop>,
{
    #[inline]
    fn get(&self, i: usize) -> Option<Prop> {
        StaticArray::get(self, i).map(|v| v.into())
    }

    #[inline]
    fn len(&self) -> usize {
        Array::len(self)
    }
}

struct Wrap<A>(A);

impl PropCol for Wrap<Utf8Array<i32>> {
    fn get(&self, i: usize) -> Option<Prop> {
        self.0.get(i).map(Prop::str)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<O: Offset> PropCol for Wrap<ListArray<O>> {
    fn get(&self, i: usize) -> Option<Prop> {
        if i >= self.0.len() {
            None
        } else {
            // safety: bounds checked above
            unsafe {
                if self.0.is_null_unchecked(i) {
                    None
                } else {
                    let value = self.0.value_unchecked(i);
                    Some(arr_as_prop(value))
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl PropCol for Wrap<FixedSizeListArray> {
    fn get(&self, i: usize) -> Option<Prop> {
        self.0.get(i).map(arr_as_prop)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

struct DTimeCol {
    arr: PrimitiveArray<i64>,
    map: fn(i64) -> Prop,
}

impl PropCol for DTimeCol {
    fn get(&self, i: usize) -> Option<Prop> {
        StaticArray::get(&self.arr, i).map(self.map)
    }

    fn len(&self) -> usize {
        self.arr.len()
    }
}
fn lift_property_col(arr: &dyn Array) -> Box<dyn PropCol> {
    match arr.data_type() {
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(arr.clone())
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::Utf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Box::new(Wrap(arr.clone()))
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            Box::new(arr.clone())
        }
        DataType::List(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            Box::new(Wrap(arr.clone()))
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            Box::new(Wrap(arr.clone()))
        }
        DataType::LargeList(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            Box::new(Wrap(arr.clone()))
        }
        DataType::Timestamp(timeunit, timezone) => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .clone();
            match timezone {
                Some(_) => match timeunit {
                    TimeUnit::Second => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::DTime(
                                DateTime::<Utc>::from_timestamp(v, 0)
                                    .expect("DateTime conversion failed"),
                            )
                        },
                    }),
                    TimeUnit::Millisecond => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::DTime(
                                DateTime::<Utc>::from_timestamp_millis(v)
                                    .expect("DateTime conversion failed"),
                            )
                        },
                    }),
                    TimeUnit::Microsecond => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::DTime(
                                DateTime::<Utc>::from_timestamp_micros(v)
                                    .expect("DateTime conversion failed"),
                            )
                        },
                    }),
                    TimeUnit::Nanosecond => Box::new(DTimeCol {
                        arr,
                        map: |v| Prop::DTime(DateTime::<Utc>::from_timestamp_nanos(v)),
                    }),
                },
                None => match timeunit {
                    TimeUnit::Second => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::NDTime(
                                DateTime::from_timestamp(v, 0)
                                    .expect("DateTime conversion failed")
                                    .naive_utc(),
                            )
                        },
                    }),
                    TimeUnit::Millisecond => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::NDTime(
                                DateTime::from_timestamp_millis(v)
                                    .expect("DateTime conversion failed")
                                    .naive_utc(),
                            )
                        },
                    }),
                    TimeUnit::Microsecond => Box::new(DTimeCol {
                        arr,
                        map: |v| {
                            Prop::NDTime(
                                DateTime::from_timestamp_micros(v)
                                    .expect("DateTime conversion failed")
                                    .naive_utc(),
                            )
                        },
                    }),
                    TimeUnit::Nanosecond => Box::new(DTimeCol {
                        arr,
                        map: |v| Prop::NDTime(DateTime::from_timestamp_nanos(v).naive_utc()),
                    }),
                },
            }
        }
        unsupported => panic!("Data type not supported: {:?}", unsupported),
    }
}
