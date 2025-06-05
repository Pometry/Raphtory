use crate::{
    errors::{GraphError, LoadError},
    io::arrow::dataframe::DFChunk,
    prelude::Prop,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use polars_arrow::{
    array::{
        Array, BooleanArray, FixedSizeListArray, ListArray, PrimitiveArray, StaticArray,
        StructArray, Utf8Array, Utf8ViewArray,
    },
    bitmap::Bitmap,
    datatypes::{ArrowDataType as DataType, TimeUnit},
    offset::Offset,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoPropList, PropType},
    storage::{arc_str::ArcStr, dict_mapper::MaybeNew},
};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

pub struct PropCols {
    prop_ids: Vec<usize>,
    cols: Vec<Box<dyn PropCol>>,
    len: usize,
}

impl PropCols {
    pub fn iter_row(&self, i: usize) -> impl Iterator<Item = (usize, Prop)> + '_ {
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
    props: &[impl AsRef<str>],
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
        .map(|(name, dtype)| Ok(prop_id_resolver(name.as_ref(), dtype)?.inner()))
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
        DataType::Utf8View => {
            let arr = arr.as_any().downcast_ref::<Utf8ViewArray>().unwrap();
            arr.iter().flatten().into_prop_list()
        }
        DataType::List(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::LargeList(_) => {
            let arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap_or_else(|| panic!("Expected TimestampMillisecondArray, got {:?}", arr));
            arr.iter()
                .flatten()
                .map(|elem| Prop::DTime(DateTime::<Utc>::from_timestamp_millis(*elem).unwrap()))
                .into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let arr = arr
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap_or_else(|| panic!("Expected TimestampMillisecondArray, got {:?}", arr));
            arr.iter()
                .flatten()
                .map(|elem| {
                    Prop::NDTime(DateTime::from_timestamp_millis(*elem).unwrap().naive_utc())
                })
                .into_prop_list()
        }
        DataType::Struct(_) => {
            let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
            let cols = arr
                .values()
                .iter()
                .map(|arr| lift_property_col(arr.as_ref()))
                .collect::<Vec<_>>();

            let mut props = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                let fields = cols
                    .iter()
                    .zip(arr.fields())
                    .filter_map(|(col, field)| {
                        col.get(i)
                            .map(|prop| (ArcStr::from(field.name.as_str()), prop))
                    })
                    .collect::<FxHashMap<_, _>>();
                props.push(Prop::Map(fields.into()));
            }

            props.into_prop_list()
        }
        DataType::Decimal(precision, scale) if *precision <= 38 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();
            arr.iter()
                .flatten()
                .map(|elem| Prop::Decimal(BigDecimal::new((*elem).into(), *scale as i64)))
                .into_prop_list()
        }
        DataType::Null => Prop::List(vec![].into()),
        dt => panic!("Data type not recognized {dt:?}"),
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
        DataType::Utf8View => Ok(PropType::Str),
        DataType::Struct(fields) => Ok(PropType::map(fields.iter().filter_map(|f| {
            data_type_as_prop_type(f.data_type())
                .ok()
                .map(move |pt| (&f.name, pt))
        }))),
        DataType::List(v) => Ok(PropType::List(Box::new(data_type_as_prop_type(
            v.data_type(),
        )?))),
        DataType::FixedSizeList(v, _) => Ok(PropType::List(Box::new(data_type_as_prop_type(
            v.data_type(),
        )?))),
        DataType::LargeList(v) => Ok(PropType::List(Box::new(data_type_as_prop_type(
            v.data_type(),
        )?))),
        DataType::Timestamp(_, v) => match v {
            None => Ok(PropType::NDTime),
            Some(_) => Ok(PropType::DTime),
        },
        DataType::Decimal(precision, scale) if *precision <= 38 => Ok(PropType::Decimal {
            scale: *scale as i64,
        }),
        DataType::Null => Ok(PropType::Empty),
        _ => Err(LoadError::InvalidPropertyType(dt.clone()).into()),
    }
}

trait PropCol: Send + Sync {
    fn get(&self, i: usize) -> Option<Prop>;
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
}

struct Wrap<A>(A);

impl PropCol for Wrap<Utf8Array<i32>> {
    fn get(&self, i: usize) -> Option<Prop> {
        self.0.get(i).map(Prop::str)
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
}

impl PropCol for Wrap<FixedSizeListArray> {
    fn get(&self, i: usize) -> Option<Prop> {
        self.0.get(i).map(arr_as_prop)
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
}

struct DecimalPropCol {
    arr: PrimitiveArray<i128>,
    scale: i64,
}

impl PropCol for DecimalPropCol {
    fn get(&self, i: usize) -> Option<Prop> {
        StaticArray::get(&self.arr, i).map(|v| Prop::Decimal(BigDecimal::new(v.into(), self.scale)))
    }
}

struct EmptyCol;

impl PropCol for EmptyCol {
    fn get(&self, _i: usize) -> Option<Prop> {
        None
    }
}

struct MapCol {
    validity: Option<Bitmap>,
    values: Vec<(String, Box<dyn PropCol>)>,
}

impl MapCol {
    fn new(arr: &dyn Array) -> Self {
        let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let validity = arr.validity().cloned();
        let values = arr
            .fields()
            .iter()
            .zip(arr.values())
            .map(|(field, col)| (field.name.clone(), lift_property_col(col.as_ref())))
            .collect();
        Self { validity, values }
    }
}

impl PropCol for MapCol {
    fn get(&self, i: usize) -> Option<Prop> {
        if self
            .validity
            .as_ref()
            .is_none_or(|validity| validity.get_bit(i))
        {
            Some(Prop::map(self.values.iter().filter_map(|(field, col)| {
                Some((field.as_str(), col.get(i)?))
            })))
        } else {
            None
        }
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
        DataType::Struct(_) => Box::new(MapCol::new(arr)),
        DataType::Utf8View => {
            let arr = arr.as_any().downcast_ref::<Utf8ViewArray>().unwrap();
            Box::new(arr.clone())
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
        DataType::Decimal(precision, scale) if *precision <= 38 => {
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();
            Box::new(DecimalPropCol {
                arr: arr.clone(),
                scale: *scale as i64,
            })
        }
        DataType::Null => Box::new(EmptyCol),
        unsupported => panic!("Data type not supported: {:?}", unsupported),
    }
}
