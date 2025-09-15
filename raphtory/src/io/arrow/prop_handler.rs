use crate::{
    errors::{GraphError, LoadError},
    io::arrow::dataframe::DFChunk,
    prelude::Prop,
};
use arrow_array::{
    cast::AsArray,
    types::{
        Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
    Array as ArrowArray, Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Array,
    FixedSizeListArray, GenericListArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
    StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, TimeUnit};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
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

pub fn combine_properties_arrow<E>(
    props: &[impl AsRef<str>],
    indices: &[usize],
    df: &DFChunk,
    prop_id_resolver: impl Fn(&str, PropType) -> Result<MaybeNew<usize>, E>,
) -> Result<PropCols, GraphError>
where
    GraphError: From<E>,
{
    let dtypes = indices
        .iter()
        .map(|idx| data_type_as_prop_type(df.chunk[*idx].data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let cols = indices
        .iter()
        .map(|idx| lift_property_col(&df.chunk[*idx]))
        .collect::<Vec<_>>();
    let prop_ids = props
        .iter()
        .zip(dtypes.into_iter())
        .map(|(name, dtype)| Ok(prop_id_resolver(name.as_ref(), dtype)?.inner()))
        .collect::<Result<Vec<_>, E>>()?;

    Ok(PropCols {
        prop_ids,
        cols,
        len: df.len(),
    })
}

fn arr_as_prop(arr: ArrayRef) -> Prop {
    match arr.data_type() {
        DataType::Boolean => {
            let arr = arr.as_boolean();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Int32 => {
            let arr = arr.as_primitive::<Int32Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Int64 => {
            let arr = arr.as_primitive::<Int64Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::UInt8 => {
            let arr = arr.as_primitive::<UInt8Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::UInt16 => {
            let arr = arr.as_primitive::<UInt16Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::UInt32 => {
            let arr = arr.as_primitive::<UInt32Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::UInt64 => {
            let arr = arr.as_primitive::<UInt64Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Float32 => {
            let arr = arr.as_primitive::<Float32Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Float64 => {
            let arr = arr.as_primitive::<Float64Type>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Utf8 => {
            let arr = arr.as_string::<i32>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::LargeUtf8 => {
            let arr = arr.as_string::<i64>();
            arr.iter().flatten().into_prop_list()
        }
        DataType::Utf8View => {
            let arr = arr.as_string_view();
            arr.iter().flatten().into_prop_list()
        }
        DataType::List(_) => {
            let arr = arr.as_list::<i32>();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = arr.as_fixed_size_list();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::LargeList(_) => {
            let arr = arr.as_list::<i64>();
            arr.iter().flatten().map(arr_as_prop).into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
            let arr = arr.as_primitive::<TimestampMillisecondType>();
            arr.iter()
                .flatten()
                .map(|elem| Prop::DTime(DateTime::<Utc>::from_timestamp_millis(elem).unwrap()))
                .into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let arr = arr.as_primitive::<TimestampMillisecondType>();
            arr.iter()
                .flatten()
                .map(|elem| {
                    Prop::NDTime(DateTime::from_timestamp_millis(elem).unwrap().naive_utc())
                })
                .into_prop_list()
        }
        DataType::Struct(_) => {
            let arr = arr.as_struct();
            let cols = arr
                .columns()
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
                            .map(|prop| (ArcStr::from(field.name().as_str()), prop))
                    })
                    .collect::<FxHashMap<_, _>>();
                props.push(Prop::Map(fields.into()));
            }

            props.into_prop_list()
        }
        DataType::Decimal128(precision, scale) if *precision <= 38 => {
            let arr = arr.as_primitive::<Decimal128Type>();
            arr.iter()
                .flatten()
                .map(|elem| Prop::Decimal(BigDecimal::new(elem.into(), *scale as i64)))
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
                .map(move |pt| (f.name(), pt))
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
        DataType::Decimal128(precision, scale) if *precision <= 38 => Ok(PropType::Decimal {
            scale: *scale as i64,
        }),
        DataType::Null => Ok(PropType::Empty),
        _ => Err(LoadError::InvalidPropertyType(dt.clone()).into()),
    }
}

trait PropCol: Send + Sync {
    fn get(&self, i: usize) -> Option<Prop>;
}

impl PropCol for BooleanArray {
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(Prop::Bool(self.value(i)))
        }
    }
}

impl<T: ArrowPrimitiveType> PropCol for PrimitiveArray<T>
where
    T::Native: Into<Prop>,
{
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(self.value(i).into())
        }
    }
}

impl<I: OffsetSizeTrait> PropCol for GenericStringArray<I> {
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(Prop::str(self.value(i)))
        }
    }
}

impl PropCol for arrow_array::StringViewArray {
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(Prop::str(self.value(i)))
        }
    }
}

impl<I: OffsetSizeTrait> PropCol for GenericListArray<I> {
    fn get(&self, i: usize) -> Option<Prop> {
        if i >= self.len() || self.is_null(i) {
            None
        } else {
            Some(arr_as_prop(self.value(i)))
        }
    }
}

impl PropCol for FixedSizeListArray {
    fn get(&self, i: usize) -> Option<Prop> {
        if i >= self.len() || self.is_null(i) {
            None
        } else {
            Some(arr_as_prop(self.value(i)))
        }
    }
}

struct EmptyCol;

impl PropCol for EmptyCol {
    fn get(&self, _i: usize) -> Option<Prop> {
        None
    }
}

struct MapCol {
    validity: Option<NullBuffer>,
    values: Vec<(String, Box<dyn PropCol>)>,
}

impl MapCol {
    fn new(arr: &StructArray) -> Self {
        let validity = arr.nulls().cloned();
        let values = arr
            .fields()
            .iter()
            .zip(arr.columns())
            .map(|(field, col)| (field.name().clone(), lift_property_col(col.as_ref())))
            .collect();
        Self { validity, values }
    }
}

impl PropCol for MapCol {
    fn get(&self, i: usize) -> Option<Prop> {
        if self
            .validity
            .as_ref()
            .is_none_or(|validity| validity.is_valid(i))
        {
            Some(Prop::map(self.values.iter().filter_map(|(field, col)| {
                Some((field.as_str(), col.get(i)?))
            })))
        } else {
            None
        }
    }
}

struct MappedPrimitiveCol<T: ArrowPrimitiveType> {
    arr: PrimitiveArray<T>,
    map: fn(T::Native) -> Prop,
}

impl<T: ArrowPrimitiveType> PropCol for MappedPrimitiveCol<T> {
    fn get(&self, i: usize) -> Option<Prop> {
        if i >= self.arr.len() || self.arr.is_null(i) {
            None
        } else {
            Some((self.map)(self.arr.value(i)))
        }
    }
}

struct DecimalPropCol {
    arr: Decimal128Array,
    scale: i64,
}

impl PropCol for DecimalPropCol {
    fn get(&self, i: usize) -> Option<Prop> {
        if i >= self.arr.len() || self.arr.is_null(i) {
            None
        } else {
            Some(Prop::Decimal(BigDecimal::new(
                self.arr.value(i).into(),
                self.scale,
            )))
        }
    }
}

fn lift_property_col(arr: &dyn Array) -> Box<dyn PropCol> {
    match arr.data_type() {
        DataType::Boolean => Box::new(arr.as_boolean().clone()),
        DataType::Int32 => Box::new(arr.as_primitive::<Int32Type>().clone()),
        DataType::Int64 => Box::new(arr.as_primitive::<Int64Type>().clone()),
        DataType::UInt8 => Box::new(arr.as_primitive::<UInt8Type>().clone()),
        DataType::UInt16 => Box::new(arr.as_primitive::<UInt16Type>().clone()),
        DataType::UInt32 => Box::new(arr.as_primitive::<UInt32Type>().clone()),
        DataType::UInt64 => Box::new(arr.as_primitive::<UInt64Type>().clone()),
        DataType::Float32 => Box::new(arr.as_primitive::<Float32Type>().clone()),
        DataType::Float64 => Box::new(arr.as_primitive::<Float64Type>().clone()),
        DataType::Utf8 => Box::new(arr.as_string::<i32>().clone()),
        DataType::LargeUtf8 => Box::new(arr.as_string::<i64>().clone()),
        DataType::Utf8View => Box::new(arr.as_string_view().clone()),
        DataType::List(_) => Box::new(arr.as_list::<i32>().clone()),
        DataType::LargeList(_) => Box::new(arr.as_list::<i64>().clone()),
        DataType::FixedSizeList(_, _) => Box::new(arr.as_fixed_size_list().clone()),
        DataType::Struct(_) => Box::new(MapCol::new(arr.as_struct())),
        DataType::Timestamp(timeunit, timezone) => match timezone {
            Some(_) => match timeunit {
                TimeUnit::Second => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampSecondType>().clone(),
                    map: |v| {
                        Prop::DTime(
                            DateTime::<Utc>::from_timestamp(v, 0)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Millisecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMillisecondType>().clone(),
                    map: |v| {
                        Prop::DTime(
                            DateTime::<Utc>::from_timestamp_millis(v)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Microsecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMicrosecondType>().clone(),
                    map: |v| {
                        Prop::DTime(
                            DateTime::<Utc>::from_timestamp_micros(v)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Nanosecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampNanosecondType>().clone(),
                    map: |v| Prop::DTime(DateTime::<Utc>::from_timestamp_nanos(v)),
                }),
            },
            None => match timeunit {
                TimeUnit::Second => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampSecondType>().clone(),
                    map: |v| {
                        Prop::NDTime(
                            DateTime::from_timestamp(v, 0)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Millisecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMillisecondType>().clone(),
                    map: |v| {
                        Prop::NDTime(
                            DateTime::from_timestamp_millis(v)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Microsecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMicrosecondType>().clone(),
                    map: |v| {
                        Prop::NDTime(
                            DateTime::from_timestamp_micros(v)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Nanosecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampNanosecondType>().clone(),
                    map: |v| Prop::NDTime(DateTime::from_timestamp_nanos(v).naive_utc()),
                }),
            },
        },
        DataType::Decimal128(precision, scale) if *precision <= 38 => {
            let arr = arr.as_primitive::<Decimal128Type>().clone();
            Box::new(DecimalPropCol {
                arr,
                scale: *scale as i64,
            })
        }
        DataType::Null => Box::new(EmptyCol),

        unsupported => panic!("Data type not supported: {:?}", unsupported),
    }
}
