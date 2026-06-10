use crate::{
    core::{
        entities::properties::prop::{IntoPropList, Prop, PropArray, PropMapRef, PropNum, PropRef},
        storage::arc_str::ArcStr,
    },
    iter::IntoDynBoxed,
};
use arrow_array::{
    cast::AsArray,
    types::{
        Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Array, FixedSizeListArray,
    GenericListArray, GenericStringArray, NullArray, OffsetSizeTrait, PrimitiveArray,
    StringViewArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, TimeUnit};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use std::{borrow::Cow, sync::Arc};

pub trait PropCol: Send + Sync + std::fmt::Debug {
    fn get(&self, i: usize) -> Option<Prop>;

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>>;

    fn as_array(&self) -> ArrayRef;

    fn iter(&self) -> Box<dyn Iterator<Item = Option<Prop>> + '_> {
        (0..self.as_array().len())
            .map(move |i| self.get(i))
            .into_dyn_boxed()
    }

    fn iter_ref(&self) -> Box<dyn Iterator<Item = Option<PropRef<'_>>> + '_> {
        (0..self.as_array().len())
            .map(move |i| self.get_ref(i))
            .into_dyn_boxed()
    }
}

#[derive(Debug)]
pub struct MapCol {
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

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self
            .validity
            .as_ref()
            .is_none_or(|validity| validity.is_valid(i))
        {
            Some(PropRef::Map(PropMapRef::PropCol { map: self, i }))
        } else {
            None
        }
    }

    fn as_array(&self) -> ArrayRef {
        let fields = self
            .values
            .iter()
            .map(|(name, col)| Field::new(name, col.as_array().data_type().clone(), true))
            .collect::<Vec<_>>();
        let columns = self.values.iter().map(|(_, col)| col.as_array()).collect();
        Arc::new(StructArray::new(
            fields.into(),
            columns,
            self.validity.clone(),
        ))
    }
}

impl PropCol for BooleanArray {
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(Prop::Bool(self.value(i)))
        }
    }

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::Bool(self.value(i)))
        }
    }

    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<Prop>> + '_> {
        self.iter().map(|opt| opt.map(Prop::Bool)).into_dyn_boxed()
    }
}

impl<T: ArrowPrimitiveType> PropCol for PrimitiveArray<T>
where
    T::Native: Into<Prop> + Into<PropNum>,
{
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(self.value(i).into())
        }
    }

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::Num(self.value(i).into()))
        }
    }

    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<Prop>> + '_> {
        self.iter()
            .map(|opt| opt.map(|v| v.into()))
            .into_dyn_boxed()
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

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::Str(self.value(i)))
        }
    }
    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<Prop>> + '_> {
        self.iter().map(|opt| opt.map(Prop::str)).into_dyn_boxed()
    }
}

impl PropCol for StringViewArray {
    fn get(&self, i: usize) -> Option<Prop> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(Prop::str(self.value(i)))
        }
    }

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::Str(self.value(i)))
        }
    }
    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<Prop>> + '_> {
        self.iter().map(|opt| opt.map(Prop::str)).into_dyn_boxed()
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

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::List(Cow::Owned(self.value(i).into())))
        }
    }
    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
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

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if self.is_null(i) || self.len() <= i {
            None
        } else {
            Some(PropRef::List(Cow::Owned(self.value(i).into())))
        }
    }
    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }
}

impl PropCol for NullArray {
    fn get(&self, _i: usize) -> Option<Prop> {
        None
    }

    fn get_ref(&self, _i: usize) -> Option<PropRef<'_>> {
        None
    }
    fn as_array(&self) -> ArrayRef {
        Arc::new(self.clone())
    }
}

#[derive(Debug)]
struct MappedPrimitiveCol<T: ArrowPrimitiveType> {
    arr: PrimitiveArray<T>,
    map: fn(T::Native) -> PropRef<'static>,
}

impl<T: ArrowPrimitiveType + std::fmt::Debug> PropCol for MappedPrimitiveCol<T> {
    fn get(&self, i: usize) -> Option<Prop> {
        self.get_ref(i).map(|p_ref| p_ref.into())
    }

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if i >= self.arr.len() || self.arr.is_null(i) {
            None
        } else {
            Some((self.map)(self.arr.value(i)))
        }
    }

    fn as_array(&self) -> ArrayRef {
        Arc::new(self.arr.clone())
    }
}

#[derive(Debug)]
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

    fn get_ref(&self, i: usize) -> Option<PropRef<'_>> {
        if i >= self.arr.len() || self.arr.is_null(i) {
            None
        } else {
            Some(PropRef::Decimal {
                num: self.arr.value(i).into(),
                scale: self.scale as i8,
            })
        }
    }

    fn as_array(&self) -> ArrayRef {
        Arc::new(self.arr.clone())
    }
}

#[derive(Debug)]
struct EmptyCol;

impl PropCol for EmptyCol {
    fn get(&self, _i: usize) -> Option<Prop> {
        None
    }

    fn get_ref(&self, _i: usize) -> Option<PropRef<'_>> {
        None
    }

    fn as_array(&self) -> ArrayRef {
        Arc::new(NullArray::new(0))
    }
}
pub fn lift_property_col(arr: &dyn Array) -> Box<dyn PropCol> {
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
                        PropRef::DTime(
                            DateTime::<Utc>::from_timestamp(v, 0)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Millisecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMillisecondType>().clone(),
                    map: |v| {
                        PropRef::DTime(
                            DateTime::<Utc>::from_timestamp_millis(v)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Microsecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMicrosecondType>().clone(),
                    map: |v| {
                        PropRef::DTime(
                            DateTime::<Utc>::from_timestamp_micros(v)
                                .expect("DateTime conversion failed"),
                        )
                    },
                }),
                TimeUnit::Nanosecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampNanosecondType>().clone(),
                    map: |v| PropRef::DTime(DateTime::<Utc>::from_timestamp_nanos(v)),
                }),
            },
            None => match timeunit {
                TimeUnit::Second => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampSecondType>().clone(),
                    map: |v| {
                        PropRef::NDTime(
                            DateTime::from_timestamp(v, 0)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Millisecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMillisecondType>().clone(),
                    map: |v| {
                        PropRef::NDTime(
                            DateTime::from_timestamp_millis(v)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Microsecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampMicrosecondType>().clone(),
                    map: |v| {
                        PropRef::NDTime(
                            DateTime::from_timestamp_micros(v)
                                .expect("DateTime conversion failed")
                                .naive_utc(),
                        )
                    },
                }),
                TimeUnit::Nanosecond => Box::new(MappedPrimitiveCol {
                    arr: arr.as_primitive::<TimestampNanosecondType>().clone(),
                    map: |v| PropRef::NDTime(DateTime::from_timestamp_nanos(v).naive_utc()),
                }),
            },
        },
        DataType::Date32 => Box::new(MappedPrimitiveCol {
            arr: arr.as_primitive::<Date32Type>().clone(),
            map: |days| {
                let ms = (days as i64) * 86_400_000; // convert days to ms
                PropRef::NDTime(
                    DateTime::from_timestamp_millis(ms)
                        .expect("DateTime conversion failed for Date32 type")
                        .naive_utc(),
                )
            },
        }),
        DataType::Date64 => Box::new(MappedPrimitiveCol {
            arr: arr.as_primitive::<Date64Type>().clone(),
            map: |ms| {
                PropRef::NDTime(
                    DateTime::from_timestamp_millis(ms)
                        .expect("DateTime conversion failed for Date64 type")
                        .naive_utc(),
                )
            },
        }),
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
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let map_fn = if tz.is_some() {
                |elem: i64| Prop::DTime(DateTime::<Utc>::from_timestamp_secs(elem).unwrap())
            } else {
                |elem: i64| Prop::NDTime(DateTime::from_timestamp_secs(elem).unwrap().naive_utc())
            };
            let arr = arr.as_primitive::<TimestampSecondType>();
            arr.iter().flatten().map(map_fn).into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let map_fn = if tz.is_some() {
                |elem: i64| Prop::DTime(DateTime::<Utc>::from_timestamp_millis(elem).unwrap())
            } else {
                |elem: i64| Prop::NDTime(DateTime::from_timestamp_millis(elem).unwrap().naive_utc())
            };
            let arr = arr.as_primitive::<TimestampMillisecondType>();
            arr.iter().flatten().map(map_fn).into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let map_fn = if tz.is_some() {
                |elem: i64| Prop::DTime(DateTime::<Utc>::from_timestamp_micros(elem).unwrap())
            } else {
                |elem: i64| Prop::NDTime(DateTime::from_timestamp_micros(elem).unwrap().naive_utc())
            };
            let arr = arr.as_primitive::<TimestampMicrosecondType>();
            arr.iter().flatten().map(map_fn).into_prop_list()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let map_fn = if tz.is_some() {
                |elem: i64| Prop::DTime(DateTime::<Utc>::from_timestamp_nanos(elem))
            } else {
                |elem: i64| Prop::NDTime(DateTime::from_timestamp_nanos(elem).naive_utc())
            };
            let arr = arr.as_primitive::<TimestampNanosecondType>();
            arr.iter().flatten().map(map_fn).into_prop_list()
        }
        DataType::Date32 => {
            let arr = arr.as_primitive::<Date32Type>();
            arr.iter()
                .flatten()
                .map(|days| {
                    let ms = (days as i64) * 86_400_000;
                    Prop::NDTime(
                        DateTime::from_timestamp_millis(ms)
                            .expect("DateTime conversion failed for Date32 type")
                            .naive_utc(),
                    )
                })
                .into_prop_list()
        }
        DataType::Date64 => {
            let arr = arr.as_primitive::<Date64Type>();
            arr.iter()
                .flatten()
                .map(|ms| {
                    Prop::NDTime(
                        DateTime::from_timestamp_millis(ms)
                            .expect("DateTime conversion failed for Date64 type")
                            .naive_utc(),
                    )
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
        DataType::Null => Prop::List(PropArray::default()),
        dt => panic!("Data type not recognized {dt:?}"),
    }
}
