use arrow_array::{
    cast::AsArray, types::*, Array, ArrowPrimitiveType, OffsetSizeTrait, StructArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::DateTime;
use serde::{ser::SerializeMap, Serialize};

use crate::core::entities::properties::prop::{Prop, PropRef};

#[derive(Debug, Clone, Copy)]
pub struct ArrowRow<'a> {
    array: &'a StructArray,
    index: usize,
}

impl<'a> PartialEq for ArrowRow<'a> {
    // this has the downside of returning false for rows with same fields but different order of columns
    fn eq(&self, other: &Self) -> bool {
        if self.array.num_columns() != other.array.num_columns() {
            return false;
        }

        //FIXME: it could be that the fields don't match in order but the values are the same
        for col in 0..self.array.num_columns() {
            let self_prop = self.prop_ref(col);
            let other_prop = other.prop_ref(col);
            if self_prop != other_prop {
                return false;
            }
        }
        true
    }
}

impl<'a> Serialize for ArrowRow<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.array.num_columns()))?;
        for col in 0..self.array.num_columns() {
            let field = &self.array.fields()[col];
            let key = field.name();
            let value = self.prop_ref(col);
            state.serialize_entry(key, &value)?;
        }
        state.end()
    }
}

impl<'a> ArrowRow<'a> {
    pub fn primitive_value<T: ArrowPrimitiveType>(&self, col: usize) -> Option<T::Native> {
        let primitive_array = self.array.column(col).as_primitive_opt::<T>()?;
        (primitive_array.len() > self.index && !primitive_array.is_null(self.index))
            .then(|| primitive_array.value(self.index))
    }

    fn primitive_dt<T: DirectConvert>(&self, col: usize) -> Option<(T::Native, &DataType)> {
        let col = self.array.column(col).as_primitive_opt::<T>()?;
        (col.len() > self.index && !col.is_null(self.index))
            .then(|| (col.value(self.index), col.data_type()))
    }

    fn primitive_prop<T: DirectConvert>(&self, col: usize) -> Option<Prop> {
        let (value, dt) = self.primitive_dt::<T>(col)?;
        let prop = T::prop(value, dt);
        Some(prop)
    }

    fn primitive_prop_ref<T: DirectConvert>(self, col: usize) -> Option<PropRef<'static>> {
        let col = self.array.column(col).as_primitive_opt::<T>()?;
        let (value, dt) = (col.len() > self.index && !col.is_null(self.index))
            .then(|| (col.value(self.index), col.data_type()))?;
        let prop_ref = T::prop_ref(value, dt);
        Some(prop_ref)
    }

    fn struct_prop(&self, col: usize) -> Option<Prop> {
        let col = self.array.column(col).as_struct_opt()?;
        let row = ArrowRow::new(col, self.index);
        if col.len() > self.index && !col.is_null(self.index) {
            row.into_prop()
        } else {
            None
        }
    }

    fn list_prop<O: OffsetSizeTrait>(&self, col: usize) -> Option<Prop> {
        let col = self.array.column(col).as_list_opt::<O>()?;
        let row = col.value(self.index);
        if col.len() > self.index && !col.is_null(self.index) {
            Some(row.into())
        } else {
            None
        }
    }

    fn struct_prop_ref(&self, col: usize) -> Option<PropRef<'a>> {
        let column = self.array.column(col).as_struct_opt()?;
        if self.index < column.len() && column.is_valid(self.index) {
            let row = ArrowRow::new(column, self.index);
            Some(PropRef::from(row))
        } else {
            None
        }
    }

    pub fn bool_value(&self, col: usize) -> Option<bool> {
        let column = self.array.column(col);
        match column.data_type() {
            DataType::Boolean => {
                let col = column.as_boolean();
                (col.len() > self.index && !col.is_null(self.index)).then(|| col.value(self.index))
            }
            _ => None,
        }
    }

    pub fn str_value(self, col: usize) -> Option<&'a str> {
        let column = self.array.column(col);
        let len = column.len();
        let valid = len > self.index && !column.is_null(self.index);
        match column.data_type() {
            DataType::Utf8 => valid.then(|| column.as_string::<i32>().value(self.index)),
            DataType::LargeUtf8 => valid.then(|| column.as_string::<i64>().value(self.index)),
            DataType::Utf8View => valid.then(|| column.as_string_view().value(self.index)),
            _ => None,
        }
    }

    pub fn prop_value(self, col: usize) -> Option<Prop> {
        let dtype = self.array.fields().get(col)?.data_type();
        match dtype {
            DataType::Null => None,
            DataType::Boolean => self.bool_value(col).map(|b| b.into()),
            DataType::Int32 => self.primitive_prop::<Int32Type>(col),
            DataType::Int64 => self.primitive_prop::<Int64Type>(col),
            DataType::UInt8 => self.primitive_prop::<UInt8Type>(col),
            DataType::UInt16 => self.primitive_prop::<UInt16Type>(col),
            DataType::UInt32 => self.primitive_prop::<UInt32Type>(col),
            DataType::UInt64 => self.primitive_prop::<UInt64Type>(col),
            DataType::Float32 => self.primitive_prop::<Float32Type>(col),
            DataType::Float64 => self.primitive_prop::<Float64Type>(col),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => self.primitive_prop::<TimestampSecondType>(col),
                TimeUnit::Millisecond => self.primitive_prop::<TimestampMillisecondType>(col),
                TimeUnit::Microsecond => self.primitive_prop::<TimestampMicrosecondType>(col),
                TimeUnit::Nanosecond => self.primitive_prop::<TimestampNanosecondType>(col),
            },
            DataType::Date32 => self.primitive_prop::<Date32Type>(col),
            DataType::Date64 => self.primitive_prop::<Date64Type>(col),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                self.str_value(col).map(|v| v.into())
            }
            DataType::Decimal128(_, _) => self.primitive_prop::<Decimal128Type>(col),
            DataType::Struct(_) => self.struct_prop(col),
            DataType::List(_) => self.list_prop::<i32>(col),
            DataType::LargeList(_) => self.list_prop::<i64>(col),
            _ => None,
        }
    }

    pub fn prop_ref(self, col: usize) -> Option<PropRef<'a>> {
        let dtype = self.array.fields().get(col)?.data_type();
        match dtype {
            DataType::Null => None,
            DataType::Boolean => self.bool_value(col).map(|b| b.into()),
            DataType::Int32 => self.primitive_prop_ref::<Int32Type>(col),
            DataType::Int64 => self.primitive_prop_ref::<Int64Type>(col),
            DataType::UInt8 => self.primitive_prop_ref::<UInt8Type>(col),
            DataType::UInt16 => self.primitive_prop_ref::<UInt16Type>(col),
            DataType::UInt32 => self.primitive_prop_ref::<UInt32Type>(col),
            DataType::UInt64 => self.primitive_prop_ref::<UInt64Type>(col),
            DataType::Float32 => self.primitive_prop_ref::<Float32Type>(col),
            DataType::Float64 => self.primitive_prop_ref::<Float64Type>(col),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => self.primitive_prop_ref::<TimestampSecondType>(col),
                TimeUnit::Millisecond => self.primitive_prop_ref::<TimestampMillisecondType>(col),
                TimeUnit::Microsecond => self.primitive_prop_ref::<TimestampMicrosecondType>(col),
                TimeUnit::Nanosecond => self.primitive_prop_ref::<TimestampNanosecondType>(col),
            },
            DataType::Date32 => self.primitive_prop_ref::<Date32Type>(col),
            DataType::Date64 => self.primitive_prop_ref::<Date64Type>(col),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                self.str_value(col).map(|v| v.into())
            }
            DataType::Decimal128(_, _) => self.primitive_prop_ref::<Decimal128Type>(col),
            DataType::Struct(_) => self.struct_prop_ref(col),
            _ => None,
        }
    }

    pub fn into_prop(self) -> Option<Prop> {
        if self.index >= self.array.len() || self.array.is_null(self.index) {
            None
        } else {
            let map = Prop::map(
                self.array
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(col, field)| {
                        Some((field.name().as_ref(), self.prop_value(col)?))
                    }),
            );
            Some(map)
        }
    }

    pub fn is_valid(&self, col: usize) -> bool {
        self.array.column(col).is_valid(self.index)
    }
}

impl<'a> ArrowRow<'a> {
    pub fn new(array: &'a StructArray, index: usize) -> Self {
        Self { array, index }
    }

    pub fn get<T: 'static>(&self, column: usize) -> Option<&T> {
        self.array.column(column).as_any().downcast_ref()
    }
}

pub trait DirectConvert: ArrowPrimitiveType {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static>;
    fn prop(native: Self::Native, dtype: &DataType) -> Prop {
        Self::prop_ref(native, dtype).into()
    }
}

impl DirectConvert for UInt8Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for UInt16Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for UInt32Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for UInt64Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for Int32Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for Int64Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for Float32Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for Float64Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(native)
    }
}

impl DirectConvert for Date64Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(DateTime::from_timestamp_millis(native).unwrap())
    }
}

impl DirectConvert for Date32Type {
    fn prop_ref(native: Self::Native, _dtype: &DataType) -> PropRef<'static> {
        PropRef::from(
            Date32Type::to_naive_date(native)
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc(),
        )
    }
}

impl DirectConvert for TimestampNanosecondType {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static> {
        match dtype {
            DataType::Timestamp(_, tz) => match tz {
                None => PropRef::from(DateTime::from_timestamp_nanos(native).naive_utc()),
                Some(_) => PropRef::from(DateTime::from_timestamp_nanos(native)),
            },
            _ => unreachable!(),
        }
    }
}

impl DirectConvert for TimestampMicrosecondType {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static> {
        match dtype {
            DataType::Timestamp(_, tz) => match tz {
                None => PropRef::from(DateTime::from_timestamp_micros(native).unwrap().naive_utc()),
                Some(_) => PropRef::from(DateTime::from_timestamp_micros(native).unwrap()),
            },
            _ => unreachable!(),
        }
    }
}

impl DirectConvert for TimestampMillisecondType {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static> {
        match dtype {
            DataType::Timestamp(_, tz) => match tz {
                None => PropRef::from(DateTime::from_timestamp_millis(native).unwrap().naive_utc()),
                Some(_) => PropRef::from(DateTime::from_timestamp_millis(native).unwrap()),
            },
            _ => unreachable!(),
        }
    }
}

impl DirectConvert for TimestampSecondType {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static> {
        match dtype {
            DataType::Timestamp(_, tz) => match tz {
                None => PropRef::from(DateTime::from_timestamp(native, 0).unwrap().naive_utc()),
                Some(_) => PropRef::from(DateTime::from_timestamp(native, 0).unwrap()),
            },
            _ => unreachable!(),
        }
    }
}

impl DirectConvert for Decimal128Type {
    fn prop_ref(native: Self::Native, dtype: &DataType) -> PropRef<'static> {
        match dtype {
            DataType::Decimal128(_, scale) => PropRef::Decimal {
                num: native,
                scale: *scale as i8,
            },
            _ => unreachable!(),
        }
    }
}
