use crate::{
    core::entities::properties::prop::{
        unify_types, ArrowRow, DirectConvert, Prop, PropType, SerdeProp,
    },
    iter::{BoxedLIter, IntoDynBoxed},
};
use arrow_array::{
    cast::AsArray, types::*, Array, ArrayRef, ArrowPrimitiveType, OffsetSizeTrait, PrimitiveArray,
};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug, Clone, derive_more::From)]
pub enum PropArray {
    Vec(Arc<Vec<Prop>>),
    Array(ArrayRef),
}

impl Default for PropArray {
    fn default() -> Self {
        PropArray::Vec(vec![].into())
    }
}

impl From<Vec<Prop>> for PropArray {
    fn from(vec: Vec<Prop>) -> Self {
        PropArray::Vec(Arc::from(vec))
    }
}

impl Hash for PropArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PropArray::Array(array) => {
                let data = array.to_data();
                let dtype = array.data_type();
                dtype.hash(state);
                data.offset().hash(state);
                data.len().hash(state);
                for buffer in data.buffers() {
                    buffer.hash(state);
                }
            }
            PropArray::Vec(ps) => {
                ps.hash(state);
            }
        }
    }
}

impl PropArray {
    pub fn len(&self) -> usize {
        match self {
            PropArray::Array(arr) => arr.len(),
            PropArray::Vec(ps) => ps.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            PropArray::Vec(ps) => ps.is_empty(),
            PropArray::Array(arr) => arr.is_empty(),
        }
    }

    pub fn dtype(&self) -> PropType {
        match self {
            PropArray::Vec(ps) if ps.is_empty() => PropType::Empty,
            PropArray::Vec(ps) => ps
                .iter()
                .map(|p| p.dtype())
                .reduce(|dt1, dt2| {
                    unify_types(&dt1, &dt2, &mut false)
                        .unwrap_or_else(|e| panic!("Failed to unify props {e}"))
                })
                .unwrap(),
            PropArray::Array(a) => PropType::from(a.data_type()),
        }
    }

    pub fn into_array_ref(self) -> Option<ArrayRef> {
        match self {
            PropArray::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn as_array_ref(&self) -> Option<&ArrayRef> {
        match self {
            PropArray::Array(arr) => Some(arr),
            _ => None,
        }
    }

    // TODO: need something that returns PropRef instead to avoid allocations
    pub fn iter(&self) -> impl Iterator<Item = Prop> + '_ {
        self.iter_all().flatten()
    }

    pub fn iter_all(&self) -> BoxedLIter<'_, Option<Prop>> {
        match self {
            PropArray::Vec(ps) => ps.iter().cloned().map(Some).into_dyn_boxed(),
            PropArray::Array(arr) => {
                let dtype = arr.data_type();
                match dtype {
                    DataType::Boolean => arr
                        .as_boolean()
                        .iter()
                        .map(|p| p.map(Prop::Bool))
                        .into_dyn_boxed(),
                    DataType::Int32 => as_primitive_iter::<Int32Type>(arr),
                    DataType::Int64 => as_primitive_iter::<Int64Type>(arr),
                    DataType::UInt8 => as_primitive_iter::<UInt8Type>(arr),
                    DataType::UInt16 => as_primitive_iter::<UInt16Type>(arr),
                    DataType::UInt32 => as_primitive_iter::<UInt32Type>(arr),
                    DataType::UInt64 => as_primitive_iter::<UInt64Type>(arr),
                    DataType::Float32 => as_primitive_iter::<Float32Type>(arr),
                    DataType::Float64 => as_primitive_iter::<Float64Type>(arr),
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => as_primitive_iter::<TimestampSecondType>(arr),
                        TimeUnit::Millisecond => as_primitive_iter::<TimestampMillisecondType>(arr),
                        TimeUnit::Microsecond => as_primitive_iter::<TimestampMicrosecondType>(arr),
                        TimeUnit::Nanosecond => as_primitive_iter::<TimestampNanosecondType>(arr),
                    },
                    DataType::Date32 => as_primitive_iter::<Date32Type>(arr),
                    DataType::Date64 => as_primitive_iter::<Date64Type>(arr),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => as_str_iter(arr),
                    DataType::Decimal128(_, _) => as_primitive_iter::<Decimal128Type>(arr),
                    DataType::Struct(_) => as_struct_iter(arr),
                    DataType::List(_) => as_list_iter::<i32>(arr),
                    DataType::LargeList(_) => as_list_iter::<i64>(arr),
                    _ => std::iter::empty().into_dyn_boxed(),
                }
            }
        }
    }
}

fn as_primitive_iter<TT: DirectConvert>(arr: &ArrayRef) -> BoxedLIter<'_, Option<Prop>> {
    arr.as_primitive_opt::<TT>()
        .into_iter()
        .flat_map(|primitive_array| {
            let dt = arr.data_type();
            primitive_array.iter().map(|v| v.map(|v| TT::prop(v, dt)))
        })
        .into_dyn_boxed()
}

fn as_str_iter(arr: &ArrayRef) -> BoxedLIter<'_, Option<Prop>> {
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_string::<i32>()
            .into_iter()
            .map(|opt_str| opt_str.map(|s| Prop::str(s.to_string())))
            .into_dyn_boxed(),
        DataType::LargeUtf8 => arr
            .as_string::<i64>()
            .into_iter()
            .map(|opt_str| opt_str.map(|s| Prop::str(s.to_string())))
            .into_dyn_boxed(),
        DataType::Utf8View => arr
            .as_string_view()
            .into_iter()
            .map(|opt_str| opt_str.map(|s| Prop::str(s.to_string())))
            .into_dyn_boxed(),
        _ => panic!("as_str_iter called on non-string array"),
    }
}

fn as_struct_iter(arr: &ArrayRef) -> BoxedLIter<'_, Option<Prop>> {
    let arr = arr.as_struct();
    (0..arr.len())
        .map(|row| (!arr.is_null(row)).then(|| ArrowRow::new(arr, row)))
        .map(|arrow_row| arrow_row.and_then(|row| row.into_prop()))
        .into_dyn_boxed()
}

fn as_list_iter<O: OffsetSizeTrait>(arr: &ArrayRef) -> BoxedLIter<'_, Option<Prop>> {
    let arr = arr.as_list::<O>();
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                let value_array = arr.value(i);
                let prop_array = PropArray::Array(value_array);
                Some(Prop::List(prop_array))
            }
        })
        .into_dyn_boxed()
}

impl Serialize for PropArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_seq(Some(self.len()))?;
        for prop in self.iter_all() {
            state.serialize_element(&prop.as_ref().map(SerdeProp))?;
        }
        state.end()
    }
}

impl PartialEq for PropArray {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropArray::Vec(l), PropArray::Vec(r)) => l.eq(r),
            (PropArray::Array(a), PropArray::Array(b)) => a.eq(b),
            _ => {
                let mut l_iter = self.iter_all();
                let mut r_iter = other.iter_all();
                loop {
                    match (l_iter.next(), r_iter.next()) {
                        (Some(lv), Some(rv)) => {
                            if lv != rv {
                                return false;
                            }
                        }
                        (None, None) => return true,
                        _ => return false,
                    }
                }
            }
        }
    }
}

impl PartialOrd for PropArray {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (PropArray::Vec(l), PropArray::Vec(r)) => l.partial_cmp(r),
            _ => {
                let mut l_iter = self.iter_all();
                let mut r_iter = other.iter_all();
                loop {
                    match (l_iter.next(), r_iter.next()) {
                        (Some(lv), Some(rv)) => match lv.partial_cmp(&rv) {
                            Some(std::cmp::Ordering::Equal) => continue,
                            other => return other,
                        },
                        (None, None) => return Some(std::cmp::Ordering::Equal),
                        (None, Some(_)) => return Some(std::cmp::Ordering::Less),
                        (Some(_), None) => return Some(std::cmp::Ordering::Greater),
                    }
                }
            }
        }
    }
}

impl Prop {
    pub fn from_arr<TT: ArrowPrimitiveType>(vals: Vec<TT::Native>) -> Self
    where
        PrimitiveArray<TT>: From<Vec<TT::Native>>,
    {
        let array = PrimitiveArray::<TT>::from(vals);
        Prop::List(PropArray::Array(Arc::new(array)))
    }
}

pub fn arrow_dtype_from_prop_type(prop_type: &PropType) -> DataType {
    match prop_type {
        PropType::Str => DataType::Utf8View,
        PropType::U8 => DataType::UInt8,
        PropType::U16 => DataType::UInt16,
        PropType::I32 => DataType::Int32,
        PropType::I64 => DataType::Int64,
        PropType::U32 => DataType::UInt32,
        PropType::U64 => DataType::UInt64,
        PropType::F32 => DataType::Float32,
        PropType::F64 => DataType::Float64,
        PropType::Bool => DataType::Boolean,
        PropType::NDTime => DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        PropType::DTime => {
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
        }
        PropType::List(d_type) => {
            DataType::LargeList(Field::new("data", arrow_dtype_from_prop_type(d_type), true).into())
        }
        PropType::Map(d_type) => {
            let fields = d_type
                .iter()
                .map(|(k, v)| Field::new(k.to_string(), arrow_dtype_from_prop_type(v), true))
                .collect::<Vec<_>>();
            if fields.is_empty() {
                DataType::Struct(Fields::from_iter([Field::new(
                    "__empty__",
                    DataType::Null,
                    true,
                )]))
            } else {
                DataType::Struct(fields.into())
            }
        }
        // 38 comes from here: https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
        PropType::Decimal { scale } => DataType::Decimal128(38, (*scale).try_into().unwrap()),
        PropType::Empty => {
            // this is odd, we'll just pick one and hope for the best
            DataType::Null
        }
    }
}

pub trait PropArrayUnwrap: Sized {
    fn into_array(self) -> Option<ArrayRef>;
    fn unwrap_array(self) -> ArrayRef {
        self.into_array().unwrap()
    }
}

impl<P: PropArrayUnwrap> PropArrayUnwrap for Option<P> {
    fn into_array(self) -> Option<ArrayRef> {
        self.and_then(|p| p.into_array())
    }
}

impl PropArrayUnwrap for Prop {
    fn into_array(self) -> Option<ArrayRef> {
        if let Prop::List(v) = self {
            v.into_array_ref()
        } else {
            None
        }
    }
}
