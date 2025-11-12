use crate::core::{
    entities::{
        properties::prop::{prop_ref_enum::PropRef, PropNum, PropType},
        GidRef,
    },
    storage::arc_str::ArcStr,
};
use arrow_array::{cast::AsArray, ArrayRef, LargeListArray, StructArray};
#[cfg(feature = "arrow")]
use arrow_schema::{DataType, Field, FieldRef};
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use rustc_hash::{FxBuildHasher, FxHashMap};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize,
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
use thiserror::Error;

#[cfg(feature = "arrow")]
use crate::core::entities::properties::prop::prop_array::*;

pub const DECIMAL_MAX: i128 = 99999999999999999999999999999999999999i128; // equivalent to parquet decimal(38, 0)

#[derive(Error, Debug)]
#[error("Decimal {0} too large.")]
pub struct InvalidBigDecimal(BigDecimal);

/// Denotes the types of properties allowed to be stored in the graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, derive_more::From)]
pub enum Prop {
    Str(ArcStr),
    U8(u8),
    U16(u16),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
    List(PropArray),
    Map(Arc<FxHashMap<ArcStr, Prop>>),
    NDTime(NaiveDateTime),
    DTime(DateTime<Utc>),
    Decimal(BigDecimal),
}

impl From<GidRef<'_>> for Prop {
    fn from(value: GidRef<'_>) -> Self {
        match value {
            GidRef::U64(n) => Prop::U64(n),
            GidRef::Str(s) => Prop::str(s),
        }
    }
}

impl<'a> From<PropRef<'a>> for Prop {
    fn from(value: PropRef<'a>) -> Self {
        match value {
            PropRef::Str(s) => Prop::Str(s.into()),
            PropRef::Num(n) => match n {
                PropNum::U8(u) => Prop::U8(u),
                PropNum::U16(u) => Prop::U16(u),
                PropNum::I32(i) => Prop::I32(i),
                PropNum::I64(i) => Prop::I64(i),
                PropNum::U32(u) => Prop::U32(u),
                PropNum::U64(u) => Prop::U64(u),
                PropNum::F32(f) => Prop::F32(f),
                PropNum::F64(f) => Prop::F64(f),
            },
            PropRef::Bool(b) => Prop::Bool(b),
            PropRef::List(v) => Prop::List(v.as_ref().clone()),
            PropRef::Map(m) => m
                .into_prop()
                .unwrap_or_else(|| Prop::Map(Arc::new(Default::default()))),
            PropRef::NDTime(dt) => Prop::NDTime(dt),
            PropRef::DTime(dt) => Prop::DTime(dt),
            PropRef::Decimal { num, scale } => {
                Prop::Decimal(BigDecimal::from_bigint(num.into(), scale as i64))
            }
        }
    }
}

impl Hash for Prop {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Prop::Str(s) => s.hash(state),
            Prop::U8(u) => u.hash(state),
            Prop::U16(u) => u.hash(state),
            Prop::I32(i) => i.hash(state),
            Prop::I64(i) => i.hash(state),
            Prop::U32(u) => u.hash(state),
            Prop::U64(u) => u.hash(state),
            Prop::F32(f) => {
                let bits = f.to_bits();
                bits.hash(state);
            }
            Prop::F64(f) => {
                let bits = f.to_bits();
                bits.hash(state);
            }
            Prop::Bool(b) => b.hash(state),
            Prop::NDTime(dt) => dt.hash(state),
            Prop::DTime(dt) => dt.hash(state),
            Prop::List(v) => {
                for prop in v.iter() {
                    prop.hash(state);
                }
            }
            Prop::Map(m) => {
                for (key, prop) in m.iter() {
                    key.hash(state);
                    prop.hash(state);
                }
            }
            Prop::Decimal(d) => d.hash(state),
        }
    }
}

impl Eq for Prop {}

impl PartialOrd for Prop {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Prop::Str(a), Prop::Str(b)) => a.partial_cmp(b),
            (Prop::U8(a), Prop::U8(b)) => a.partial_cmp(b),
            (Prop::U16(a), Prop::U16(b)) => a.partial_cmp(b),
            (Prop::I32(a), Prop::I32(b)) => a.partial_cmp(b),
            (Prop::I64(a), Prop::I64(b)) => a.partial_cmp(b),
            (Prop::U32(a), Prop::U32(b)) => a.partial_cmp(b),
            (Prop::U64(a), Prop::U64(b)) => a.partial_cmp(b),
            (Prop::F32(a), Prop::F32(b)) => a.partial_cmp(b),
            (Prop::F64(a), Prop::F64(b)) => a.partial_cmp(b),
            (Prop::Bool(a), Prop::Bool(b)) => a.partial_cmp(b),
            (Prop::NDTime(a), Prop::NDTime(b)) => a.partial_cmp(b),
            (Prop::DTime(a), Prop::DTime(b)) => a.partial_cmp(b),
            (Prop::List(a), Prop::List(b)) => a.partial_cmp(b),
            (Prop::Decimal(a), Prop::Decimal(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

pub struct SerdeProp<'a>(pub &'a Prop);
#[derive(Clone, Copy, Debug)]
pub struct SerdeList<'a>(pub &'a PropArray);
#[derive(Clone, Copy)]
pub struct SerdeMap<'a>(pub &'a HashMap<ArcStr, Prop, FxBuildHasher>);

#[derive(Clone, Copy, Serialize)]
pub struct SerdeRow<P: Serialize> {
    value: Option<P>,
}

impl<'a> Serialize for SerdeList<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_seq(Some(self.0.len()))?;
        for prop in self.0.iter() {
            state.serialize_element(&SerdeProp(&prop))?;
        }
        state.end()
    }
}

impl<'a> Serialize for SerdeMap<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            state.serialize_entry(k, &SerdeProp(v))?;
        }
        state.end()
    }
}

impl<'a> Serialize for SerdeProp<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Prop::I32(i) => serializer.serialize_i32(*i),
            Prop::I64(i) => serializer.serialize_i64(*i),
            Prop::F32(f) => serializer.serialize_f32(*f),
            Prop::F64(f) => serializer.serialize_f64(*f),
            Prop::U8(u) => serializer.serialize_u8(*u),
            Prop::U16(u) => serializer.serialize_u16(*u),
            Prop::U32(u) => serializer.serialize_u32(*u),
            Prop::U64(u) => serializer.serialize_u64(*u),
            Prop::Str(s) => serializer.serialize_str(s),
            Prop::Bool(b) => serializer.serialize_bool(*b),
            Prop::DTime(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            Prop::NDTime(dt) => serializer.serialize_i64(dt.and_utc().timestamp_millis()),
            Prop::List(l) => SerdeList(l).serialize(serializer),
            Prop::Map(m) => SerdeMap(m).serialize(serializer),
            Prop::Decimal(dec) => serializer.serialize_str(&dec.to_string()),
        }
    }
}

pub fn validate_prop(prop: Prop) -> Result<Prop, InvalidBigDecimal> {
    match prop {
        Prop::Decimal(ref bd) => {
            let (bint, scale) = bd.as_bigint_and_exponent();
            if bint <= BigInt::from(DECIMAL_MAX) && scale <= 38 {
                Ok(prop)
            } else {
                Err(InvalidBigDecimal(bd.clone()))
            }
        }
        _ => Ok(prop),
    }
}

impl Prop {
    pub fn try_from_bd(bd: BigDecimal) -> Result<Prop, InvalidBigDecimal> {
        let prop = Prop::Decimal(bd);
        validate_prop(prop)
    }

    pub fn map(vals: impl IntoIterator<Item = (impl Into<ArcStr>, impl Into<Prop>)>) -> Self {
        let h_map: FxHashMap<_, _> = vals
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Prop::Map(h_map.into())
    }

    pub fn as_map(&self) -> Option<SerdeMap<'_>> {
        match self {
            Prop::Map(map) => Some(SerdeMap(map)),
            _ => None,
        }
    }

    pub fn dtype(&self) -> PropType {
        match self {
            Prop::Str(_) => PropType::Str,
            Prop::U8(_) => PropType::U8,
            Prop::U16(_) => PropType::U16,
            Prop::I32(_) => PropType::I32,
            Prop::I64(_) => PropType::I64,
            Prop::U32(_) => PropType::U32,
            Prop::U64(_) => PropType::U64,
            Prop::F32(_) => PropType::F32,
            Prop::F64(_) => PropType::F64,
            Prop::Bool(_) => PropType::Bool,
            Prop::List(list) => PropType::List(Box::new(list.dtype())),
            Prop::Map(map) => PropType::map(map.iter().map(|(k, v)| (k, v.dtype()))),
            Prop::NDTime(_) => PropType::NDTime,
            Prop::DTime(_) => PropType::DTime,
            Prop::Decimal(d) => PropType::Decimal {
                scale: d.as_bigint_and_scale().1,
            },
        }
    }

    pub fn str<S: Into<ArcStr>>(s: S) -> Prop {
        Prop::Str(s.into())
    }

    pub fn list<P: Into<Prop>, I: IntoIterator<Item = P>>(vals: I) -> Prop {
        Prop::List(PropArray::Vec(
            vals.into_iter().map_into().collect::<Vec<_>>().into(),
        ))
    }

    pub fn add(self, other: Prop) -> Option<Prop> {
        match (self, other) {
            (Prop::U8(a), Prop::U8(b)) => Some(Prop::U8(a + b)),
            (Prop::U16(a), Prop::U16(b)) => Some(Prop::U16(a + b)),
            (Prop::I32(a), Prop::I32(b)) => Some(Prop::I32(a + b)),
            (Prop::I64(a), Prop::I64(b)) => Some(Prop::I64(a + b)),
            (Prop::U32(a), Prop::U32(b)) => Some(Prop::U32(a + b)),
            (Prop::U64(a), Prop::U64(b)) => Some(Prop::U64(a + b)),
            (Prop::F32(a), Prop::F32(b)) => Some(Prop::F32(a + b)),
            (Prop::F64(a), Prop::F64(b)) => Some(Prop::F64(a + b)),
            (Prop::Str(a), Prop::Str(b)) => Some(Prop::Str((a.to_string() + b.as_ref()).into())),
            (Prop::Decimal(a), Prop::Decimal(b)) => Some(Prop::Decimal(a + b)),
            _ => None,
        }
    }

    pub fn min(self, other: Prop) -> Option<Prop> {
        self.partial_cmp(&other).map(|ord| match ord {
            Ordering::Less => self,
            Ordering::Equal => self,
            Ordering::Greater => other,
        })
    }

    pub fn max(self, other: Prop) -> Option<Prop> {
        self.partial_cmp(&other).map(|ord| match ord {
            Ordering::Less => other,
            Ordering::Equal => self,
            Ordering::Greater => self,
        })
    }

    pub fn divide(self, other: Prop) -> Option<Prop> {
        match (self, other) {
            (Prop::U8(a), Prop::U8(b)) if b != 0 => Some(Prop::U8(a / b)),
            (Prop::U16(a), Prop::U16(b)) if b != 0 => Some(Prop::U16(a / b)),
            (Prop::I32(a), Prop::I32(b)) if b != 0 => Some(Prop::I32(a / b)),
            (Prop::I64(a), Prop::I64(b)) if b != 0 => Some(Prop::I64(a / b)),
            (Prop::U32(a), Prop::U32(b)) if b != 0 => Some(Prop::U32(a / b)),
            (Prop::U64(a), Prop::U64(b)) if b != 0 => Some(Prop::U64(a / b)),
            (Prop::F32(a), Prop::F32(b)) => Some(Prop::F32(a / b)),
            (Prop::F64(a), Prop::F64(b)) => Some(Prop::F64(a / b)),
            (Prop::Decimal(a), Prop::Decimal(b)) if b != BigDecimal::from(0) => {
                Some(Prop::Decimal(a / b))
            }
            _ => None,
        }
    }
}

#[cfg(feature = "arrow")]
pub fn list_array_from_props<P: Serialize + std::fmt::Debug + Clone>(
    dt: &DataType,
    props: impl IntoIterator<Item = Option<P>>,
) -> LargeListArray {
    use arrow_schema::{Field, Fields};
    use serde_arrow::ArrayBuilder;

    let fields: Fields = vec![Field::new("value", dt.clone(), true)].into();

    let mut builder = ArrayBuilder::from_arrow(&fields)
        .unwrap_or_else(|e| panic!("Failed to make array builder {e}"));

    for value in props {
        builder.push(SerdeRow { value }).unwrap_or_else(|e| {
            panic!("Failed to push list to array builder {e} for type {fields:?}",)
        });
    }

    let arrays = builder
        .to_arrow()
        .unwrap_or_else(|e| panic!("Failed to convert to arrow array {e}"));

    arrays.first().unwrap().as_list::<i64>().clone()
}

#[cfg(feature = "arrow")]
pub fn struct_array_from_props<P: Serialize>(
    dt: &DataType,
    props: impl IntoIterator<Item = Option<P>>,
) -> StructArray {
    use serde_arrow::ArrayBuilder;

    let fields = [FieldRef::new(Field::new("value", dt.clone(), true))];

    let mut builder = ArrayBuilder::from_arrow(&fields)
        .unwrap_or_else(|e| panic!("Failed to make array builder {e}"));

    for p in props {
        builder
            .push(SerdeRow { value: p })
            .unwrap_or_else(|e| panic!("Failed to push map to array builder {e}"))
    }

    let arrays = builder
        .to_arrow()
        .unwrap_or_else(|e| panic!("Failed to convert to arrow array {e}"));
    arrays.first().unwrap().as_struct().clone()
}

impl Display for Prop {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Prop::Str(value) => write!(f, "{}", value),
            Prop::U8(value) => write!(f, "{}", value),
            Prop::U16(value) => write!(f, "{}", value),
            Prop::I32(value) => write!(f, "{}", value),
            Prop::I64(value) => write!(f, "{}", value),
            Prop::U32(value) => write!(f, "{}", value),
            Prop::U64(value) => write!(f, "{}", value),
            Prop::F32(value) => write!(f, "{}", value),
            Prop::F64(value) => write!(f, "{}", value),
            Prop::Bool(value) => write!(f, "{}", value),
            Prop::DTime(value) => write!(f, "{}", value),
            Prop::NDTime(value) => write!(f, "{}", value),
            Prop::List(value) => {
                write!(
                    f,
                    "[{}]",
                    value
                        .iter()
                        .map(|item| {
                            match item {
                                Prop::Str(_) => {
                                    format!("\"{}\"", item)
                                }
                                _ => {
                                    format!("{}", item)
                                }
                            }
                        })
                        .join(", ")
                )
            }
            Prop::Map(value) => {
                write!(
                    f,
                    "{{{}}}",
                    value
                        .iter()
                        .map(|(key, val)| {
                            match val {
                                Prop::Str(_) => {
                                    format!("\"{}\": \"{}\"", key, val)
                                }
                                _ => {
                                    format!("\"{}\": {}", key, val)
                                }
                            }
                        })
                        .join(", ")
                )
            }
            Prop::Decimal(d) => write!(f, "Decimal({})", d.as_bigint_and_scale().1),
        }
    }
}

impl From<&str> for Prop {
    fn from(s: &str) -> Self {
        Prop::Str(s.into())
    }
}

impl From<String> for Prop {
    fn from(s: String) -> Self {
        Prop::Str(s.into())
    }
}

impl From<HashMap<ArcStr, Prop>> for Prop {
    fn from(value: HashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value.into_iter().collect()))
    }
}

impl From<FxHashMap<ArcStr, Prop>> for Prop {
    fn from(value: FxHashMap<ArcStr, Prop>) -> Self {
        Prop::Map(Arc::new(value))
    }
}

impl From<Vec<Prop>> for Prop {
    fn from(value: Vec<Prop>) -> Self {
        Prop::List(Arc::new(value).into())
    }
}

impl From<&Prop> for Prop {
    fn from(value: &Prop) -> Self {
        value.clone()
    }
}

#[cfg(feature = "arrow")]
impl From<ArrayRef> for Prop {
    fn from(value: ArrayRef) -> Self {
        Prop::List(PropArray::from(value))
    }
}

pub trait IntoPropMap {
    fn into_prop_map(self) -> Prop;
}

impl<I: IntoIterator<Item = (K, V)>, K: Into<ArcStr>, V: Into<Prop>> IntoPropMap for I {
    fn into_prop_map(self) -> Prop {
        Prop::Map(Arc::new(
            self.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        ))
    }
}

pub trait IntoPropList {
    fn into_prop_list(self) -> Prop;
}

impl<I: IntoIterator<Item = K>, K: Into<Prop>> IntoPropList for I {
    fn into_prop_list(self) -> Prop {
        let vec = self.into_iter().map(|v| v.into()).collect::<Vec<_>>();
        Prop::List(Arc::new(vec).into())
    }
}

pub trait IntoProp {
    fn into_prop(self) -> Prop;
}

impl<T: Into<Prop>> IntoProp for T {
    fn into_prop(self) -> Prop {
        self.into()
    }
}

pub fn sort_comparable_props(props: Vec<&Prop>) -> Vec<&Prop> {
    // Filter out non-comparable props
    let mut comparable_props: Vec<_> = props
        .into_iter()
        .filter(|p| {
            matches!(
                p,
                Prop::Str(_)
                    | Prop::U8(_)
                    | Prop::U16(_)
                    | Prop::I32(_)
                    | Prop::I64(_)
                    | Prop::U32(_)
                    | Prop::U64(_)
                    | Prop::F32(_)
                    | Prop::F64(_)
                    | Prop::Bool(_)
                    | Prop::NDTime(_)
                    | Prop::DTime(_)
            )
        })
        .collect();

    // Sort the comparable props
    comparable_props.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    comparable_props
}
