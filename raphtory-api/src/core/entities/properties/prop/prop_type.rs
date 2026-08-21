use arrow_schema::{ArrowError, DataType};
use serde::{Deserialize, Serialize};
use std::{
    cell::LazyCell,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    str::FromStr,
    sync::Arc,
};

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Wrong type for property {name}: expected {expected:?} but actual type is {actual:?}")]
pub struct PropError {
    pub(crate) name: String,
    pub(crate) expected: PropType,
    pub(crate) actual: PropType,
}

impl PropError {
    pub fn with_name(self, name: String) -> PropError {
        Self { name, ..self }
    }
}

#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PropType {
    #[default]
    Empty,
    Str,
    U8,
    U16,
    I32,
    I64,
    U32,
    U64,
    F32,
    F64,
    Bool,
    List(Box<PropType>),
    Map(Arc<HashMap<String, PropType>>),
    NDTime,
    DTime,
    Decimal {
        scale: i64,
    },
}

impl fmt::Debug for PropType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Only the container spellings differ from `Display` (`List(x)` vs
        // `List<x>`, `Map({..})` vs `Map{ .. }`, `Decimal { scale: n }` vs
        // `Decimal(n)`); every unit variant delegates.
        match self {
            PropType::List(p_type) => f.debug_tuple("List").field(p_type).finish(),
            PropType::Map(p_type) => {
                // The derived impl would iterate the HashMap, whose order is
                // seeded per process — the same type would render differently
                // between runs. Sorting keeps the derived shape, made
                // deterministic; equality stays order-independent.
                let mut fields: Vec<_> = p_type.iter().collect();
                fields.sort_by_key(|(k, _)| k.as_str());
                write!(f, "Map(")?;
                let mut map = f.debug_map();
                for (k, v) in fields {
                    map.entry(k, v);
                }
                map.finish()?;
                write!(f, ")")
            }
            PropType::Decimal { scale } => f.debug_struct("Decimal").field("scale", scale).finish(),
            unit => Display::fmt(unit, f),
        }
    }
}

impl Display for PropType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let type_str = match self {
            PropType::Empty => "Empty",
            PropType::Str => "Str",
            PropType::U8 => "U8",
            PropType::U16 => "U16",
            PropType::I32 => "I32",
            PropType::I64 => "I64",
            PropType::U32 => "U32",
            PropType::U64 => "U64",
            PropType::F32 => "F32",
            PropType::F64 => "F64",
            PropType::Bool => "Bool",
            PropType::List(p_type) => return write!(f, "List<{}>", p_type),
            PropType::Map(p_type) => {
                let mut types = p_type
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<String>>();
                types.sort();
                return write!(f, "Map{{ {} }}", types.join(", "));
            }
            PropType::NDTime => "NDTime",
            PropType::DTime => "DTime",
            PropType::Decimal { scale } => return write!(f, "Decimal({})", scale),
        };

        write!(f, "{}", type_str)
    }
}

const CONTAINER_SIZE: LazyCell<usize> = LazyCell::new(|| {
    std::env::var("RAPHTORY_PROP_CONTAINER_SIZE")
        .ok()
        .map(|size| {
            size.parse::<usize>().unwrap_or_else(|_| {
                eprintln!("RAPHTORY_PROP_CONTAINER_SIZE not set or invalid, defaulting to 64");
                64
            })
        })
        .unwrap_or(64)
});

impl PropType {
    pub fn inner(&self) -> Option<&PropType> {
        match self {
            PropType::List(inner) => Some(inner.as_ref()),
            _ => None,
        }
    }

    pub fn map(fields: impl IntoIterator<Item = (impl Into<String>, PropType)>) -> Self {
        let map: HashMap<_, _> = fields.into_iter().map(|(k, v)| (k.into(), v)).collect();
        PropType::Map(Arc::from(map))
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            PropType::U8
                | PropType::U16
                | PropType::U32
                | PropType::U64
                | PropType::I32
                | PropType::I64
                | PropType::F32
                | PropType::F64
                | PropType::Decimal { .. }
        )
    }

    pub fn is_str(&self) -> bool {
        matches!(self, PropType::Str)
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, PropType::Bool)
    }

    pub fn is_date(&self) -> bool {
        matches!(self, PropType::DTime | PropType::NDTime)
    }

    pub fn has_add(&self) -> bool {
        self.is_numeric() || self.is_str()
    }

    pub fn has_divide(&self) -> bool {
        self.is_numeric()
    }

    pub fn has_cmp(&self) -> bool {
        self.is_bool() || self.is_numeric() || self.is_str() || self.is_date()
    }

    pub fn homogeneous_map_value_type(&self) -> Option<PropType> {
        if let PropType::Map(map) = self {
            let mut iter = map.values();
            if let Some(first) = iter.next() {
                if iter.all(|v| v == first) {
                    return Some(first.clone());
                }
            }
        }
        None
    }

    // This is the best guess for the size of one row of properties
    pub fn est_size(&self) -> usize {
        let container_size = *CONTAINER_SIZE;
        match self {
            PropType::Str => container_size,
            PropType::U8 | PropType::Bool => 1,
            PropType::U16 => 2,
            PropType::I32 | PropType::F32 | PropType::U32 => 4,
            PropType::I64 | PropType::F64 | PropType::U64 => 8,
            PropType::NDTime | PropType::DTime => 8,
            PropType::List(p_type) => p_type.est_size() * container_size,
            PropType::Map(p_map) => {
                p_map.values().map(|v| v.est_size()).sum::<usize>() * container_size
            }
            PropType::Decimal { .. } => 16,
            PropType::Empty => 0,
        }
    }
}

pub fn data_type_as_prop_type(dt: &DataType) -> Result<PropType, InvalidPropertyTypeErr> {
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
        DataType::Date32 => Ok(PropType::NDTime),
        DataType::Date64 => Ok(PropType::NDTime),
        DataType::Decimal128(precision, scale) if *precision <= 38 => Ok(PropType::Decimal {
            scale: *scale as i64,
        }),
        DataType::Null => Ok(PropType::Empty),
        _ => Err(InvalidPropertyTypeErr(dt.clone())),
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{0:?} not supported as property type")]
pub struct InvalidPropertyTypeErr(pub DataType);

#[derive(thiserror::Error, Debug)]
pub enum PropTypeParseError {
    #[error("Unknown type '{input}': {source}")]
    UnknownType {
        input: String,
        #[source]
        source: ArrowError,
    },
    #[error("Unsupported type '{input}': {source}")]
    UnsupportedType {
        input: String,
        #[source]
        source: InvalidPropertyTypeErr,
    },
}

impl FromStr for PropType {
    type Err = PropTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let data_type: DataType = s
            .parse()
            .map_err(|source| PropTypeParseError::UnknownType {
                input: s.to_owned(),
                source,
            })?;
        data_type_as_prop_type(&data_type).map_err(|source| PropTypeParseError::UnsupportedType {
            input: s.to_owned(),
            source,
        })
    }
}

pub mod arrow {
    use crate::core::entities::properties::prop::{PropType, EMPTY_MAP_FIELD_NAME};
    use arrow_schema::{DataType, Field, Fields, TimeUnit};

    impl From<&DataType> for PropType {
        fn from(value: &DataType) -> Self {
            match value {
                DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => PropType::Str,
                DataType::UInt8 => PropType::U8,
                DataType::UInt16 => PropType::U16,
                DataType::Int32 => PropType::I32,
                DataType::Int64 => PropType::I64,
                DataType::UInt32 => PropType::U32,
                DataType::UInt64 => PropType::U64,
                DataType::Float32 => PropType::F32,
                DataType::Float64 => PropType::F64,
                DataType::Decimal128(_, scale) => PropType::Decimal {
                    scale: *scale as i64,
                },
                DataType::Boolean => PropType::Bool,
                DataType::Timestamp(TimeUnit::Millisecond, None) => PropType::NDTime,
                DataType::Timestamp(TimeUnit::Millisecond, tz) if tz.as_deref() == Some("UTC") => {
                    PropType::DTime
                }
                DataType::Struct(fields) => PropType::map(
                    fields
                        .iter()
                        .filter(|field| field.name() != EMPTY_MAP_FIELD_NAME)
                        .map(|f| (f.name().to_string(), PropType::from(f.data_type()))),
                ),
                DataType::List(field) | DataType::LargeList(field) => {
                    PropType::List(Box::new(PropType::from(field.data_type())))
                }
                DataType::Null => PropType::Empty,
                dtype => panic!("unsupported type {dtype:?}"),
            }
        }
    }

    impl From<&PropType> for DataType {
        fn from(value: &PropType) -> Self {
            match value {
                PropType::Str => DataType::Utf8View,
                PropType::U8 => DataType::UInt8,
                PropType::U16 => DataType::UInt16,
                PropType::I32 => DataType::Int32,
                PropType::I64 => DataType::Int64,
                PropType::U32 => DataType::UInt32,
                PropType::U64 => DataType::UInt64,
                PropType::F32 => DataType::Float32,
                PropType::F64 => DataType::Float64,
                PropType::Decimal { scale } => {
                    DataType::Decimal128(38, (*scale).try_into().unwrap())
                }
                PropType::Bool => DataType::Boolean,
                PropType::NDTime => DataType::Timestamp(TimeUnit::Millisecond, None),
                PropType::DTime => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                PropType::List(p_type) => DataType::LargeList(
                    Field::new("data", DataType::from(p_type.as_ref()), true).into(),
                ),
                PropType::Map(p_type) => {
                    let mut fields = p_type
                        .iter()
                        .map(|(name, p_type)| Field::new(name, DataType::from(p_type), true))
                        .collect::<Vec<_>>();
                    fields.sort_by(|l, r| l.name().cmp(r.name()));

                    if fields.is_empty() {
                        DataType::Struct(Fields::from_iter([Field::new(
                            EMPTY_MAP_FIELD_NAME,
                            DataType::Null,
                            true,
                        )]))
                    } else {
                        DataType::Struct(fields.into())
                    }
                }
                PropType::Empty => DataType::Null,
            }
        }
    }

    impl From<PropType> for DataType {
        fn from(value: PropType) -> Self {
            DataType::from(&value)
        }
    }
}

// step through these types trees and check they are structurally the same
// if we encounter an empty we replace it with the other type
// the result is the unified type or err if the types are not compatible
pub fn unify_types(l: &PropType, r: &PropType, unified: &mut bool) -> Result<PropType, PropError> {
    match (l, r) {
        (PropType::Empty, r) => {
            *unified = true;
            Ok(r.clone())
        }
        (l, PropType::Empty) => {
            *unified = true;
            Ok(l.clone())
        }
        (PropType::Str, PropType::Str) => Ok(PropType::Str),
        (PropType::U8, PropType::U8) => Ok(PropType::U8),
        (PropType::U16, PropType::U16) => Ok(PropType::U16),
        (PropType::I32, PropType::I32) => Ok(PropType::I32),
        (PropType::I64, PropType::I64) => Ok(PropType::I64),
        (PropType::U32, PropType::U32) => Ok(PropType::U32),
        (PropType::U64, PropType::U64) => Ok(PropType::U64),
        (PropType::F32, PropType::F32) => Ok(PropType::F32),
        (PropType::F64, PropType::F64) => Ok(PropType::F64),
        (PropType::Bool, PropType::Bool) => Ok(PropType::Bool),
        (PropType::NDTime, PropType::NDTime) => Ok(PropType::NDTime),
        (PropType::DTime, PropType::DTime) => Ok(PropType::DTime),
        (PropType::List(l_type), PropType::List(r_type)) => {
            unify_types(l_type, r_type, unified).map(|t| PropType::List(Box::new(t)))
        }
        (PropType::Map(l_map), PropType::Map(r_map)) => {
            // maps need to be merged and only overlapping keys need to be unified

            let mut merged = HashMap::new();
            for (k, v) in l_map.iter() {
                if let Some(r_v) = r_map.get(k) {
                    let merged_prop = unify_types(v, r_v, unified)?;
                    merged.insert(k.clone(), merged_prop);
                } else {
                    merged.insert(k.clone(), v.clone());
                    *unified = true;
                }
            }
            for (k, v) in r_map.iter() {
                if !merged.contains_key(k) {
                    merged.insert(k.clone(), v.clone());
                    *unified = true;
                }
            }
            Ok(PropType::Map(merged.into()))
        }
        (PropType::Decimal { scale: l_scale }, PropType::Decimal { scale: r_scale })
            if l_scale == r_scale =>
        {
            Ok(PropType::Decimal { scale: *l_scale })
        }
        (_, _) => Err(PropError {
            name: "unknown".to_string(),
            expected: l.clone(),
            actual: r.clone(),
        }),
    }
}

// fast check before we actually unify, 99% of the time this will be enough
// there are 3 outcomes,
// types are identical so no unification needed, None
// types can be unified Some(true)
// and types cannot be unified Some(false)
pub fn check_for_unification(l: &PropType, r: &PropType) -> Option<bool> {
    match (l, r) {
        (PropType::Empty, _) => Some(true),
        (_, PropType::Empty) => Some(true),
        (PropType::Str, PropType::Str) => None,
        (PropType::U8, PropType::U8) => None,
        (PropType::U16, PropType::U16) => None,
        (PropType::I32, PropType::I32) => None,
        (PropType::I64, PropType::I64) => None,
        (PropType::U32, PropType::U32) => None,
        (PropType::U64, PropType::U64) => None,
        (PropType::F32, PropType::F32) => None,
        (PropType::F64, PropType::F64) => None,
        (PropType::Bool, PropType::Bool) => None,
        (PropType::NDTime, PropType::NDTime) => None,
        (PropType::DTime, PropType::DTime) => None,
        (PropType::List(l_type), PropType::List(r_type)) => check_for_unification(l_type, r_type),
        (PropType::Map(l_map), PropType::Map(r_map)) => {
            let keys_check = l_map
                .keys()
                .any(|k| !r_map.contains_key(k))
                .then_some(true)
                .or_else(|| r_map.keys().any(|k| !l_map.contains_key(k)).then_some(true));

            // check for unification of the values
            let inner_checks = l_map
                .iter()
                .filter_map(|(l_key, l_d_type)| {
                    r_map
                        .get(l_key)
                        .and_then(|r_d_type| check_for_unification(r_d_type, l_d_type))
                })
                .chain(r_map.iter().filter_map(|(r_key, r_d_type)| {
                    l_map
                        .get(r_key)
                        .and_then(|l_d_type| check_for_unification(r_d_type, l_d_type))
                }));
            for check in inner_checks {
                if check {
                    return Some(true);
                }
            }
            keys_check
        }
        (PropType::Decimal { scale: l_scale }, PropType::Decimal { scale: r_scale })
            if l_scale == r_scale =>
        {
            None
        }
        _ => Some(false),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::{DataType, Field, Fields, TimeUnit};
    use proptest::{collection::btree_map, prelude::*};

    // `Display` is the stable rendering of a type: map fields come out sorted,
    // so repeated formatting of the same type is identical. (The derived `Debug`
    // walks the underlying hash map and does not have this property.)
    #[test]
    fn display_of_map_type_is_stable_and_sorted() {
        let map_type = PropType::map([
            ("zeta", PropType::I64),
            ("alpha", PropType::Str),
            ("mid", PropType::Bool),
        ]);
        let rendered = map_type.to_string();
        assert_eq!(rendered, "Map{ alpha: Str, mid: Bool, zeta: I64 }");
        for _ in 0..20 {
            assert_eq!(map_type.to_string(), rendered);
        }
    }

    #[test]
    fn test_unify_types_ne() {
        let l = PropType::List(Box::new(PropType::U8));
        let r = PropType::List(Box::new(PropType::U16));
        assert!(unify_types(&l, &r, &mut false).is_err());

        let l = PropType::map([("a".to_string(), PropType::U8)]);
        let r = PropType::map([("a".to_string(), PropType::U16)]);
        assert!(unify_types(&l, &r, &mut false).is_err());

        let l = PropType::List(Box::new(PropType::U8));
        let r = PropType::List(Box::new(PropType::U16));
        assert!(unify_types(&l, &r, &mut false).is_err());
    }

    #[test]
    fn test_unify_types_eq() {
        let l = PropType::List(Box::new(PropType::U8));
        let r = PropType::List(Box::new(PropType::U8));
        assert_eq!(
            unify_types(&l, &r, &mut false),
            Ok(PropType::List(Box::new(PropType::U8)))
        );

        let l = PropType::map([("a".to_string(), PropType::U8)]);
        let r = PropType::map([("a".to_string(), PropType::U8)]);
        assert_eq!(
            unify_types(&l, &r, &mut false),
            Ok(PropType::map([("a".to_string(), PropType::U8)]))
        );
    }

    #[test]
    fn test_unify_maps() {
        let l = PropType::map([("a".to_string(), PropType::U8)]);
        let r = PropType::map([("a".to_string(), PropType::U16)]);
        assert!(unify_types(&l, &r, &mut false).is_err());

        let l = PropType::map([("a".to_string(), PropType::U8)]);
        let r = PropType::map([("b".to_string(), PropType::U16)]);
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::map([
                ("a".to_string(), PropType::U8),
                ("b".to_string(), PropType::U16)
            ]))
        );
        assert!(unify);

        let l = PropType::map([("a".to_string(), PropType::U8)]);
        let r = PropType::map([
            ("a".to_string(), PropType::U8),
            ("b".to_string(), PropType::U16),
        ]);
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::map([
                ("a".to_string(), PropType::U8),
                ("b".to_string(), PropType::U16)
            ]))
        );
        assert!(unify);

        let l = PropType::map([
            ("a".to_string(), PropType::U8),
            ("b".to_string(), PropType::U16),
        ]);
        let r = PropType::map([("a".to_string(), PropType::U8)]);
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::map([
                ("a".to_string(), PropType::U8),
                ("b".to_string(), PropType::U16)
            ]))
        );
        assert!(unify);
    }

    #[test]
    fn test_unify() {
        let l = PropType::Empty;
        let r = PropType::U8;
        let mut unify = false;
        assert_eq!(unify_types(&l, &r, &mut unify), Ok(PropType::U8));
        assert!(unify);

        let l = PropType::Str;
        let r = PropType::Empty;
        let mut unify = false;
        assert_eq!(unify_types(&l, &r, &mut unify), Ok(PropType::Str));
        assert!(unify);

        let l = PropType::List(Box::new(PropType::List(Box::new(PropType::U8))));
        let r = PropType::List(Box::new(PropType::Empty));
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::List(Box::new(PropType::List(Box::new(
                PropType::U8
            )))))
        );
        assert!(unify);

        let l = PropType::List(Box::new(PropType::map([("a".to_string(), PropType::U8)])));
        let r = PropType::List(Box::new(PropType::map([
            ("a".to_string(), PropType::Empty),
            ("b".to_string(), PropType::Str),
        ])));
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::List(Box::new(PropType::map([
                ("a".to_string(), PropType::U8),
                ("b".to_string(), PropType::Str)
            ]))))
        );
        assert!(unify);
    }

    #[test]
    fn size_of_proptype() {
        let size = size_of::<PropType>();
        println!("PropType = {size}");
        let size = size_of::<HashMap<String, PropType>>();
        println!("Map = {size}");
        let size = size_of::<PropError>();
        println!("PropError = {size}")
    }

    fn field_name() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[a-z][a-z0-9_]{0,6}")
            .unwrap()
            .prop_filter("not the empty map sentinel", |name| {
                name != crate::core::entities::properties::prop::EMPTY_MAP_FIELD_NAME
            })
    }

    fn canonical_data_type() -> impl Strategy<Value = DataType> {
        let leaf = prop_oneof![
            Just(DataType::Boolean),
            Just(DataType::Int32),
            Just(DataType::Int64),
            Just(DataType::UInt8),
            Just(DataType::UInt16),
            Just(DataType::UInt32),
            Just(DataType::UInt64),
            Just(DataType::Float32),
            Just(DataType::Float64),
            Just(DataType::Utf8View),
            Just(DataType::Timestamp(TimeUnit::Millisecond, None)),
            Just(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            )),
            (0i8..=38).prop_map(|scale| DataType::Decimal128(38, scale)),
            Just(DataType::Null),
        ];

        leaf.prop_recursive(4, 64, 4, |inner| {
            prop_oneof![
                inner.clone().prop_map(|data_type| DataType::LargeList(
                    Field::new("data", data_type, true).into()
                )),
                btree_map(field_name(), inner, 0..4).prop_map(|fields| {
                    if fields.is_empty() {
                        DataType::Struct(Fields::from_iter([Field::new(
                            crate::core::entities::properties::prop::EMPTY_MAP_FIELD_NAME,
                            DataType::Null,
                            true,
                        )]))
                    } else {
                        DataType::Struct(
                            fields
                                .into_iter()
                                .map(|(name, data_type)| Field::new(name, data_type, true))
                                .collect::<Vec<_>>()
                                .into(),
                        )
                    }
                }),
            ]
        })
    }

    proptest! {
        #[test]
        fn data_type_to_prop_type_to_data_type_is_transitive(data_type in canonical_data_type()) {
            prop_assert_eq!(DataType::from(PropType::from(&data_type)), data_type);
        }

        #[test]
        fn prop_type_to_data_type_to_prop_type_is_transitive(data_type in canonical_data_type()) {
            let prop_type = PropType::from(&data_type);
            let round_tripped: DataType = (&prop_type).into();

            prop_assert_eq!(PropType::from(&round_tripped), prop_type);
        }
    }
}
