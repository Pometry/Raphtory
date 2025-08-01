use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    fmt::{Display, Formatter},
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

#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
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
    Array(Box<PropType>),
    Decimal {
        scale: i64,
    },
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
            PropType::Array(p_type) => return write!(f, "Array<{}>", p_type),
            PropType::Decimal { scale } => return write!(f, "Decimal({})", scale),
        };

        write!(f, "{}", type_str)
    }
}

impl PropType {
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
}

#[cfg(feature = "storage")]
mod storage {
    use crate::core::entities::properties::prop::PropType;
    use polars_arrow::datatypes::ArrowDataType as PolarsDataType;

    impl From<&PolarsDataType> for PropType {
        fn from(value: &PolarsDataType) -> Self {
            match value {
                PolarsDataType::Utf8 => PropType::Str,
                PolarsDataType::LargeUtf8 => PropType::Str,
                PolarsDataType::Utf8View => PropType::Str,
                PolarsDataType::UInt8 => PropType::U8,
                PolarsDataType::UInt16 => PropType::U16,
                PolarsDataType::Int32 => PropType::I32,
                PolarsDataType::Int64 => PropType::I64,
                PolarsDataType::UInt32 => PropType::U32,
                PolarsDataType::UInt64 => PropType::U64,
                PolarsDataType::Float32 => PropType::F32,
                PolarsDataType::Float64 => PropType::F64,
                PolarsDataType::Decimal(_, scale) => PropType::Decimal {
                    scale: *scale as i64,
                },
                PolarsDataType::Boolean => PropType::Bool,

                _ => PropType::Empty,
            }
        }
    }
}

#[cfg(feature = "arrow")]
mod arrow {
    use crate::core::entities::properties::prop::PropType;
    use arrow_schema::DataType;

    impl From<&DataType> for PropType {
        fn from(value: &DataType) -> Self {
            match value {
                DataType::Utf8 => PropType::Str,
                DataType::LargeUtf8 => PropType::Str,
                DataType::Utf8View => PropType::Str,
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

                _ => PropType::Empty,
            }
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
        (PropType::Array(l_type), PropType::Array(r_type)) => {
            unify_types(l_type, r_type, unified).map(|t| PropType::Array(Box::new(t)))
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

#[cfg(test)]
mod test {
    use super::*;

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

        let l = PropType::Array(Box::new(PropType::map([("a".to_string(), PropType::U8)])));
        let r = PropType::Array(Box::new(PropType::map([
            ("a".to_string(), PropType::Empty),
            ("b".to_string(), PropType::Str),
        ])));
        let mut unify = false;
        assert_eq!(
            unify_types(&l, &r, &mut unify),
            Ok(PropType::Array(Box::new(PropType::map([
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
}
