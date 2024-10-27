use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

pub mod entities;
pub mod input;
pub mod storage;
pub mod utils;

/// Denotes the direction of an edge. Can be incoming, outgoing or both.
#[derive(Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Debug, Default, Serialize, Deserialize)]
pub enum Direction {
    OUT,
    IN,
    #[default]
    BOTH,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
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
    List,
    Map,
    NDTime,
    Graph,
    PersistentGraph,
    Document,
    DTime,
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
            PropType::List => "List",
            PropType::Map => "Map",
            PropType::NDTime => "NDTime",
            PropType::Graph => "Graph",
            PropType::PersistentGraph => "PersistentGraph",
            PropType::Document => "Document",
            PropType::DTime => "DTime",
        };

        write!(f, "{}", type_str)
    }
}

impl PropType {
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
}

#[cfg(feature = "storage")]
use polars_arrow::datatypes::ArrowDataType as DataType;

#[cfg(feature = "storage")]
impl From<&DataType> for PropType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Utf8 => PropType::Str,
            DataType::LargeUtf8 => PropType::Str,
            DataType::UInt8 => PropType::U8,
            DataType::UInt16 => PropType::U16,
            DataType::Int32 => PropType::I32,
            DataType::Int64 => PropType::I64,
            DataType::UInt32 => PropType::U32,
            DataType::UInt64 => PropType::U64,
            DataType::Float32 => PropType::F32,
            DataType::Float64 => PropType::F64,
            DataType::Boolean => PropType::Bool,

            _ => PropType::Empty,
        }
    }
}
