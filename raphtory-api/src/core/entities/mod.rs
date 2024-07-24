use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

#[cfg(feature = "python")]
use pyo3::exceptions::PyTypeError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use self::edges::edge_ref::EdgeRef;
use num_traits::cast::ToPrimitive;

use super::input::input_node::parse_u64_strict;

pub mod edges;

// the only reason this is public is because the physical ids of the nodes don't move
#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct VID(pub usize);

impl VID {
    pub fn index(&self) -> usize {
        self.0
    }

    pub fn as_u64(&self) -> u64 {
        self.0 as u64
    }
}

impl From<usize> for VID {
    fn from(id: usize) -> Self {
        VID(id)
    }
}

impl From<VID> for usize {
    fn from(id: VID) -> Self {
        id.0
    }
}

#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct EID(pub usize);

impl From<EID> for usize {
    fn from(id: EID) -> Self {
        id.0
    }
}

impl From<usize> for EID {
    fn from(id: usize) -> Self {
        EID(id)
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Default,
)]
pub struct ELID {
    edge: EID,
    layer: Option<usize>,
}

impl ELID {
    pub fn new(edge: EID, layer: Option<usize>) -> Self {
        Self { edge, layer }
    }
    pub fn pid(&self) -> EID {
        self.edge
    }

    pub fn layer(&self) -> Option<usize> {
        self.layer
    }
}

impl From<EdgeRef> for ELID {
    fn from(value: EdgeRef) -> Self {
        ELID {
            edge: value.pid(),
            layer: value.layer().copied(),
        }
    }
}
impl EID {
    pub fn from_u64(id: u64) -> Self {
        EID(id as usize)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum GID {
    U64(u64),
    I64(i64),
    Str(String),
}

impl Default for GID {
    fn default() -> Self {
        GID::U64(0)
    }
}

impl Display for GID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GID::U64(v) => write!(f, "{}", v),
            GID::I64(v) => write!(f, "{}", v),
            GID::Str(v) => write!(f, "{}", v),
        }
    }
}

#[cfg(feature = "python")]
impl IntoPy<PyObject> for GID {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            GID::U64(v) => v.into_py(py),
            GID::I64(v) => v.into_py(py),
            GID::Str(v) => v.into_py(py),
        }
    }
}

#[cfg(feature = "python")]
impl ToPyObject for GID {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            GID::U64(v) => v.to_object(py),
            GID::I64(v) => v.to_object(py),
            GID::Str(v) => v.to_object(py),
        }
    }
}

#[cfg(feature = "python")]
impl<'source> FromPyObject<'source> for GID {
    fn extract(id: &'source PyAny) -> PyResult<Self> {
        id.extract::<String>()
            .map(GID::Str)
            .or_else(|_| {
                id.extract::<u64>()
                    .map(GID::U64)
                    .or_else(|_| id.extract::<i64>().map(GID::I64))
            })
            .map_err(|_| {
                let msg = "IDs need to be strings or an unsigned integers";
                PyTypeError::new_err(msg)
            })
    }
}

impl GID {
    pub fn into_str(self) -> Option<String> {
        match self {
            GID::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_i64(self) -> Option<i64> {
        match self {
            GID::I64(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_u64(self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            GID::Str(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            GID::I64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_str(&self) -> Cow<String> {
        match self {
            GID::U64(v) => Cow::Owned(v.to_string()),
            GID::I64(v) => Cow::Owned(v.to_string()),
            GID::Str(v) => Cow::Borrowed(v),
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            GID::U64(v) => v.to_i64(),
            GID::I64(v) => Some(*v),
            GID::Str(v) => parse_u64_strict(v)?.to_i64(),
        }
    }

    pub fn to_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            GID::I64(v) => v.to_u64(),
            GID::Str(v) => parse_u64_strict(v),
        }
    }
}

impl From<u64> for GID {
    fn from(id: u64) -> Self {
        Self::U64(id)
    }
}

impl From<i64> for GID {
    fn from(id: i64) -> Self {
        Self::I64(id)
    }
}

impl From<String> for GID {
    fn from(id: String) -> Self {
        Self::Str(id)
    }
}

impl From<&str> for GID {
    fn from(id: &str) -> Self {
        Self::Str(id.to_string())
    }
}

impl<'a> From<GidRef<'a>> for GID {
    fn from(value: GidRef<'a>) -> Self {
        match value {
            GidRef::U64(v) => GID::U64(v),
            GidRef::I64(v) => GID::I64(v),
            GidRef::Str(v) => GID::Str(v.to_owned()),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum GidRef<'a> {
    U64(u64),
    I64(i64),
    Str(&'a str),
}

impl<'a> From<&'a GID> for GidRef<'a> {
    fn from(value: &'a GID) -> Self {
        match value {
            GID::U64(v) => GidRef::U64(*v),
            GID::I64(v) => GidRef::I64(*v),
            GID::Str(v) => GidRef::Str(v),
        }
    }
}

impl<'a> GidRef<'a> {
    pub fn as_str(self) -> Option<&'a str> {
        match self {
            GidRef::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(self) -> Option<i64> {
        match self {
            GidRef::I64(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_u64(self) -> Option<u64> {
        match self {
            GidRef::U64(v) => Some(v),
            _ => None,
        }
    }

    pub fn to_owned(self) -> GID {
        match self {
            GidRef::U64(v) => GID::U64(v),
            GidRef::I64(v) => GID::I64(v),
            GidRef::Str(v) => GID::Str(v.to_owned()),
        }
    }

    pub fn to_str(self) -> Cow<'a, str> {
        match self {
            GidRef::U64(v) => Cow::Owned(v.to_string()),
            GidRef::I64(v) => Cow::Owned(v.to_string()),
            GidRef::Str(v) => Cow::Borrowed(v),
        }
    }

    pub fn to_i64(self) -> Option<i64> {
        match self {
            GidRef::U64(v) => v.to_i64(),
            GidRef::I64(v) => Some(v),
            GidRef::Str(v) => parse_u64_strict(v)?.to_i64(),
        }
    }

    pub fn to_u64(self) -> Option<u64> {
        match self {
            GidRef::U64(v) => Some(v),
            GidRef::I64(v) => v.to_u64(),
            GidRef::Str(v) => parse_u64_strict(v),
        }
    }
}
