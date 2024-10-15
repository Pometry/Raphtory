use self::edges::edge_ref::EdgeRef;
use super::input::input_node::parse_u64_strict;
use bytemuck::{Pod, Zeroable};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

pub mod edges;
pub mod properties;

// the only reason this is public is because the physical ids of the nodes don't move
#[repr(transparent)]
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Pod, Zeroable,
)]
pub struct VID(pub usize);

impl Default for VID {
    fn default() -> Self {
        VID(usize::MAX)
    }
}

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
    Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize, Pod, Zeroable,
)]
pub struct EID(pub usize);

impl Default for EID {
    fn default() -> Self {
        EID(usize::MAX)
    }
}

impl EID {
    pub fn as_u64(self) -> u64 {
        self.0 as u64
    }
}

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

impl EID {
    pub fn from_u64(id: u64) -> Self {
        EID(id as usize)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum GID {
    U64(u64),
    Str(String),
}

impl Default for GID {
    fn default() -> Self {
        GID::U64(u64::MAX)
    }
}

impl Display for GID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GID::U64(v) => write!(f, "{}", v),
            GID::Str(v) => write!(f, "{}", v),
        }
    }
}

impl GID {
    pub fn dtype(&self) -> GidType {
        match self {
            GID::U64(_) => GidType::U64,
            GID::Str(_) => GidType::Str,
        }
    }
    pub fn into_str(self) -> Option<String> {
        match self {
            GID::Str(v) => Some(v),
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

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_str(&self) -> Cow<str> {
        match self {
            GID::U64(v) => Cow::Owned(v.to_string()),
            GID::Str(v) => Cow::Borrowed(v),
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            GID::U64(v) => v.to_i64(),
            GID::Str(v) => parse_u64_strict(v)?.to_i64(),
        }
    }

    pub fn to_u64(&self) -> Option<u64> {
        match self {
            GID::U64(v) => Some(*v),
            GID::Str(v) => parse_u64_strict(v),
        }
    }

    pub fn as_ref(&self) -> GidRef {
        match self {
            GID::U64(v) => GidRef::U64(*v),
            GID::Str(v) => GidRef::Str(v),
        }
    }
}

impl From<u64> for GID {
    fn from(id: u64) -> Self {
        Self::U64(id)
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
            GidRef::Str(v) => GID::Str(v.to_owned()),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum GidRef<'a> {
    U64(u64),
    Str(&'a str),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GidType {
    U64,
    Str,
}

impl Display for GidType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GidType::U64 => {
                write!(f, "Numeric")
            }
            GidType::Str => {
                write!(f, "String")
            }
        }
    }
}

impl Display for GidRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GidRef::U64(v) => write!(f, "{}", v),
            GidRef::Str(v) => write!(f, "{}", v),
        }
    }
}

impl<'a> From<&'a GID> for GidRef<'a> {
    fn from(value: &'a GID) -> Self {
        match value {
            GID::U64(v) => GidRef::U64(*v),
            GID::Str(v) => GidRef::Str(v),
        }
    }
}

impl<'a> GidRef<'a> {
    pub fn dtype(self) -> GidType {
        match self {
            GidRef::U64(_) => GidType::U64,
            GidRef::Str(_) => GidType::Str,
        }
    }
    pub fn as_str(self) -> Option<&'a str> {
        match self {
            GidRef::Str(s) => Some(s),
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
            GidRef::Str(v) => GID::Str(v.to_owned()),
        }
    }

    pub fn to_str(self) -> Cow<'a, str> {
        match self {
            GidRef::U64(v) => Cow::Owned(v.to_string()),
            GidRef::Str(v) => Cow::Borrowed(v),
        }
    }

    pub fn to_i64(self) -> Option<i64> {
        match self {
            GidRef::U64(v) => v.to_i64(),
            GidRef::Str(v) => parse_u64_strict(v)?.to_i64(),
        }
    }

    pub fn to_u64(self) -> Option<u64> {
        match self {
            GidRef::U64(v) => Some(v),
            GidRef::Str(v) => parse_u64_strict(v),
        }
    }
}
