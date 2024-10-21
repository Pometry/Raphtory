use super::{input::input_node::parse_u64_strict, storage::arc_str::ArcStr};
use bytemuck::{Pod, Zeroable};
use edges::edge_ref::EdgeRef;
use either::Either;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
    sync::Arc,
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

#[derive(Clone, Debug)]
pub enum LayerIds {
    None,
    All,
    One(usize),
    Multiple(Arc<[usize]>),
}

impl LayerIds {
    pub fn find(&self, layer_id: usize) -> Option<usize> {
        match self {
            LayerIds::All => Some(layer_id),
            LayerIds::One(id) => {
                if *id == layer_id {
                    Some(layer_id)
                } else {
                    None
                }
            }
            LayerIds::Multiple(ids) => ids.binary_search(&layer_id).ok().map(|_| layer_id),
            LayerIds::None => None,
        }
    }

    pub fn intersect(&self, other: &LayerIds) -> LayerIds {
        match (self, other) {
            (LayerIds::None, _) => LayerIds::None,
            (_, LayerIds::None) => LayerIds::None,
            (LayerIds::All, other) => other.clone(),
            (this, LayerIds::All) => this.clone(),
            (LayerIds::One(id), other) => {
                if other.contains(id) {
                    LayerIds::One(*id)
                } else {
                    LayerIds::None
                }
            }
            (LayerIds::Multiple(ids), other) => {
                let ids: Vec<usize> = ids
                    .iter()
                    .filter(|id| other.contains(id))
                    .copied()
                    .collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
                }
            }
        }
    }

    pub fn diff<'a>(
        &self,
        unique_layers: impl IntoIterator<Item = usize>,
        other: &LayerIds,
    ) -> LayerIds {
        match (self, other) {
            (LayerIds::None, _) => LayerIds::None,
            (this, LayerIds::None) => this.clone(),
            (_, LayerIds::All) => LayerIds::None,
            (LayerIds::One(id), other) => {
                if other.contains(id) {
                    LayerIds::None
                } else {
                    LayerIds::One(*id)
                }
            }
            (LayerIds::Multiple(ids), other) => {
                let ids: Vec<usize> = ids
                    .iter()
                    .filter(|id| !other.contains(id))
                    .copied()
                    .collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
                }
            }
            (LayerIds::All, other) => {
                let all_layer_ids: Vec<usize> = unique_layers
                    .into_iter()
                    .filter(|id| !other.contains(id))
                    .collect();
                match all_layer_ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(all_layer_ids[0]),
                    _ => LayerIds::Multiple(all_layer_ids.into()),
                }
            }
        }
    }

    pub fn constrain_from_edge(&self, e: EdgeRef) -> Cow<LayerIds> {
        match e.layer() {
            None => Cow::Borrowed(self),
            Some(l) => self
                .find(l)
                .map(|id| Cow::Owned(LayerIds::One(id)))
                .unwrap_or(Cow::Owned(LayerIds::None)),
        }
    }

    pub fn contains(&self, layer_id: &usize) -> bool {
        self.find(*layer_id).is_some()
    }

    pub fn is_none(&self) -> bool {
        matches!(self, LayerIds::None)
    }
}

impl From<Vec<usize>> for LayerIds {
    fn from(mut v: Vec<usize>) -> Self {
        match v.len() {
            0 => LayerIds::All,
            1 => LayerIds::One(v[0]),
            _ => {
                v.sort_unstable();
                v.dedup();
                LayerIds::Multiple(v.into())
            }
        }
    }
}

impl<const N: usize> From<[usize; N]> for LayerIds {
    fn from(v: [usize; N]) -> Self {
        match v.len() {
            0 => LayerIds::All,
            1 => LayerIds::One(v[0]),
            _ => {
                let mut v = v.to_vec();
                v.sort_unstable();
                v.dedup();
                LayerIds::Multiple(v.into())
            }
        }
    }
}

impl From<usize> for LayerIds {
    fn from(id: usize) -> Self {
        LayerIds::One(id)
    }
}

impl From<Arc<[usize]>> for LayerIds {
    fn from(id: Arc<[usize]>) -> Self {
        LayerIds::Multiple(id)
    }
}

#[derive(Debug, Clone)]
pub enum Layer {
    All,
    None,
    Default,
    One(ArcStr),
    Multiple(Arc<[ArcStr]>),
}

trait SingleLayer {
    fn name(self) -> ArcStr;
}

impl<T: SingleLayer> From<T> for Layer {
    fn from(value: T) -> Self {
        Layer::One(value.name())
    }
}

impl SingleLayer for ArcStr {
    fn name(self) -> ArcStr {
        self
    }
}

impl SingleLayer for String {
    fn name(self) -> ArcStr {
        self.into()
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> SingleLayer for &'a T {
    fn name(self) -> ArcStr {
        self.to_owned().into()
    }
}

impl<T: SingleLayer> From<Vec<T>> for Layer {
    fn from(names: Vec<T>) -> Self {
        match names.len() {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

impl<T: SingleLayer, const N: usize> From<[T; N]> for Layer {
    fn from(names: [T; N]) -> Self {
        match N {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

#[derive(Copy, Clone, PartialOrd, PartialEq, Debug, Eq, Hash, Ord)]
pub enum NodeRef<'a> {
    Internal(VID),
    External(GidRef<'a>),
}

pub trait AsNodeRef {
    fn as_node_ref(&self) -> NodeRef;

    fn into_gid(self) -> Either<GID, VID>
    where
        Self: Sized,
    {
        match self.as_node_ref() {
            NodeRef::Internal(vid) => Either::Right(vid),
            NodeRef::External(gid) => Either::Left(gid.into()),
        }
    }

    fn as_gid_ref(&self) -> Either<GidRef, VID> {
        match self.as_node_ref() {
            NodeRef::Internal(vid) => Either::Right(vid),
            NodeRef::External(u) => Either::Left(u),
        }
    }
}

impl<'a> AsNodeRef for NodeRef<'a> {
    fn as_node_ref(&self) -> NodeRef {
        *self
    }
}

impl AsNodeRef for VID {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::Internal(*self)
    }
}

impl AsNodeRef for u64 {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::U64(*self))
    }
}

impl AsNodeRef for String {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::Str(&self))
    }
}

impl<'a> AsNodeRef for &'a str {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(GidRef::Str(self))
    }
}

impl<'a, V: AsNodeRef> AsNodeRef for &'a V {
    fn as_node_ref(&self) -> NodeRef {
        V::as_node_ref(self)
    }
}

impl AsNodeRef for GID {
    fn as_node_ref(&self) -> NodeRef {
        let gid_ref: GidRef = self.into();
        NodeRef::External(gid_ref)
    }
}

impl<'a> AsNodeRef for GidRef<'a> {
    fn as_node_ref(&self) -> NodeRef {
        NodeRef::External(*self)
    }
}

impl<'a> NodeRef<'a> {
    /// Makes a new node reference from an internal `VID`.
    /// Values are unchecked and the node is assumed to exist so use with caution!
    pub fn new(vid: VID) -> Self {
        NodeRef::Internal(vid)
    }
}
