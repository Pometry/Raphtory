use super::input::input_node::parse_u64_strict;
use crate::iter::IntoDynBoxed;
use bytemuck::{Pod, Zeroable};
use edges::edge_ref::EdgeRef;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt::{Debug, Display, Formatter},
    iter,
};

pub mod edges;
pub mod layers;
pub mod properties;

pub use layers::*;

// The only reason this is public is because the physical IDs of the nodes don’t move.
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

    pub fn with_layer(self, layer: usize) -> ELID {
        ELID::new(self, layer)
    }

    pub fn with_layer_deletion(self, layer: usize) -> ELID {
        ELID::new_deletion(self, layer)
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

impl From<ELID> for EID {
    fn from(elid: ELID) -> Self {
        elid.edge
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct ELID {
    pub edge: EID,
    layer_and_deletion: usize,
}

impl Debug for ELID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ELID")
            .field("edge", &self.edge)
            .field("layer", &self.layer())
            .field("deletion", &self.is_deletion())
            .finish()
    }
}

const LAYER_FLAG: usize = 1usize.reverse_bits();
pub const MAX_LAYER: usize = usize::MAX & !LAYER_FLAG;

impl ELID {
    pub fn new(edge: EID, layer: usize) -> Self {
        ELID {
            edge,
            layer_and_deletion: layer,
        }
    }

    pub fn new_deletion(edge: EID, layer: usize) -> Self {
        ELID {
            edge,
            layer_and_deletion: layer | LAYER_FLAG,
        }
    }

    pub fn layer(&self) -> usize {
        self.layer_and_deletion & !LAYER_FLAG
    }

    pub fn is_deletion(&self) -> bool {
        self.layer_and_deletion & LAYER_FLAG != 0
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum GID {
    U64(u64),
    Str(String),
}

impl PartialEq<str> for GID {
    fn eq(&self, other: &str) -> bool {
        match self {
            GID::U64(_) => false,
            GID::Str(id) => id == other,
        }
    }
}

impl PartialEq<String> for GID {
    fn eq(&self, other: &String) -> bool {
        match self {
            GID::U64(_) => false,
            GID::Str(id) => id == other,
        }
    }
}

impl PartialEq<u64> for GID {
    fn eq(&self, other: &u64) -> bool {
        match self {
            GID::Str(_) => false,
            GID::U64(id) => id == other,
        }
    }
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

impl<'a> From<&'a str> for GidRef<'a> {
    fn from(value: &'a str) -> Self {
        GidRef::Str(value)
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

#[cfg(test)]
mod test {
    use crate::core::entities::Multiple;

    #[test]
    fn empty_bit_multiple() {
        let bm = super::Multiple::default();
        let actual = bm.into_iter().collect::<Vec<_>>();
        let expected: Vec<usize> = vec![];
        assert_eq!(actual, expected);
    }

    #[test]
    fn set_one() {
        let bm: Multiple = [1].into_iter().collect();
        let actual = bm.into_iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![1usize]);
    }

    #[test]
    fn set_two() {
        let bm: Multiple = [1, 67].into_iter().collect();

        let actual = bm.into_iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![1usize, 67]);
    }
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
            LayerIds::Multiple(ids) => ids.binary_search(&layer_id).map(|_| layer_id),
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
                let ids: Vec<usize> = ids.iter().filter(|id| other.contains(id)).collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
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

    pub fn is_single(&self) -> bool {
        matches!(self, LayerIds::One(_))
    }

    pub fn iter(&self, num_layers: usize) -> impl Iterator<Item = usize> + use<'_> {
        match self {
            LayerIds::None => iter::empty().into_dyn_boxed(),
            LayerIds::All => (0..num_layers).into_dyn_boxed(),
            LayerIds::One(id) => iter::once(*id).into_dyn_boxed(),
            LayerIds::Multiple(ids) => ids.into_iter().into_dyn_boxed(),
        }
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

#[cfg(test)]
mod tests {
    use crate::core::entities::{EID, MAX_LAYER};
    use proptest::{prop_assert, prop_assert_eq, proptest};

    #[test]
    fn test_elid_layer() {
        proptest!(|(eid in 0..=usize::MAX, layer in 0..=MAX_LAYER)| {
            let elid = EID(eid).with_layer(layer);
            prop_assert_eq!(elid.layer(), layer);
            prop_assert!(!elid.is_deletion());
        })
    }

    #[test]
    fn test_elid_deletion() {
        proptest!(|(eid in 0..=usize::MAX, layer in 0..=MAX_LAYER)| {
            let elid = EID(eid).with_layer_deletion(layer);
            prop_assert_eq!(elid.layer(), layer);
            prop_assert!(elid.is_deletion());
        })
    }
}
