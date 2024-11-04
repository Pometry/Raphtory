use std::{borrow::Cow, sync::Arc};

use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub mod edges;
pub mod graph;
pub mod nodes;
pub mod properties;

pub use raphtory_api::core::entities::*;

#[derive(Clone, Debug)]
pub enum LayerIds {
    None,
    All,
    One(usize),
    Multiple(Multiple),
}

#[derive(Clone, Debug, Default)]
pub struct Multiple(pub Arc<[usize]>);

impl Multiple {
    #[inline]
    pub fn binary_search(&self, pos: &usize) -> Option<usize> {
        self.0.binary_search(pos).ok()
    }

    #[inline]
    pub fn into_iter(&self) -> impl Iterator<Item = usize> {
        let ids = self.0.clone();
        (0..ids.len()).map(move |i| ids[i])
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().copied()
    }

    #[inline]
    pub fn find(&self, id: usize) -> Option<usize> {
        self.0.get(id).copied()
    }

    #[inline]
    pub fn par_iter(&self) -> impl rayon::iter::ParallelIterator<Item = usize> {
        let bit_vec = self.0.clone();
        (0..bit_vec.len()).into_par_iter().map(move |i| bit_vec[i])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl FromIterator<usize> for Multiple {
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        Multiple(iter.into_iter().collect())
    }
}

impl From<Vec<usize>> for Multiple {
    fn from(v: Vec<usize>) -> Self {
        v.into_iter().collect()
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
        let mut bm: Multiple = [1].into_iter().collect();
        let actual = bm.into_iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![1usize]);
    }

    #[test]
    fn set_two() {
        let mut bm: Multiple = [1, 67].into_iter().collect();

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

    pub fn diff<'a>(
        &self,
        graph: impl crate::prelude::GraphViewOps<'a>,
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
                let ids: Vec<usize> = ids.iter().filter(|id| !other.contains(id)).collect();
                match ids.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(ids[0]),
                    _ => LayerIds::Multiple(ids.into()),
                }
            }
            (LayerIds::All, other) => {
                let all_layer_ids: Vec<usize> = graph
                    .unique_layers()
                    .map(|name| graph.get_layer_id(name.as_ref()).unwrap())
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
