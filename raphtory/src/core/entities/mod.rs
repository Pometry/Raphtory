use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use std::{borrow::Cow, sync::Arc};

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

impl From<Arc<[usize]>> for LayerIds {
    fn from(id: Arc<[usize]>) -> Self {
        LayerIds::Multiple(id)
    }
}
