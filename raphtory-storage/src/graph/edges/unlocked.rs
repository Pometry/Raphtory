use raphtory_core::entities::{LayerIds, EID};
use rayon::prelude::*;
use storage::{pages::edge_store::EdgeStorageInner, utils::Iter4, Extension, Layer};

use crate::graph::edges::edge_entry::EdgeStorageEntry;

#[derive(Copy, Clone, Debug)]
pub struct UnlockedEdges<'a>(pub(crate) &'a Layer<Extension>);

impl<'a> UnlockedEdges<'a> {
    pub fn storage(&self) -> &'a EdgeStorageInner<storage::ES<Extension>, Extension> {
        self.0.edges()
    }

    pub fn edge(&self, e_id: EID) -> EdgeStorageEntry<'a> {
        EdgeStorageEntry::Unlocked(self.0.edges().edge(e_id))
    }

    pub fn iter_layer(self, layer_id: usize) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
        self.0
            .edges()
            .iter(layer_id)
            .map(EdgeStorageEntry::Unlocked)
    }

    pub fn iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::All => Iter4::J(self.iter_layer(0)),
            LayerIds::One(layer_id) => Iter4::K(self.iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.iter_layer(0)
                    .filter(|edge| edge.as_ref().has_layers(multiple)),
            ),
        }
    }

    pub fn par_iter_layer(
        self,
        layer_id: usize,
    ) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> + 'a {
        self.0
            .edges()
            .par_iter(layer_id)
            .map(EdgeStorageEntry::Unlocked)
    }

    pub fn par_iter(
        self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> + 'a {
        match layer_ids {
            LayerIds::None => Iter4::I(rayon::iter::empty()),
            LayerIds::All => Iter4::J(self.par_iter_layer(0)),
            LayerIds::One(layer_id) => Iter4::K(self.par_iter_layer(*layer_id)),
            LayerIds::Multiple(multiple) => Iter4::L(
                self.par_iter_layer(0)
                    .filter(|edge| edge.as_ref().has_layers(multiple)),
            ),
        }
    }
}
