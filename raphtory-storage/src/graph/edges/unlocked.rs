use raphtory_api::core::entities::EID;
use raphtory_core::storage::raw_edges::EdgeRGuard;
use rayon::prelude::*;
use storage::{Extension, Layer};

use crate::graph::edges::edge_entry::EdgeStorageEntry;

#[derive(Copy, Clone, Debug)]
pub struct UnlockedEdges<'a>(pub(crate) &'a Layer<Extension>);

impl<'a> UnlockedEdges<'a> {
    pub fn iter(self, layer_id: usize) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
        self.0
            .edges()
            .iter(layer_id)
            .map(EdgeStorageEntry::Unlocked)
    }

    pub fn par_iter(
        self,
        layer_id: usize,
    ) -> impl ParallelIterator<Item = EdgeStorageEntry<'a>> + 'a {
        self.0
            .edges()
            .par_iter(layer_id)
            .map(EdgeStorageEntry::Unlocked)
    }

    #[inline]
    pub fn len(self) -> usize {
        self.0.edges().num_edges()
    }
}
