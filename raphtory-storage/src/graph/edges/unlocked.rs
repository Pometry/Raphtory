use raphtory_api::core::entities::EID;
use raphtory_core::{
    entities::graph::tgraph_storage::GraphStorage, storage::raw_edges::EdgeRGuard,
};
use rayon::prelude::*;
use storage::{Extension, Layer};

#[derive(Copy, Clone, Debug)]
pub struct UnlockedEdges<'a>(pub(crate) &'a Layer<Extension>);

impl<'a> UnlockedEdges<'a> {
    pub fn iter(self) -> impl Iterator<Item = EdgeRGuard<'a>> + 'a {
        let storage = self.0;
        (0..storage.edges_len())
            .map(EID)
            .map(|eid| storage.edge_entry(eid))
    }

    pub fn par_iter(self) -> impl ParallelIterator<Item = EdgeRGuard<'a>> + 'a {
        let storage = self.0;
        (0..storage.edges_len())
            .into_par_iter()
            .map(EID)
            .map(|eid| storage.edge_entry(eid))
    }

    #[inline]
    pub fn len(self) -> usize {
        self.0.edges_len()
    }
}
