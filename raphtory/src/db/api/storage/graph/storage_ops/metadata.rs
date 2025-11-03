use raphtory_api::core::storage::arc_str::ArcStr;
use storage::api::graph::GraphEntryOps;
use crate::{
    db::api::{properties::internal::InternalMetadataOps, view::BoxedLIter},
    prelude::Prop,
};

use super::GraphStorage;

impl InternalMetadataOps for GraphStorage {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().get_metadata_id(name)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph_meta().get_metadata_name(id)
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(self.graph_meta().metadata_ids())
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        let graph_entry = self.graph_entry();

        // Return the metadata value for the given property id.
        graph_entry.get_metadata(id)
    }

    fn metadata_keys(&self) -> BoxedLIter<'_, ArcStr> {
        Box::new(self.graph_meta().metadata_names().into_iter())
    }
}
