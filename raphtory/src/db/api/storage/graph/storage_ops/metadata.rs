use raphtory_api::core::storage::arc_str::ArcStr;

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

    fn metadata_ids(&self) -> BoxedLIter<usize> {
        Box::new(self.graph_meta().metadata_ids())
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_metadata(id)
    }

    fn metadata_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.graph_meta().metadata_names().into_iter())
    }
}
