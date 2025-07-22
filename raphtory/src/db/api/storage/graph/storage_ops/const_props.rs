use raphtory_api::core::storage::arc_str::ArcStr;

use crate::{
    db::api::{properties::internal::InternalMetadataPropertiesOps, view::BoxedLIter},
    prelude::Prop,
};

use super::GraphStorage;

impl InternalMetadataPropertiesOps for GraphStorage {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().get_metadata_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph_meta().get_metadata_name(id)
    }

    fn const_prop_ids(&self) -> BoxedLIter<usize> {
        Box::new(self.graph_meta().metadata_ids())
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_metadata(id)
    }

    fn const_prop_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.graph_meta().metadata_names().into_iter())
    }
}
