use crate::{
    db::api::{properties::internal::InternalMetadataOps, view::BoxedLIter},
    prelude::Prop,
};
use raphtory_api::{core::storage::arc_str::ArcStr, iter::IntoDynBoxed};
use storage::api::graph_props::{GraphPropEntryOps, GraphPropRefOps};

use super::GraphStorage;

impl InternalMetadataOps for GraphStorage {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.graph_props_meta().metadata_mapper().get_id(name)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph_props_meta()
            .metadata_mapper()
            .get_name(id)
            .clone()
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph_props_meta()
            .metadata_mapper()
            .ids()
            .into_dyn_boxed()
    }

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        let graph_entry = self.graph_entry();

        // Return the metadata value for the given property id.
        graph_entry.as_ref().get_metadata(id)
    }

    fn metadata_keys(&self) -> BoxedLIter<'_, ArcStr> {
        self.graph_props_meta()
            .metadata_mapper()
            .keys()
            .into_iter()
            .into_dyn_boxed()
    }
}
