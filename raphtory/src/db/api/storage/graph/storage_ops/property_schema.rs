use super::GraphStorage;
use crate::db::api::{
    properties::internal::{EdgePropertySchemaOps, NodePropertySchemaOps},
    view::BoxedLIter,
};
use raphtory_api::{
    core::{entities::LayerId, storage::arc_str::ArcStr},
    iter::IntoDynBoxed,
};

impl NodePropertySchemaOps for GraphStorage {
    fn node_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.node_meta()
            .temporal_prop_mapper()
            .ids()
            .into_dyn_boxed()
    }
    fn node_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.node_meta().temporal_prop_mapper().get_id(name)
    }
    fn node_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        Some(self.node_meta().temporal_prop_mapper().get_name(id).clone())
    }
    fn node_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.node_meta().metadata_mapper().ids().into_dyn_boxed()
    }
    fn node_visible_metadata_id(&self, name: &str) -> Option<usize> {
        self.node_meta().metadata_mapper().get_id(name)
    }
    fn node_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        Some(self.node_meta().metadata_mapper().get_name(id).clone())
    }
    fn node_layer_has_temporal_prop(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.nodes().layer_has_temporal_prop(layer_id, prop_id)
    }
    fn node_layer_has_metadata(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.nodes().layer_has_metadata(layer_id, prop_id)
    }
}

impl EdgePropertySchemaOps for GraphStorage {
    fn edge_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.edge_meta()
            .temporal_prop_mapper()
            .ids()
            .into_dyn_boxed()
    }
    fn edge_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.edge_meta().temporal_prop_mapper().get_id(name)
    }
    fn edge_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        Some(self.edge_meta().temporal_prop_mapper().get_name(id).clone())
    }
    fn edge_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        self.edge_meta().metadata_mapper().ids().into_dyn_boxed()
    }
    fn edge_visible_metadata_id(&self, name: &str) -> Option<usize> {
        self.edge_meta().metadata_mapper().get_id(name)
    }
    fn edge_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        Some(self.edge_meta().metadata_mapper().get_name(id).clone())
    }
    fn edge_layer_has_temporal_prop(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.edges().layer_has_temporal_prop(layer_id, prop_id)
    }
    fn edge_layer_has_metadata(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.edges().layer_has_metadata(layer_id, prop_id)
    }
}
