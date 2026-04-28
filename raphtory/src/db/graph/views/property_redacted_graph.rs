use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::{
            EdgePropertySchemaOps, InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps,
            InheritPropertiesOps, NodePropertySchemaOps,
        },
        view::internal::{
            EdgeTimeSemanticsOps, GraphTimeSemanticsOps, GraphView, Immutable,
            InheritAllEdgeFilterOps, InheritCoreGraphOps, InheritEdgeHistoryFilter,
            InheritListOps, InheritMaterialize, InheritNodeHistoryFilter, InheritNodeFilterOps,
            InheritStorageOps, InheritTimeSemantics, Static,
        },
    },
    db::api::view::BoxedLIter,
};
use raphtory_api::{core::storage::arc_str::ArcStr, inherit::Base};
use raphtory_storage::layer_ops::InheritLayerOps;
use std::{collections::HashSet, sync::Arc};

/// Per-entity, per-category property restrictions.
/// Built once from the stored access filter and carried through `GraphPermission::Read`.
#[derive(Clone, Debug, Default)]
pub struct PropertyRedaction {
    pub node_hidden_props: Arc<HashSet<String>>,
    pub node_hidden_meta: Arc<HashSet<String>>,
    pub edge_hidden_props: Arc<HashSet<String>>,
    pub edge_hidden_meta: Arc<HashSet<String>>,
    pub graph_hidden_props: Arc<HashSet<String>>,
    pub graph_hidden_meta: Arc<HashSet<String>>,
}

impl PropertyRedaction {
    pub fn has_restrictions(&self) -> bool {
        !self.node_hidden_props.is_empty()
            || !self.node_hidden_meta.is_empty()
            || !self.edge_hidden_props.is_empty()
            || !self.edge_hidden_meta.is_empty()
            || !self.graph_hidden_props.is_empty()
            || !self.graph_hidden_meta.is_empty()
    }
}

/// Graph view that hides specified property keys from node and edge responses.
/// Applied once at graph-open time; all downstream APIs see only permitted properties.
#[derive(Clone)]
pub struct PropertyRedactedGraph<G> {
    pub graph: G,
    pub redaction: Arc<PropertyRedaction>,
}

impl<G> PropertyRedactedGraph<G> {
    pub fn new(graph: G, redaction: Arc<PropertyRedaction>) -> Self {
        Self { graph, redaction }
    }
}

impl<G> Static for PropertyRedactedGraph<G> {}
impl<G: GraphView> Immutable for PropertyRedactedGraph<G> {}

impl<G: GraphView> Base for PropertyRedactedGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &G {
        &self.graph
    }
}

impl<G: GraphView> InheritTimeSemantics for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritListOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritCoreGraphOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritMaterialize for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritPropertiesOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritStorageOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritEdgeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritAllEdgeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritLayerOps for PropertyRedactedGraph<G> {}

// PropertyRedactedGraph overrides NodePropertySchemaOps to filter hidden keys.
// It does NOT implement the inherit markers — it provides its own implementation directly.

impl<G: GraphView + NodePropertySchemaOps> NodePropertySchemaOps for PropertyRedactedGraph<G> {
    fn node_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        let hidden = self.redaction.node_hidden_props.as_ref();
        Box::new(self.graph.node_visible_temporal_prop_ids().filter(move |&id| {
            self.graph
                .node_visible_temporal_prop_name(id)
                .map_or(false, |name| !hidden.contains(name.as_ref()))
        }))
    }

    fn node_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        if self.redaction.node_hidden_props.contains(name) {
            return None;
        }
        self.graph.node_visible_temporal_prop_id(name)
    }

    fn node_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        let name = self.graph.node_visible_temporal_prop_name(id)?;
        (!self.redaction.node_hidden_props.contains(name.as_ref())).then_some(name)
    }

    fn node_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        let hidden = self.redaction.node_hidden_meta.as_ref();
        Box::new(self.graph.node_visible_metadata_ids().filter(move |&id| {
            self.graph
                .node_visible_metadata_name(id)
                .map_or(false, |name| !hidden.contains(name.as_ref()))
        }))
    }

    fn node_visible_metadata_id(&self, name: &str) -> Option<usize> {
        if self.redaction.node_hidden_meta.contains(name) {
            return None;
        }
        self.graph.node_visible_metadata_id(name)
    }

    fn node_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        let name = self.graph.node_visible_metadata_name(id)?;
        (!self.redaction.node_hidden_meta.contains(name.as_ref())).then_some(name)
    }
}

impl<G: GraphView + EdgePropertySchemaOps> EdgePropertySchemaOps for PropertyRedactedGraph<G> {
    fn edge_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        let hidden = self.redaction.edge_hidden_props.as_ref();
        Box::new(self.graph.edge_visible_temporal_prop_ids().filter(move |&id| {
            self.graph
                .edge_visible_temporal_prop_name(id)
                .map_or(false, |name| !hidden.contains(name.as_ref()))
        }))
    }

    fn edge_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        if self.redaction.edge_hidden_props.contains(name) {
            return None;
        }
        self.graph.edge_visible_temporal_prop_id(name)
    }

    fn edge_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        let name = self.graph.edge_visible_temporal_prop_name(id)?;
        (!self.redaction.edge_hidden_props.contains(name.as_ref())).then_some(name)
    }

    fn edge_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        let hidden = self.redaction.edge_hidden_meta.as_ref();
        Box::new(self.graph.edge_visible_metadata_ids().filter(move |&id| {
            self.graph
                .edge_visible_metadata_name(id)
                .map_or(false, |name| !hidden.contains(name.as_ref()))
        }))
    }

    fn edge_visible_metadata_id(&self, name: &str) -> Option<usize> {
        if self.redaction.edge_hidden_meta.contains(name) {
            return None;
        }
        self.graph.edge_visible_metadata_id(name)
    }

    fn edge_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        let name = self.graph.edge_visible_metadata_name(id)?;
        (!self.redaction.edge_hidden_meta.contains(name.as_ref())).then_some(name)
    }
}
