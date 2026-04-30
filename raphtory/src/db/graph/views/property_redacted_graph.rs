use crate::db::api::{
    properties::internal::{
        EdgePropertySchemaOps, InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps,
        InheritTemporalPropertyViewOps, InternalMetadataOps, InternalTemporalPropertiesOps,
        NodePropertySchemaOps,
    },
    view::{
        internal::{
            GraphView, Immutable, InheritAllEdgeFilterOps, InheritCoreGraphOps,
            InheritEdgeHistoryFilter, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
            InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics, Static,
        },
        BoxedLIter,
    },
};
use raphtory_api::{core::storage::arc_str::ArcStr, inherit::Base};
use raphtory_storage::layer_ops::InheritLayerOps;
use std::{collections::HashSet, sync::Arc};

/// Per-entity, per-category property restrictions.
/// Built once from the stored access filter and carried through `GraphPermission::Read`.
/// All sets are consumed at `PropertyRedactedGraph::new()` time to build the visibility arrays.
#[derive(Clone, Debug, Default)]
pub struct PropertyRedaction {
    pub node_hidden_props: HashSet<String>,
    pub node_hidden_meta: HashSet<String>,
    pub edge_hidden_props: HashSet<String>,
    pub edge_hidden_meta: HashSet<String>,
    pub graph_hidden_props: HashSet<String>,
    pub graph_hidden_meta: HashSet<String>,
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

/// Precomputed boolean visibility arrays for all property categories.
/// Wrapped in `Arc` so `PropertyRedactedGraph<G>: Clone` is cheap.
pub struct RedactionArrays {
    node_props_visible: Box<[bool]>,
    node_meta_visible: Box<[bool]>,
    edge_props_visible: Box<[bool]>,
    edge_meta_visible: Box<[bool]>,
    graph_props_visible: Box<[bool]>,
    graph_meta_visible: Box<[bool]>,
}

/// Precompute a boolean visibility array indexed by property ID.
/// IDs not in the array (out of range) default to hidden — properties added after the filter
/// was configured are conservatively treated as redacted.
fn compute_visibility<F>(
    ids: BoxedLIter<usize>,
    name_fn: F,
    hidden: &HashSet<String>,
) -> Box<[bool]>
where
    F: Fn(usize) -> Option<ArcStr>,
{
    let ids: Vec<usize> = ids.collect();
    if ids.is_empty() {
        return Box::new([]);
    }
    let max_id = *ids.iter().max().unwrap();
    // Default false (hidden); only mark visible IDs explicitly.
    let mut visible = vec![false; max_id + 1];
    for id in ids {
        if let Some(name) = name_fn(id) {
            if !hidden.contains(name.as_ref()) {
                visible[id] = true;
            }
        }
    }
    visible.into_boxed_slice()
}

/// Graph view that hides specified property keys from node, edge, and graph-level responses.
/// Applied once at graph-open time; all downstream APIs see only permitted properties.
///
/// Visibility is precomputed into `Box<[bool]>` arrays (indexed by prop ID) at construction
/// to avoid the id → name → hash-set lookup cycle on every property access.
/// Arrays are stored behind `Arc` so cloning the view is O(1).
#[derive(Clone)]
pub struct PropertyRedactedGraph<G> {
    pub graph: G,
    redaction: Arc<RedactionArrays>,
}

impl<G: GraphView> PropertyRedactedGraph<G> {
    pub fn new(graph: G, redaction: &PropertyRedaction) -> Self {
        let node_props_visible = compute_visibility(
            graph.node_visible_temporal_prop_ids(),
            |id| graph.node_visible_temporal_prop_name(id),
            &redaction.node_hidden_props,
        );
        let node_meta_visible = compute_visibility(
            graph.node_visible_metadata_ids(),
            |id| graph.node_visible_metadata_name(id),
            &redaction.node_hidden_meta,
        );
        let edge_props_visible = compute_visibility(
            graph.edge_visible_temporal_prop_ids(),
            |id| graph.edge_visible_temporal_prop_name(id),
            &redaction.edge_hidden_props,
        );
        let edge_meta_visible = compute_visibility(
            graph.edge_visible_metadata_ids(),
            |id| graph.edge_visible_metadata_name(id),
            &redaction.edge_hidden_meta,
        );
        let graph_props_visible = compute_visibility(
            graph.temporal_prop_ids(),
            |id| Some(graph.get_temporal_prop_name(id)),
            &redaction.graph_hidden_props,
        );
        let graph_meta_visible = compute_visibility(
            graph.metadata_ids(),
            |id| Some(graph.get_metadata_name(id)),
            &redaction.graph_hidden_meta,
        );
        Self {
            graph,
            redaction: Arc::new(RedactionArrays {
                node_props_visible,
                node_meta_visible,
                edge_props_visible,
                edge_meta_visible,
                graph_props_visible,
                graph_meta_visible,
            }),
        }
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
impl<G: GraphView> InheritStorageOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritEdgeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritAllEdgeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritLayerOps for PropertyRedactedGraph<G> {}

// Temporal property view ops (dtype, history iteration) are delegated to the base graph —
// redaction only affects which IDs are visible, not the values themselves.
impl<G: GraphView> InheritTemporalPropertyViewOps for PropertyRedactedGraph<G> {}

// PropertyRedactedGraph overrides NodePropertySchemaOps and EdgePropertySchemaOps to filter
// hidden keys. It does NOT implement the inherit markers — it provides its own implementations.

#[inline(always)]
fn is_visible(visible: &[bool], id: usize) -> bool {
    visible.get(id).copied().unwrap_or(false)
}

impl<G: GraphView> NodePropertySchemaOps for PropertyRedactedGraph<G> {
    fn node_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .node_visible_temporal_prop_ids()
                .filter(|&id| is_visible(&self.redaction.node_props_visible, id)),
        )
    }

    fn node_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.node_visible_temporal_prop_id(name)?;
        is_visible(&self.redaction.node_props_visible, id).then_some(id)
    }

    fn node_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.redaction.node_props_visible, id) {
            return None;
        }
        self.graph.node_visible_temporal_prop_name(id)
    }

    fn node_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .node_visible_metadata_ids()
                .filter(|&id| is_visible(&self.redaction.node_meta_visible, id)),
        )
    }

    fn node_visible_metadata_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.node_visible_metadata_id(name)?;
        is_visible(&self.redaction.node_meta_visible, id).then_some(id)
    }

    fn node_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.redaction.node_meta_visible, id) {
            return None;
        }
        self.graph.node_visible_metadata_name(id)
    }
}

impl<G: GraphView> EdgePropertySchemaOps for PropertyRedactedGraph<G> {
    fn edge_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .edge_visible_temporal_prop_ids()
                .filter(|&id| is_visible(&self.redaction.edge_props_visible, id)),
        )
    }

    fn edge_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.edge_visible_temporal_prop_id(name)?;
        is_visible(&self.redaction.edge_props_visible, id).then_some(id)
    }

    fn edge_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.redaction.edge_props_visible, id) {
            return None;
        }
        self.graph.edge_visible_temporal_prop_name(id)
    }

    fn edge_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .edge_visible_metadata_ids()
                .filter(|&id| is_visible(&self.redaction.edge_meta_visible, id)),
        )
    }

    fn edge_visible_metadata_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.edge_visible_metadata_id(name)?;
        is_visible(&self.redaction.edge_meta_visible, id).then_some(id)
    }

    fn edge_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.redaction.edge_meta_visible, id) {
            return None;
        }
        self.graph.edge_visible_metadata_name(id)
    }
}

// Graph-level property redaction: override InternalTemporalPropertiesOps and InternalMetadataOps
// so that graph.properties() and graph.metadata() only surface visible keys.
// InheritPropertiesOps is NOT used — we provide direct implementations here.

impl<G: GraphView> InternalTemporalPropertiesOps for PropertyRedactedGraph<G> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.get_temporal_prop_id(name)?;
        is_visible(&self.redaction.graph_props_visible, id).then_some(id)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph.get_temporal_prop_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .temporal_prop_ids()
                .filter(|&id| is_visible(&self.redaction.graph_props_visible, id)),
        )
    }
}

impl<G: GraphView> InternalMetadataOps for PropertyRedactedGraph<G> {
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.get_metadata_id(name)?;
        is_visible(&self.redaction.graph_meta_visible, id).then_some(id)
    }

    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.graph.get_metadata_name(id)
    }

    fn metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .metadata_ids()
                .filter(|&id| is_visible(&self.redaction.graph_meta_visible, id)),
        )
    }

    fn get_metadata(&self, id: usize) -> Option<raphtory_api::core::entities::properties::prop::Prop> {
        if !is_visible(&self.redaction.graph_meta_visible, id) {
            return None;
        }
        self.graph.get_metadata(id)
    }
}
