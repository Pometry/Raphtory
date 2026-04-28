use crate::{
    db::api::{
        properties::internal::{
            EdgePropertySchemaOps, InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps,
            InheritPropertiesOps, NodePropertySchemaOps,
        },
        view::internal::{
            GraphView, Immutable, InheritAllEdgeFilterOps, InheritCoreGraphOps,
            InheritEdgeHistoryFilter, InheritListOps, InheritMaterialize,
            InheritNodeHistoryFilter, InheritNodeFilterOps, InheritStorageOps,
            InheritTimeSemantics, Static,
        },
    },
    db::api::view::BoxedLIter,
};
use raphtory_api::{core::storage::arc_str::ArcStr, inherit::Base};
use raphtory_storage::layer_ops::InheritLayerOps;
use std::collections::HashSet;

/// Per-entity, per-category property restrictions.
/// Built once from the stored access filter and carried through `GraphPermission::Read`.
/// `graph_hidden_*` are used by the GQL layer for graph-own properties; the node/edge sets are
/// consumed at `PropertyRedactedGraph::new()` time to build the visibility arrays.
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

/// Precompute a boolean visibility array indexed by property ID.
/// IDs not in the array (out of range) default to visible.
fn compute_visibility<F>(ids: BoxedLIter<usize>, name_fn: F, hidden: &HashSet<String>) -> Box<[bool]>
where
    F: Fn(usize) -> Option<ArcStr>,
{
    let ids: Vec<usize> = ids.collect();
    if ids.is_empty() {
        return Box::new([]);
    }
    let max_id = *ids.iter().max().unwrap();
    let mut visible = vec![true; max_id + 1];
    for id in ids {
        if let Some(name) = name_fn(id) {
            if hidden.contains(name.as_ref()) {
                visible[id] = false;
            }
        }
    }
    visible.into_boxed_slice()
}

/// Graph view that hides specified property keys from node and edge responses.
/// Applied once at graph-open time; all downstream APIs see only permitted properties.
///
/// Visibility is precomputed into `Box<[bool]>` arrays (indexed by prop ID) at construction
/// to avoid the id → name → hash-set lookup cycle on every property access.
///
/// TODO: expose via a `GraphViewOps` method so this can be used outside of the auth layer
/// (e.g. in materialisation pipelines).
#[derive(Clone)]
pub struct PropertyRedactedGraph<G> {
    pub graph: G,
    node_props_visible: Box<[bool]>,
    node_meta_visible: Box<[bool]>,
    edge_props_visible: Box<[bool]>,
    edge_meta_visible: Box<[bool]>,
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
        Self {
            graph,
            node_props_visible,
            node_meta_visible,
            edge_props_visible,
            edge_meta_visible,
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
impl<G: GraphView> InheritPropertiesOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritStorageOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritEdgeHistoryFilter for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritNodeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritAllEdgeFilterOps for PropertyRedactedGraph<G> {}
impl<G: GraphView> InheritLayerOps for PropertyRedactedGraph<G> {}

// PropertyRedactedGraph overrides NodePropertySchemaOps and EdgePropertySchemaOps to filter
// hidden keys. It does NOT implement the inherit markers — it provides its own implementations.

#[inline(always)]
fn is_visible(visible: &[bool], id: usize) -> bool {
    visible.get(id).copied().unwrap_or(true)
}

impl<G: GraphView> NodePropertySchemaOps for PropertyRedactedGraph<G> {
    fn node_visible_temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .node_visible_temporal_prop_ids()
                .filter(|&id| is_visible(&self.node_props_visible, id)),
        )
    }

    fn node_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.node_visible_temporal_prop_id(name)?;
        is_visible(&self.node_props_visible, id).then_some(id)
    }

    fn node_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.node_props_visible, id) {
            return None;
        }
        self.graph.node_visible_temporal_prop_name(id)
    }

    fn node_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .node_visible_metadata_ids()
                .filter(|&id| is_visible(&self.node_meta_visible, id)),
        )
    }

    fn node_visible_metadata_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.node_visible_metadata_id(name)?;
        is_visible(&self.node_meta_visible, id).then_some(id)
    }

    fn node_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.node_meta_visible, id) {
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
                .filter(|&id| is_visible(&self.edge_props_visible, id)),
        )
    }

    fn edge_visible_temporal_prop_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.edge_visible_temporal_prop_id(name)?;
        is_visible(&self.edge_props_visible, id).then_some(id)
    }

    fn edge_visible_temporal_prop_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.edge_props_visible, id) {
            return None;
        }
        self.graph.edge_visible_temporal_prop_name(id)
    }

    fn edge_visible_metadata_ids(&self) -> BoxedLIter<'_, usize> {
        Box::new(
            self.graph
                .edge_visible_metadata_ids()
                .filter(|&id| is_visible(&self.edge_meta_visible, id)),
        )
    }

    fn edge_visible_metadata_id(&self, name: &str) -> Option<usize> {
        let id = self.graph.edge_visible_metadata_id(name)?;
        is_visible(&self.edge_meta_visible, id).then_some(id)
    }

    fn edge_visible_metadata_name(&self, id: usize) -> Option<ArcStr> {
        if !is_visible(&self.edge_meta_visible, id) {
            return None;
        }
        self.graph.edge_visible_metadata_name(id)
    }
}
