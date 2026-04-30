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
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::arc_str::ArcStr},
    inherit::Base,
};
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

    fn get_metadata(&self, id: usize) -> Option<Prop> {
        if !is_visible(&self.redaction.graph_meta_visible, id) {
            return None;
        }
        self.graph.get_metadata(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::graph::graph::Graph, prelude::*};

    fn make_graph() -> Graph {
        let g = Graph::new();
        // node with temporal prop "salary" and metadata "ssn"
        let n = g
            .add_node(
                1,
                "alice",
                [
                    ("salary", Prop::I64(100_000)),
                    ("region", Prop::str("us-west")),
                ],
                None,
                None,
            )
            .unwrap();
        n.add_metadata([
            ("ssn", Prop::str("123-45-6789")),
            ("dept", Prop::str("eng")),
        ])
        .unwrap();
        // edge with temporal prop "amount" and metadata "ref"
        let e = g
            .add_edge(
                1,
                "alice",
                "bob",
                [("amount", Prop::I64(50)), ("kind", Prop::str("transfer"))],
                None,
            )
            .unwrap();
        e.add_metadata(
            [("ref", Prop::str("REF001")), ("channel", Prop::str("web"))],
            None,
        )
        .unwrap();
        // graph-level temporal prop and metadata
        g.add_properties(
            1,
            [("env", Prop::str("prod")), ("version", Prop::str("1.0"))],
        )
        .unwrap();
        g.add_metadata([("secret", Prop::str("s3cr3t")), ("org", Prop::str("acme"))])
            .unwrap();
        g
    }

    #[test]
    fn test_exclude_node_temporal_property() {
        let g = make_graph();
        let view = g.exclude_node_properties(["salary"]);
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be visible"
        );
    }

    #[test]
    fn test_exclude_node_metadata() {
        let g = make_graph();
        let view = g.exclude_node_metadata(["ssn"]);
        let alice = view.node("alice").unwrap();
        assert!(
            alice.metadata().get("ssn").is_none(),
            "ssn metadata should be hidden"
        );
        assert!(
            alice.metadata().get("dept").is_some(),
            "dept should be visible"
        );
        // temporal properties are unaffected
        assert!(
            alice.properties().get("salary").is_some(),
            "salary temporal prop should still be visible"
        );
    }

    #[test]
    fn test_exclude_node_properties_does_not_hide_metadata() {
        let g = make_graph();
        // exclude_node_properties only hides temporal props, not metadata
        let view = g.exclude_node_properties(["salary"]);
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary temporal prop should be hidden"
        );
        assert!(
            alice.metadata().get("ssn").is_some(),
            "ssn metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_edge_properties() {
        let g = make_graph();
        let view = g.exclude_edge_properties(["amount"]);
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            e.properties().get("amount").is_none(),
            "amount should be hidden"
        );
        assert!(
            e.properties().get("kind").is_some(),
            "kind should be visible"
        );
        assert!(
            e.metadata().get("ref").is_some(),
            "ref metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_edge_metadata() {
        let g = make_graph();
        let view = g.exclude_edge_metadata(["ref"]);
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            e.metadata().get("ref").is_none(),
            "ref metadata should be hidden"
        );
        assert!(
            e.metadata().get("channel").is_some(),
            "channel should be visible"
        );
        assert!(
            e.properties().get("amount").is_some(),
            "amount temporal prop should still be visible"
        );
    }

    #[test]
    fn test_exclude_graph_properties() {
        let g = make_graph();
        let view = g.exclude_graph_properties(["env"]);
        assert!(
            view.properties().get("env").is_none(),
            "env should be hidden"
        );
        assert!(
            view.properties().get("version").is_some(),
            "version should be visible"
        );
        assert!(
            view.metadata().get("secret").is_some(),
            "secret metadata should still be visible"
        );
    }

    #[test]
    fn test_exclude_graph_metadata() {
        let g = make_graph();
        let view = g.exclude_graph_metadata(["secret"]);
        assert!(
            view.metadata().get("secret").is_none(),
            "secret metadata should be hidden"
        );
        assert!(
            view.metadata().get("org").is_some(),
            "org should be visible"
        );
        assert!(
            view.properties().get("env").is_some(),
            "env temporal prop should still be visible"
        );
    }

    #[test]
    fn test_chaining_node_and_edge() {
        let g = make_graph();
        let view = g
            .exclude_node_properties(["salary"])
            .exclude_edge_properties(["amount"]);
        let alice = view.node("alice").unwrap();
        let e = view.edge("alice", "bob").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be visible"
        );
        assert!(
            e.properties().get("amount").is_none(),
            "amount should be hidden"
        );
        assert!(
            e.properties().get("kind").is_some(),
            "kind should be visible"
        );
    }

    #[test]
    fn test_materialize_redacts_node_temporal_property() {
        let g = make_graph();
        let view = g.exclude_node_properties(["salary"]);
        let materialized = view.materialize().unwrap();
        let alice = materialized.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be absent from materialized graph"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_node_metadata() {
        let g = make_graph();
        let view = g.exclude_node_metadata(["ssn"]);
        let materialized = view.materialize().unwrap();
        let alice = materialized.node("alice").unwrap();
        assert!(
            alice.metadata().get("ssn").is_none(),
            "ssn should be absent from materialized graph"
        );
        assert!(
            alice.metadata().get("dept").is_some(),
            "dept should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_edge_metadata() {
        let g = make_graph();
        let view = g.exclude_edge_metadata(["ref"]);
        let materialized = view.materialize().unwrap();
        let e = materialized.edge("alice", "bob").unwrap();
        assert!(
            e.metadata().get("ref").is_none(),
            "ref should be absent from materialized graph"
        );
        assert!(
            e.metadata().get("channel").is_some(),
            "channel should be in materialized graph"
        );
    }

    #[test]
    fn test_materialize_redacts_graph_metadata() {
        let g = make_graph();
        let view = g.exclude_graph_metadata(["secret"]);
        let materialized = view.materialize().unwrap();
        assert!(
            materialized.metadata().get("secret").is_none(),
            "secret should be absent from materialized graph"
        );
        assert!(
            materialized.metadata().get("org").is_some(),
            "org should be in materialized graph"
        );
    }

    #[test]
    fn test_window_then_exclude() {
        let g = make_graph();
        // Chaining with time operations works because WindowedGraph: GraphViewOps
        let view = g.window(0, 2).exclude_node_properties(["salary"]);
        let alice = view.node("alice").unwrap();
        assert!(
            alice.properties().get("salary").is_none(),
            "salary should be hidden in windowed+redacted view"
        );
        assert!(
            alice.properties().get("region").is_some(),
            "region should still be visible"
        );
    }
}
