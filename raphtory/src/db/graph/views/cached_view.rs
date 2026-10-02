use crate::{
    core::entities::LayerIds,
    db::api::{
        properties::internal::{
            InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
        },
        view::internal::{
            EdgeTimeSemanticsOps, FilterOps, GraphView, Immutable, InheritEdgeHistoryFilter,
            InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
            InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
            InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, InternalLayerOps,
            InternalNodeFilterOps, Static,
        },
    },
    prelude::{GraphViewOps, Layer, LayerOps},
    storage::core_ops::InheritCoreGraphOps,
};
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, ELID},
        storage::timeindex::{AsTime, EventTime},
    },
    inherit::Base,
};
use raphtory_storage::graph::{
    edges::edge_storage_ops::EdgeStorageOps,
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use rayon::prelude::*;
use roaring::RoaringTreemap;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};
use storage::EdgeEntryRef;

#[derive(Clone)]
pub struct CachedView<G> {
    pub(crate) graph: G,
    pub(crate) global_nodes_mask: Arc<RoaringTreemap>,
    pub(crate) layered_mask: Arc<[(RoaringTreemap, RoaringTreemap, Option<RoaringTreemap>)]>,
}

impl<G> Static for CachedView<G> {}

impl<G: Debug> Debug for CachedView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedView")
            .field("graph", &self.graph)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for CachedView<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for CachedView<G> {}
impl<'graph, G: GraphView> InheritTimeSemantics for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for CachedView<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> CachedView<G> {
    pub fn new(graph: G) -> Self {
        // A view per layer slot. Slot 0 is the static layer, which `unique_layers` does not return.
        let mut layered_graphs = vec![graph.layers(Layer::None).ok()];
        for name in graph.unique_layers() {
            let id = graph.get_layer_id(&name).unwrap().0;
            if layered_graphs.len() <= id {
                layered_graphs.resize_with(id + 1, || None);
            }
            layered_graphs[id] = Some(graph.layers(name).unwrap());
        }
        let empty = || vec![RoaringTreemap::new(); layered_graphs.len()];

        // one pass over the nodes
        let storage = graph.core_graph().lock();
        let (global_nodes_mask, layered_node_masks) = graph
            .node_list()
            .nodes_par_iter(&storage)
            .fold(
                || (RoaringTreemap::new(), empty()),
                |(mut global, mut per_layer), vid| {
                    if let Some(node) = storage.try_core_node(vid) {
                        let (node, id) = (node.as_ref(), vid.as_u64());
                        if graph.filter_node(node) {
                            push(&mut global, id);
                        }
                        for (nodes, layer) in per_layer.iter_mut().zip(&layered_graphs) {
                            if layer.as_ref().is_some_and(|layer| layer.filter_node(node)) {
                                push(nodes, id);
                            }
                        }
                    }
                    (global, per_layer)
                },
            )
            .reduce(
                || (RoaringTreemap::new(), empty()),
                |(global, per_layer), (other_global, other_per_layer)| {
                    (global | other_global, union(per_layer, other_per_layer))
                },
            );

        // One pass over the edges. Its endpoints are checked against the node masks for that layer.
        let edges = graph.core_edges();
        let exploded = graph.internal_exploded_edge_filtered();
        let (layered_edge_masks, exploded_ids) = edges
            .par_iter(&LayerIds::All)
            .fold(
                || (empty(), vec![Vec::new(); layered_graphs.len()]),
                |(mut masks, mut exploded_ids), edge| {
                    let (src, dst) = (edge.src().as_u64(), edge.dst().as_u64());
                    for id in edge.layer_ids_iter(&LayerIds::All).map(|id| id.0) {
                        let Some(Some(layer_g)) = layered_graphs.get(id) else {
                            // this layer is filtered out in this graph view
                            continue;
                        };
                        let nodes = &layered_node_masks[id];
                        if nodes.contains(src)
                            && nodes.contains(dst)
                            && layer_g.filter_edge_except_nodes(edge)
                        {
                            push(&mut masks[id], edge.eid().as_u64());
                            if exploded {
                                exploded_ids[id].extend(
                                    graph
                                        .edge_time_semantics()
                                        .edge_exploded(edge, layer_g, layer_g.layer_ids())
                                        .map(|(t, _)| t.i() as u64),
                                );
                            }
                        }
                    }
                    (masks, exploded_ids)
                },
            )
            .reduce(
                || (empty(), vec![Vec::new(); layered_graphs.len()]),
                |(masks, mut exploded_ids), (other_masks, other_exploded_ids)| {
                    for (ids, other) in exploded_ids.iter_mut().zip(other_exploded_ids) {
                        ids.extend(other);
                    }
                    (union(masks, other_masks), exploded_ids)
                },
            );

        let layered_mask = layered_node_masks
            .into_iter()
            .zip(layered_edge_masks)
            .zip(exploded_ids)
            .enumerate()
            .map(|(id, ((nodes, edges), exploded_ids))| {
                // the static layer and missing layers always have an exploded filter and it is empty
                let named = id != STATIC_GRAPH_LAYER_ID.0 && layered_graphs[id].is_some();
                let exploded_filter = (!named || exploded).then(|| sorted_id_mask(exploded_ids));
                (nodes, edges, exploded_filter)
            })
            .collect();

        Self {
            graph,
            global_nodes_mask: Arc::new(global_nodes_mask),
            layered_mask,
        }
    }
}

/// Add `id` to `mask`. Ids arrive in storage order, which is ascending, so this is almost always an
/// append rather than a search + insert, which is faster.
fn push(mask: &mut RoaringTreemap, id: u64) {
    if mask.try_push(id).is_err() {
        mask.insert(id);
    }
}

fn union(mut masks: Vec<RoaringTreemap>, others: Vec<RoaringTreemap>) -> Vec<RoaringTreemap> {
    for (mask, other) in masks.iter_mut().zip(others) {
        *mask |= other;
    }
    masks
}

/// Exploded edges are keyed by event id, which is not in storage order, so these are sorted first.
fn sorted_id_mask(mut ids: Vec<u64>) -> RoaringTreemap {
    ids.par_sort_unstable();
    ids.dedup();
    RoaringTreemap::from_sorted_iter(ids).expect("sorted and deduplicated")
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for CachedView<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalExplodedEdgeFilterOps for CachedView<G> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.graph.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.graph.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        self.layered_mask
            .get(eid.layer().0)
            .is_some_and(|(_, _, exploded_filter)| {
                exploded_filter
                    .as_ref()
                    .is_none_or(|filter| filter.contains(t.i() as u64))
            })
    }

    fn node_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_exploded_edge_filter(&self) -> bool {
        true
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for CachedView<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.graph.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.graph.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.layered_mask
            .get(layer.0)
            .is_some_and(|(_, edge_filter, _)| edge_filter.contains(edge.eid().as_u64()))
    }

    fn node_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn edge_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_edge_layer_filter(&self) -> bool {
        true
    }
}
impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for CachedView<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.graph.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.graph.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        let filter_fn =
            |(_, edges, _): &(RoaringTreemap, RoaringTreemap, Option<RoaringTreemap>)| {
                edges.contains(edge.eid().as_u64())
            };
        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => self.layered_mask.iter().any(filter_fn),
            LayerIds::One(id) => self.layered_mask.get(id.0).is_some_and(filter_fn),
            LayerIds::Multiple(multiple) => multiple
                .iter()
                .any(|id| self.layered_mask.get(id.0).is_some_and(filter_fn)),
        }
    }
    fn edge_filter_includes_window_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_edge_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_edge_filter(&self) -> bool {
        true
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        true
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalNodeFilterOps for CachedView<G> {
    fn internal_nodes_filtered(&self) -> bool {
        self.graph.internal_nodes_filtered()
    }
    fn internal_node_list_trusted(&self) -> bool {
        self.graph.internal_node_list_trusted()
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn edge_layer_filter_includes_node_filter(&self) -> bool {
        true
    }

    fn exploded_edge_filter_includes_node_filter(&self) -> bool {
        true
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        match layer_ids {
            // The unlayered nodes should still be returned when no layer is selected
            LayerIds::None => self
                .layered_mask
                .get(STATIC_GRAPH_LAYER_ID.0)
                .is_some_and(|(nodes, _, _)| nodes.contains(node.vid().as_u64())),
            LayerIds::All => self.global_nodes_mask.contains(node.vid().as_u64()),
            LayerIds::One(id) => self
                .layered_mask
                .get(id.0)
                .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                .unwrap_or(false),
            LayerIds::Multiple(multiple) => multiple.iter().any(|id| {
                self.layered_mask
                    .get(id.0)
                    .map(|(nodes, _, _)| nodes.contains(node.vid().as_u64()))
                    .unwrap_or(false)
            }),
        }
    }

    fn node_filter_includes_window_filter(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::db::graph::views::filter::model::{ExplodedEdgeFilter, PropertyFilterFactory};
    use crate::prelude::*;

    fn fixture() -> Graph {
        let graph = Graph::new();
        // Added without a layer, so it lives in the static layer and every view shows it.
        graph
            .add_node(0, "unlayered", NO_PROPS, None, None)
            .unwrap();
        graph
            .add_edge(0, "a", "b", NO_PROPS, Some("layer_a"))
            .unwrap();
        graph
            .add_edge(0, "c", "d", NO_PROPS, Some("layer_b"))
            .unwrap();
        // No layer name, so this one lands in the default layer — an ordinary layer, unlike the
        // static layer nodes get.
        graph.add_edge(0, "e", "f", NO_PROPS, None).unwrap();
        graph
    }

    fn names<'a, G: GraphViewOps<'a>>(graph: &G) -> Vec<String> {
        let mut names: Vec<String> = graph.nodes().name().into_iter().map(|(_, n)| n).collect();
        names.sort();
        names
    }

    /// Every edge as `src-dst@layer`, sorted — enough to catch an edge appearing in the wrong layer
    /// as well as one appearing at all.
    fn edges<'a, G: GraphViewOps<'a>>(graph: &G) -> Vec<String> {
        let mut edges: Vec<String> = graph
            .edges()
            .iter()
            .flat_map(|edge| {
                let (src, dst) = (edge.src().name(), edge.dst().name());
                edge.layer_names()
                    .into_iter()
                    .map(|layer| format!("{src}-{dst}@{layer}"))
                    .collect::<Vec<_>>()
            })
            .collect();
        edges.sort();
        edges
    }

    /// Caching a view must not change what it contains: nodes or edges.
    fn assert_caching_changes_nothing<'a, G: GraphViewOps<'a> + Clone>(view: &G, what: &str) {
        let cached = view.cache_view();
        assert_eq!(names(&cached), names(view), "nodes disagree for {what}");
        assert_eq!(edges(&cached), edges(view), "edges disagree for {what}");
        assert_eq!(
            cached.count_edges(),
            view.count_edges(),
            "edge count disagrees for {what}"
        );
    }

    /// Make sure a cache with a graph with no layers selected still returns unlayered nodes.
    #[test]
    fn caching_a_no_layer_view_keeps_exactly_its_unlayered_nodes() {
        let graph = fixture();
        let no_layers = graph
            .exclude_layers(["layer_a", "layer_b", "_default"])
            .unwrap();

        let direct = names(&no_layers);
        assert_eq!(
            direct,
            vec!["unlayered".to_string()],
            "the engine's own answer"
        );
        assert_eq!(names(&no_layers.cache_view()), direct);
        assert_eq!(
            no_layers.cache_view().edges().len(),
            no_layers.edges().len()
        );
    }

    /// The same, but with the layers excluded *after* caching rather than before.
    #[test]
    fn excluding_every_layer_after_caching_also_keeps_only_unlayered_nodes() {
        let graph = fixture();
        let cached = graph.cache_view();

        let excluded = ["layer_a", "layer_b", "_default"];
        let direct = names(&graph.exclude_layers(excluded).unwrap());
        let after = names(&cached.exclude_layers(excluded).unwrap());
        assert_eq!(
            after, direct,
            "caching must not change what excluding layers shows"
        );
    }

    /// Normal case.
    #[test]
    fn caching_agrees_on_all_layers_and_on_one() {
        let graph = fixture();
        assert_eq!(names(&graph.cache_view()), names(&graph));

        let one = graph.layers("layer_a").unwrap();
        assert_eq!(names(&one.cache_view()), names(&one));
        assert_eq!(one.cache_view().edges().len(), one.edges().len());
    }

    /// Edges across every shape of layer selection. Unlike nodes, edges have no "visible in every
    /// view" layer; an edge added without a layer name goes to the _default layer, which is an
    /// ordinary one. Selecting no layers really does mean no edges.
    #[test]
    fn caching_agrees_on_edges_for_every_layer_selection() {
        let graph = fixture();

        assert_caching_changes_nothing(&graph, "the whole graph");
        assert_caching_changes_nothing(&graph.layers("layer_a").unwrap(), "one named layer");
        assert_caching_changes_nothing(&graph.layers("_default").unwrap(), "the default layer");
        assert_caching_changes_nothing(
            &graph.layers(vec!["layer_a", "layer_b"]).unwrap(),
            "several named layers",
        );
        assert_caching_changes_nothing(
            &graph.exclude_layers("layer_a").unwrap(),
            "one layer excluded",
        );

        let none = graph
            .exclude_layers(vec!["layer_a", "layer_b", "_default"])
            .unwrap();
        assert!(edges(&none).is_empty(), "no layer selected means no edges");
        assert_caching_changes_nothing(&none, "every layer excluded");
    }

    /// The default layer is an ordinary layer, so excluding it hides its edges — the asymmetry with
    /// nodes, whose static layer survives every exclusion.
    #[test]
    fn the_default_layer_is_not_exempt_the_way_the_static_node_layer_is() {
        let graph = fixture();
        let without_default = graph.exclude_layers("_default").unwrap();

        assert!(
            !edges(&without_default)
                .iter()
                .any(|e| e.contains("@_default")),
            "excluding the default layer must hide its edges, got {:?}",
            edges(&without_default)
        );
        // The unlayered node is still there, which is the rule edges do not share.
        assert!(names(&without_default).contains(&"unlayered".to_string()));
        assert_caching_changes_nothing(&without_default, "the default layer excluded");
    }

    /// Node and edge properties for filters to cut on, and an edge that sits in two layers.
    fn filtered_fixture() -> Graph {
        let graph = Graph::new();
        for (name, x) in [
            ("a", 1i64),
            ("b", 2),
            ("c", 3),
            ("d", 1),
            ("e", 2),
            ("f", 5),
        ] {
            graph.add_node(0, name, [("x", x)], None, None).unwrap();
        }
        graph
            .add_node(0, "unlayered", [("x", 1i64)], None, None)
            .unwrap();
        graph
            .add_edge(0, "a", "b", [("w", 1i64)], Some("layer_a"))
            .unwrap();
        graph
            .add_edge(1, "a", "b", [("w", 5i64)], Some("layer_b"))
            .unwrap();
        graph
            .add_edge(0, "b", "d", [("w", 2i64)], Some("layer_b"))
            .unwrap();
        graph
            .add_edge(5, "b", "d", [("w", 7i64)], Some("layer_b"))
            .unwrap();
        graph
            .add_edge(2, "c", "f", [("w", 4i64)], Some("layer_a"))
            .unwrap();
        graph.add_edge(10, "e", "a", [("w", 3i64)], None).unwrap();
        graph
    }

    /// Caching must change nothing, whichever layers are selected before or after caching, and
    /// down to the exploded edges.
    fn assert_caching_changes_nothing_in_any_layer<'a, G: GraphViewOps<'a> + Clone>(
        view: &G,
        what: &str,
    ) {
        assert_caching_changes_nothing(view, what);
        let cached = view.cache_view();
        assert_eq!(
            cached.count_temporal_edges(),
            view.count_temporal_edges(),
            "exploded edges disagree for {what}"
        );
        for layer in ["layer_a", "layer_b", "_default"] {
            let direct = view.layers(layer).unwrap();
            let after = cached.layers(layer).unwrap();
            assert_eq!(names(&after), names(&direct), "nodes in {layer} for {what}");
            assert_eq!(edges(&after), edges(&direct), "edges in {layer} for {what}");
            assert_eq!(
                after.count_temporal_edges(),
                direct.count_temporal_edges(),
                "exploded edges in {layer} for {what}"
            );
            assert_caching_changes_nothing(&direct, &format!("{what}, only {layer}"));
        }
        assert_caching_changes_nothing(
            &view.exclude_layers("layer_a").unwrap(),
            &format!("{what}, without layer_a"),
        );
    }

    /// Each filter takes its own path through caching: a node filter is checked per node and then
    /// against both ends of an edge, a window per edge, an exploded filter per update.
    #[test]
    fn caching_agrees_with_node_window_and_exploded_filters() {
        let graph = filtered_fixture();
        let nodes = graph.filter(NodeFilter.property("x").lt(3i64)).unwrap();
        assert!(
            names(&nodes).len() < names(&graph).len(),
            "the node filter must hide something"
        );
        assert_caching_changes_nothing_in_any_layer(&nodes, "a node filter");
        assert_caching_changes_nothing_in_any_layer(&graph.window(0, 6), "a window");
        let exploded = graph
            .filter(ExplodedEdgeFilter.property("w").gt(2i64))
            .unwrap();
        assert!(
            exploded.count_temporal_edges() < graph.count_temporal_edges(),
            "the exploded filter must hide something"
        );
        assert_caching_changes_nothing_in_any_layer(&exploded, "an exploded edge filter");
        assert_caching_changes_nothing_in_any_layer(
            &nodes.window(0, 6),
            "a node filter inside a window",
        );
    }
}
