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
        entities::{LayerId, ELID},
        storage::timeindex::{AsTime, EventTime},
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{
        edges::edge_storage_ops::EdgeStorageOps,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
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
    /// Nodes a view with no layer selected still shows — the unlayered ones, which live in
    /// `STATIC_GRAPH_LAYER_ID` and are visible in every view (see
    /// `NodeStorageOps::tprop_iter_layers`). `unique_layers` never yields the no-layer case, so
    /// this cannot fall out of the per-layer masks and is asked for separately.
    pub(crate) unlayered_nodes_mask: Arc<RoaringTreemap>,
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
        let mut layered_masks = vec![];
        // Derived by asking the graph rather than by reasoning about which nodes are unlayered, so
        // it follows the same rules as every other mask here.
        let unlayered_nodes_mask = Arc::new(
            graph
                .layers(Layer::None)
                .map(|no_layers| no_layers.nodes().iter().map(|n| n.node.as_u64()).collect())
                .unwrap_or_default(),
        );
        let global_nodes_mask = Arc::new(
            graph
                .nodes()
                .iter()
                .map(|node| node.node.as_u64())
                .collect(),
        );
        for l_name in graph.unique_layers() {
            let l_id = graph.get_layer_id(&l_name).unwrap().0;
            let layer_g = graph.layers(l_name).unwrap();

            let nodes = layer_g
                .nodes()
                .par_iter()
                .map(|node| node.node.as_u64())
                .collect::<Vec<_>>();

            let nodes: RoaringTreemap = nodes.into_iter().collect();

            let edges = layer_g.core_edges();

            let edges_chunks = edges
                .as_ref()
                .par_iter(&LayerIds::All)
                .filter(|edge| {
                    layer_g.filter_edge(edge.as_ref())
                        && nodes.contains(edge.src().as_u64())
                        && nodes.contains(edge.dst().as_u64())
                })
                .map(|edge| edge.eid().as_u64())
                .collect_vec_list();
            let edges_filter: RoaringTreemap = edges_chunks.into_iter().flatten().collect();

            let exploded_filter = if graph.internal_exploded_edge_filtered() {
                Some(
                    edges
                        .par_iter(&LayerIds::All)
                        .flat_map_iter(|e| {
                            edges_filter
                                .contains(e.eid().as_u64())
                                .then_some(e)
                                .into_iter()
                                .flat_map(|e| {
                                    let timesemantics = graph.edge_time_semantics();
                                    timesemantics
                                        .edge_exploded(e, &layer_g, layer_g.layer_ids())
                                        .map(|(t, _)| t.i() as u64)
                                })
                        })
                        .collect_vec_list()
                        .into_iter()
                        .flatten()
                        .collect(),
                )
            } else {
                None
            };

            if layered_masks.len() < l_id + 1 {
                layered_masks.resize(
                    l_id + 1,
                    (
                        RoaringTreemap::new(),
                        RoaringTreemap::new(),
                        Some(RoaringTreemap::new()),
                    ),
                );
            }

            layered_masks[l_id] = (nodes, edges_filter, exploded_filter);
        }

        Self {
            graph,
            global_nodes_mask,
            unlayered_nodes_mask,
            layered_mask: layered_masks.into(),
        }
    }
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
            // Not `false`, and not the global mask either: selecting no layers hides every
            // *layered* node but still shows the unlayered ones. Answering anything else makes
            // caching a view change what it contains, which is the one thing it must never do.
            LayerIds::None => self.unlayered_nodes_mask.contains(node.vid().as_u64()),
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

    /// Caching a view must not change what it contains — nodes or edges.
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

    /// Caching a view must not change what it contains, and selecting no layers is the case that
    /// separates "no nodes" from "the unlayered ones": layered nodes go, unlayered nodes stay.
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

    /// The same, but with the layers excluded *after* caching rather than before. The masks are
    /// then built over the whole graph, so an implementation that answered the no-layer case with
    /// its global mask would leak every layered node here while looking correct above.
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

    /// And the ordinary cases still agree, so the no-layer fix did not come at their expense.
    #[test]
    fn caching_agrees_on_all_layers_and_on_one() {
        let graph = fixture();
        assert_eq!(names(&graph.cache_view()), names(&graph));

        let one = graph.layers("layer_a").unwrap();
        assert_eq!(names(&one.cache_view()), names(&one));
        assert_eq!(one.cache_view().edges().len(), one.edges().len());
    }

    /// Edges across every shape of layer selection. Unlike nodes, edges have no "visible in every
    /// view" layer — an edge added without a layer name goes to the default layer, which is an
    /// ordinary one — so selecting no layers really does mean no edges.
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
}
