use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            view::internal::{
                Base, Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, NodeTypeFilter},
    },
    prelude::GraphViewOps,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct NodeTypeFilteredGraph<G> {
    pub(crate) graph: G,
    pub(crate) node_types_filter: Arc<[bool]>,
}

impl<G> Static for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeTypeFilteredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeTypeFilteredGraph<G> {
    pub fn new(graph: G, node_types_filter: Arc<[bool]>) -> Self {
        Self {
            graph,
            node_types_filter,
        }
    }
}

impl InternalNodeFilterOps for NodeTypeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeTypeFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .get_keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(NodeTypeFilteredGraph::new(graph, node_types_filter.into()))
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeTypeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeTypeFilteredGraph<G> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.node_types_filter
            .get(node.node_type_id())
            .copied()
            .unwrap_or(false)
            && self.graph.filter_node(node, layer_ids)
    }
}

#[cfg(test)]
mod tests_node_type_filtered_subgraph {
    use crate::{
        db::graph::views::filter::model::property_filter::{PropertyFilter, PropertyRef},
        prelude::*,
    };

    #[test]
    fn test_type_filtered_subgraph() {
        let graph = Graph::new();
        let edges = vec![
            (1, "A", "B", vec![("p1", 1u64)], None),
            (2, "B", "C", vec![("p1", 2u64)], None),
            (3, "C", "D", vec![("p1", 3u64)], None),
            (4, "D", "E", vec![("p1", 4u64)], None),
        ];

        for (id, src, dst, props, layer) in &edges {
            graph
                .add_edge(*id, src, dst, props.clone(), *layer)
                .unwrap();
        }

        let nodes = vec![
            (1, "A", vec![("p1", 1u64)], Some("water_tribe")),
            (2, "B", vec![("p1", 2u64)], Some("water_tribe")),
            (3, "C", vec![("p1", 1u64)], Some("fire_nation")),
            (4, "D", vec![("p1", 1u64)], Some("air_nomads")),
        ];

        for (id, name, props, layer) in &nodes {
            graph.add_node(*id, name, props.clone(), *layer).unwrap();
        }

        let type_filtered_subgraph = graph
            .subgraph_node_types(vec!["fire_nation", "air_nomads"])
            .window(1, 5);

        assert_eq!(type_filtered_subgraph.nodes(), vec!["C", "D"]);

        assert_eq!(
            type_filtered_subgraph
                .filter_nodes(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
                .unwrap()
                .nodes(),
            vec!["C", "D"]
        );

        assert!(type_filtered_subgraph
            .filter_edges(PropertyFilter::eq(PropertyRef::Property("p1".into()), 1u64))
            .unwrap()
            .edges()
            .is_empty())
    }

    mod test_filters_node_type_filtered_subgraph {
        use crate::{
            db::api::view::StaticGraphViewOps,
            prelude::{AdditionOps, NodeViewOps},
        };

        macro_rules! assert_filter_nodes_results {
            // Default usage with all variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {
                assert_filter_nodes_results!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // Custom variant list
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_filter_nodes_results_variant!($init_fn, $filter.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        macro_rules! assert_filter_nodes_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    filter_nodes_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    filter_nodes_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        filter_nodes_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        filter_nodes_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_nodes_results_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_nodes_results_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_nodes_results_w_variant!($init_fn, $filter.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        macro_rules! assert_filter_nodes_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_nodes_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_nodes_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_nodes_results_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_nodes_results_pg_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_nodes_results_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        macro_rules! assert_filter_nodes_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_nodes_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_nodes_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results {
            // Default usage for all variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {
                assert_search_nodes_results!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // With custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_search_nodes_results_variant!($init_fn, $filter.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    search_nodes_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    search_nodes_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        search_nodes_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        search_nodes_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_nodes_results_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_nodes_results_w_variant!($init_fn, $filter.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_nodes_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_nodes_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results_w {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_nodes_results_pg_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_nodes_results_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_nodes_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_nodes_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_nodes_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results_pg_w {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_nodes_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {};
        }

        macro_rules! assert_filter_edges_results {
            // Default usage with all variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {
                assert_filter_edges_results!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // Custom variant list
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_filter_edges_results_variant!($init_fn, $filter.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        macro_rules! assert_filter_edges_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    filter_edges_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    filter_edges_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        filter_edges_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        filter_edges_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_edges_results_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_edges_results_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_edges_results_w_variant!($init_fn, $filter.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        macro_rules! assert_filter_edges_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_edges_results_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_edges_results_pg_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_edges_results_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        macro_rules! assert_filter_edges_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results {
            // Default usage for all variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {
                assert_search_edges_results!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // With custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_search_edges_results_variant!($init_fn, $filter.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    search_edges_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result =
                    search_edges_with($filter.clone(), graph.subgraph_node_types(node_types));
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        search_edges_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result =
                        search_edges_with($filter.clone(), dgs.subgraph_node_types(node_types));
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, event_disk_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_edges_results_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_edges_results_w_variant!($init_fn, $filter.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_w {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_edges_results_pg_w!(
                    $init_fn,
                    $filter,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_edges_results_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_pg_w {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_pg_w_variant {
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {};
        }

        macro_rules! assert_filter_edges_results_layers {
            // Default usage with all variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr) => {
                assert_filter_edges_results_layers!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // Custom variant list
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_filter_edges_results_layers_variant!($init_fn, $filter.clone(), $layers.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        macro_rules! assert_filter_edges_results_layers_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap(),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap(),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).layers($layers).unwrap(),
                    );
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).layers($layers).unwrap(),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_edges_results_layers_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_edges_results_layers_w!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_edges_results_layers_w_variant!($init_fn, $filter.clone(), $layers.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        macro_rules! assert_filter_edges_results_layers_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap()
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types)
                            .layers($layers)
                            .unwrap()
                            .window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        macro_rules! assert_filter_edges_results_layers_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_filter_edges_results_layers_pg_w!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_filter_edges_results_layers_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $layers.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        macro_rules! assert_filter_edges_results_layers_pg_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = filter_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap()
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = filter_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types)
                            .layers($layers)
                            .unwrap()
                            .window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers {
            // Default usage for all variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr) => {
                assert_search_edges_results_layers!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $expected,
                    variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
                );
            };

            // With custom variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
                $(
                    assert_search_edges_results_layers_variant!($init_fn, $filter.clone(), $layers.clone(), $node_types.clone(), $expected, $variant);
                )+
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap(),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap(),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap().into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).layers($layers).unwrap(),
                    );
                    assert_eq!($expected, result);
                }
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp)
                        .unwrap()
                        .into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types).layers($layers).unwrap(),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, event_disk_graph) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $expected:expr, persistent_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers_w {
            // Default case (graph + event_disk_graph)
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_edges_results_layers_w!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $w,
                    $expected,
                    variants = [graph, event_disk_graph]
                );
            };

            // Custom variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_edges_results_layers_w_variant!($init_fn, $filter.clone(), $layers.clone(), $node_types.clone(), $w, $expected, $variant);
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {{
                let graph = $init_fn(Graph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap()
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types)
                            .layers($layers)
                            .unwrap()
                            .window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers_w {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, graph) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, event_disk_graph) => {};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers_pg_w {
            // Default to both variants
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {
                assert_search_edges_results_layers_pg_w!(
                    $init_fn,
                    $filter,
                    $layers,
                    $node_types,
                    $w,
                    $expected,
                    variants = [persistent_graph, persistent_disk_graph]
                );
            };

            // Variant-controlled
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
                $(
                    assert_search_edges_results_layers_pg_w_variant!(
                        $init_fn,
                        $filter.clone(),
                        $layers.clone(),
                        $node_types.clone(),
                        $w,
                        $expected,
                        $variant
                    );
                )*
            }};
        }

        #[cfg(feature = "search")]
        macro_rules! assert_search_edges_results_layers_pg_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {{
                let graph = $init_fn(PersistentGraph::new());
                let node_types: Vec<String> =
                    $node_types.unwrap_or_else(|| get_all_node_types(&graph));
                let result = search_edges_with(
                    $filter.clone(),
                    graph
                        .subgraph_node_types(node_types)
                        .layers($layers)
                        .unwrap()
                        .window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }};

            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
                #[cfg(feature = "storage")]
                {
                    use crate::disk_graph::DiskGraphStorage;
                    use tempfile::TempDir;

                    let g = $init_fn(Graph::new());
                    let tmp = TempDir::new().unwrap();
                    let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                    let dgs = dgs.into_persistent_graph();
                    let node_types: Vec<String> =
                        $node_types.unwrap_or_else(|| get_all_node_types(&dgs));
                    let result = search_edges_with(
                        $filter.clone(),
                        dgs.subgraph_node_types(node_types)
                            .layers($layers)
                            .unwrap()
                            .window($w.start, $w.end),
                    );
                    assert_eq!($expected, result);
                }
            }};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers_pg_w {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {};
        }

        #[cfg(not(feature = "search"))]
        macro_rules! assert_search_edges_results_layers_pg_w_variant {
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_graph) => {};
            ($init_fn:ident, $filter:expr, $layers:expr, $node_types:expr, $w:expr, $expected:expr, persistent_disk_graph) => {};
        }

        fn get_all_node_types<G: StaticGraphViewOps + AdditionOps>(graph: &G) -> Vec<String> {
            graph
                .nodes()
                .node_type()
                .into_iter()
                .flat_map(|(_, node_type)| node_type)
                .map(|s| s.to_string())
                .collect()
        }

        mod test_nodes_filters_node_type_filtered_subgraph {
            #[cfg(feature = "search")]
            use crate::db::graph::views::test_helpers::search_nodes_with;
            use crate::{
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph, filter::model::PropertyFilterOps,
                    },
                },
                prelude::{AdditionOps, Graph, GraphViewOps, TimeOps},
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let nodes = vec![
                    (6, "N1", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                    (7, "N1", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (6, "N2", vec![("p1", Prop::U64(1u64))], Some("water_tribe")),
                    (7, "N2", vec![("p1", Prop::U64(2u64))], Some("water_tribe")),
                    (8, "N3", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (9, "N4", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (5, "N5", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (6, "N5", vec![("p1", Prop::U64(2u64))], Some("air_nomad")),
                    (5, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (6, "N6", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (3, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (5, "N7", vec![("p1", Prop::U64(1u64))], Some("air_nomad")),
                    (3, "N8", vec![("p1", Prop::U64(1u64))], Some("fire_nation")),
                    (4, "N8", vec![("p1", Prop::U64(2u64))], Some("fire_nation")),
                ];

                // Add nodes to the graph
                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                graph
            }

            use crate::db::graph::views::test_helpers::filter_nodes_with;

            use crate::db::graph::views::filter::model::property_filter::PropertyFilter;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::get_all_node_types;

            #[test]
            fn test_nodes_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_nodes_results!(init_graph, filter, None, expected_results);
                assert_search_nodes_results!(init_graph, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N7"];
                assert_filter_nodes_results!(init_graph, filter, node_types, expected_results);
                assert_search_nodes_results!(init_graph, filter, node_types, expected_results);
            }

            #[test]
            fn test_nodes_filters_w() {
                // TODO: Enable event_disk_graph for filter_nodes once bug fixed: https://github.com/Pometry/Raphtory/issues/2098
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6"];
                assert_filter_nodes_results_w!(
                    init_graph,
                    filter,
                    None,
                    6..9,
                    expected_results,
                    variants = [graph]
                );
                assert_search_nodes_results_w!(init_graph, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3"];
                assert_filter_nodes_results_w!(
                    init_graph,
                    filter,
                    node_types,
                    6..9,
                    expected_results,
                    variants = [graph]
                );
                assert_search_nodes_results_w!(
                    init_graph,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
            }

            #[test]
            fn test_nodes_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N6", "N7"];
                assert_filter_nodes_results_pg_w!(init_graph, filter, None, 6..9, expected_results);
                assert_search_nodes_results_pg_w!(init_graph, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3", "N7"];
                assert_filter_nodes_results_pg_w!(
                    init_graph,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
                assert_search_nodes_results_pg_w!(
                    init_graph,
                    filter,
                    node_types,
                    6..9,
                    expected_results
                );
            }
        }

        mod test_edges_filters_node_type_filtered_subgraph {
            #[cfg(feature = "search")]
            use crate::db::graph::views::test_helpers::search_edges_with;
            use crate::{
                core::Prop,
                db::{
                    api::view::StaticGraphViewOps,
                    graph::views::{
                        deletion_graph::PersistentGraph, filter::model::PropertyFilterOps,
                    },
                },
                prelude::{AdditionOps, Graph, GraphViewOps, LayerOps, TimeOps, NO_PROPS},
            };

            fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G) -> G {
                let edges = vec![
                    (
                        6,
                        "N1",
                        "N2",
                        vec![("p1", Prop::U64(2u64))],
                        Some("fire_nation"),
                    ),
                    (7, "N1", "N2", vec![("p1", Prop::U64(1u64))], None),
                    (
                        6,
                        "N2",
                        "N3",
                        vec![("p1", Prop::U64(1u64))],
                        Some("water_tribe"),
                    ),
                    (
                        7,
                        "N2",
                        "N3",
                        vec![("p1", Prop::U64(2u64))],
                        Some("water_tribe"),
                    ),
                    (
                        8,
                        "N3",
                        "N4",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (9, "N4", "N5", vec![("p1", Prop::U64(1u64))], None),
                    (
                        5,
                        "N5",
                        "N6",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (6, "N5", "N6", vec![("p1", Prop::U64(2u64))], None),
                    (
                        5,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        6,
                        "N6",
                        "N7",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (
                        3,
                        "N7",
                        "N8",
                        vec![("p1", Prop::U64(1u64))],
                        Some("fire_nation"),
                    ),
                    (5, "N7", "N8", vec![("p1", Prop::U64(1u64))], None),
                    (
                        3,
                        "N8",
                        "N1",
                        vec![("p1", Prop::U64(1u64))],
                        Some("air_nomad"),
                    ),
                    (
                        4,
                        "N8",
                        "N1",
                        vec![("p1", Prop::U64(2u64))],
                        Some("water_tribe"),
                    ),
                ];

                for (id, src, dst, props, layer) in &edges {
                    graph
                        .add_edge(*id, src, dst, props.clone(), *layer)
                        .unwrap();
                }

                let nodes = vec![
                    (6, "N1", NO_PROPS, Some("air_nomad")),
                    (6, "N2", NO_PROPS, Some("water_tribe")),
                    (8, "N3", NO_PROPS, Some("air_nomad")),
                    (9, "N4", NO_PROPS, Some("air_nomad")),
                    (5, "N5", NO_PROPS, Some("air_nomad")),
                    (5, "N6", NO_PROPS, Some("fire_nation")),
                    (3, "N7", NO_PROPS, Some("air_nomad")),
                    (4, "N8", NO_PROPS, Some("fire_nation")),
                ];

                for (id, name, props, layer) in &nodes {
                    graph.add_node(*id, name, props.clone(), *layer).unwrap();
                }

                graph
            }

            use crate::{db::graph::views::test_helpers::filter_edges_with};
            use crate::db::graph::views::filter::model::property_filter::PropertyFilter;
            use crate::db::graph::views::filter::node_type_filtered_graph::tests_node_type_filtered_subgraph::test_filters_node_type_filtered_subgraph::get_all_node_types;

            #[test]
            fn test_edges_filters() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_edges_results!(
                    init_graph,
                    filter,
                    None,
                    expected_results,
                    variants = [graph, event_disk_graph]
                );
                assert_search_edges_results!(init_graph, filter, None, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5"];
                assert_filter_edges_results!(
                    init_graph,
                    filter,
                    node_types.clone(),
                    expected_results,
                    variants = [graph, event_disk_graph]
                );
                assert_search_edges_results!(
                    init_graph,
                    filter,
                    node_types.clone(),
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results_layers!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    expected_results,
                    variants = [graph, event_disk_graph]
                );
                assert_search_edges_results_layers!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    expected_results
                );
            }

            #[test]
            fn test_edges_filters_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7"];
                assert_filter_edges_results_w!(init_graph, filter, None, 6..9, expected_results);
                assert_search_edges_results_w!(init_graph, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_edges_results_w!(
                    init_graph,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );
                assert_search_edges_results_w!(
                    init_graph,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results_layers_w!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    6..9,
                    expected_results
                );
                assert_search_edges_results_layers_w!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    6..9,
                    expected_results
                );
            }

            #[test]
            fn test_edges_filters_pg_w() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N6->N7", "N7->N8"];
                assert_filter_edges_results_pg_w!(
                    init_graph,
                    filter,
                    None,
                    6..9,
                    expected_results,
                    variants = []
                );
                assert_search_edges_results_pg_w!(init_graph, filter, None, 6..9, expected_results);

                let node_types: Option<Vec<String>> =
                    Some(vec!["air_nomad".into(), "water_tribe".into()]);
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_edges_results_pg_w!(
                    init_graph,
                    filter,
                    node_types,
                    6..9,
                    expected_results,
                    variants = []
                );
                assert_search_edges_results_pg_w!(
                    init_graph,
                    filter,
                    node_types.clone(),
                    6..9,
                    expected_results
                );

                let layers = vec!["fire_nation"];
                let expected_results = vec!["N3->N4"];
                assert_filter_edges_results_layers_pg_w!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    6..9,
                    expected_results,
                    variants = []
                );
                assert_search_edges_results_layers_pg_w!(
                    init_graph,
                    filter,
                    layers,
                    node_types,
                    6..9,
                    expected_results
                );
            }
        }
    }
}
