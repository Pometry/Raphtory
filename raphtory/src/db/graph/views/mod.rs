pub mod cached_view;
pub mod deletion_graph;
pub mod filter;
pub mod layer_graph;
pub mod node_subgraph;
pub mod window_graph;

pub mod macros {
    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_results_w {
        ($search_fn:ident, $filter:expr, $window:expr, $expected_results:expr) => {{
            let search_results = $search_fn($filter.clone(), $window);
            assert_eq!($expected_results, search_results);
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_results_w {
        ($search_fn:ident, $filter:expr, $window:expr, $expected_results:expr) => {};
    }
}

pub mod macros_nodes {
    #[macro_export]
    macro_rules! assert_filter_nodes_results {
        // Default usage for all variants
        ($init_fn:ident, $filter:expr, $expected:expr) => {
            assert_filter_nodes_results!(
                $init_fn,
                $filter,
                $expected,
                variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
            );
        };

        // Custom variants
        ($init_fn:ident, $filter:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
            $(
                assert_filter_nodes_results_variant!($init_fn, $filter.clone(), $expected, $variant);
            )+
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_nodes_results_variant {
        ($init_fn:ident, $filter:expr, $expected:expr, graph) => {{
            let g = $init_fn(Graph::new());
            let result = filter_nodes_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_graph) => {{
            let g = $init_fn(PersistentGraph::new());
            let result = filter_nodes_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = filter_nodes_with($filter.clone(), dgs.into_graph());
                assert_eq!($expected, result);
            }
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = filter_nodes_with($filter.clone(), dgs.into_persistent_graph());
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results {
        // Default usage for all variants
        ($init_fn:ident, $filter:expr, $expected:expr) => {
            assert_search_nodes_results!(
                $init_fn,
                $filter,
                $expected,
                variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
            );
        };

        // With custom variants
        ($init_fn:ident, $filter:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
            $(
                assert_search_nodes_results_variant!($init_fn, $filter.clone(), $expected, $variant);
            )+
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results_variant {
        ($init_fn:ident, $filter:expr, $expected:expr, graph) => {{
            let g = $init_fn(Graph::new());
            let result = search_nodes_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_graph) => {{
            let g = $init_fn(PersistentGraph::new());
            let result = search_nodes_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = search_nodes_with($filter.clone(), dgs.into_graph());
                assert_eq!($expected, result);
            }
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = search_nodes_with($filter.clone(), dgs.into_persistent_graph());
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_nodes_results {
        ($init_fn:ident, $filter:expr, $expected_results:expr) => {};
    }
}

pub mod macros_nodes_w {
    #[macro_export]
    macro_rules! assert_filter_nodes_results_w {
        // Default case (graph + event_disk_graph)
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_filter_nodes_results_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [graph, event_disk_graph]
            );
        };

        // Custom variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_filter_nodes_results_w_variant!($init_fn, $filter.clone(), $w, $expected, $variant);
            )*
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_nodes_results_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, graph) => {{
            let result = filter_nodes_with(
                $filter.clone(),
                $init_fn(Graph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let windowed = dgs.into_graph().window($w.start, $w.end);
                let result = filter_nodes_with($filter.clone(), windowed);
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_nodes_results_pg_w {
        // Default to both variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_filter_nodes_results_pg_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [persistent_graph, persistent_disk_graph]
            );
        };

        // Variant-controlled
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_filter_nodes_results_pg_w_variant!(
                    $init_fn,
                    $filter.clone(),
                    $w,
                    $expected,
                    $variant
                );
            )*
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_nodes_results_pg_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_graph) => {{
            let result = filter_nodes_with(
                $filter.clone(),
                $init_fn(PersistentGraph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, result);
        }};

        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = filter_nodes_with(
                    $filter.clone(),
                    dgs.into_persistent_graph().window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results_w {
        // Default to both graph and event_disk_graph
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_search_nodes_results_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [graph, event_disk_graph]
            );
        };

        // With explicit variant list
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_search_nodes_results_w_variant!(
                    $init_fn,
                    $filter.clone(),
                    $w,
                    $expected,
                    $variant
                );
            )*
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, graph) => {{
            let result = search_nodes_with(
                $filter.clone(),
                $init_fn(Graph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result =
                    search_nodes_with($filter.clone(), dgs.into_graph().window($w.start, $w.end));
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_nodes_results_w {
        ($init_fn:ident, $filter:expr, $w:expr, $expected_results:expr) => {};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results_pg_w {
        // Default variant expansion
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_search_nodes_results_pg_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [persistent_graph, persistent_disk_graph]
            );
        };

        // Custom variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_search_nodes_results_pg_w_variant!(
                    $init_fn,
                    $filter.clone(),
                    $w,
                    $expected,
                    $variant
                );
            )*
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_nodes_results_pg_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_graph) => {{
            let result = search_nodes_with(
                $filter.clone(),
                $init_fn(PersistentGraph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, result);
        }};

        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let graph = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&graph, &tmp).unwrap();
                let result = search_nodes_with(
                    $filter.clone(),
                    dgs.into_persistent_graph().window($w.start, $w.end),
                );
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_nodes_results_pg_w {
        ($init_fn:ident, $filter:expr, $w:expr, $expected_results:expr) => {};
    }
}

pub mod macros_edges {
    #[macro_export]
    macro_rules! assert_filter_edges_results {
        // Default usage for all 4 variants
        ($init_fn:ident, $filter:expr, $expected:expr) => {
            assert_filter_edges_results!($init_fn, $filter, $expected, variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]);
        };
        // Explicit variants
        ($init_fn:ident, $filter:expr, $expected:expr, variants = [$($variant:ident),+ $(,)?]) => {{
            $(
                assert_filter_edges_results_variant!($init_fn, $filter.clone(), $expected, $variant);
            )+
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_edges_results_variant {
        ($init_fn:ident, $filter:expr, $expected:expr, graph) => {{
            let g = $init_fn(Graph::new());
            let result = filter_edges_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_graph) => {{
            let g = $init_fn(PersistentGraph::new());
            let result = filter_edges_with($filter.clone(), g);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();
                let result = filter_edges_with($filter.clone(), dgs.into_graph());
                assert_eq!($expected, result);
            }
        }};
        ($init_fn:ident, $filter:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &tmp).unwrap();

                // TODO: PropertyFilteringNotImplemented
                let result = filter_edges_with($filter.clone(), dgs.into_persistent_graph());
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_edges_results {
        // Default usage for all 4 variants
        ($init_fn:ident, $filter:expr, $expected_results:expr) => {
            assert_search_edges_results!(
                $init_fn,
                $filter,
                $expected_results,
                variants = [graph, persistent_graph, event_disk_graph, persistent_disk_graph]
            );
        };

        // Explicit variant usage
        ($init_fn:ident, $filter:expr, $expected_results:expr, variants = [$($variant:ident),*]) => {{
            $(
                assert_search_edges_variant!($init_fn, $filter.clone(), $expected_results, $variant);
            )*
        }};
    }

    #[macro_export]
    macro_rules! assert_search_edges_variant {
        ($init_fn:ident, $filter:expr, $expected_results:expr, graph) => {{
            let g = $init_fn(Graph::new());
            let results = search_edges_with($filter, g);
            assert_eq!($expected_results, results);
        }};

        ($init_fn:ident, $filter:expr, $expected_results:expr, persistent_graph) => {{
            let g = $init_fn(PersistentGraph::new());
            let results = search_edges_with($filter, g);
            assert_eq!($expected_results, results);
        }};

        ($init_fn:ident, $filter:expr, $expected_results:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let path = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &path).unwrap();
                let results = search_edges_with($filter, dgs.into_graph());
                assert_eq!($expected_results, results);
            }
        }};

        ($init_fn:ident, $filter:expr, $expected_results:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let path = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &path).unwrap();
                let results = search_edges_with($filter, dgs.into_persistent_graph());
                assert_eq!($expected_results, results);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_edges_results {
        ($init_fn:ident, $filter:expr, $expected_results:expr) => {};
    }
}

pub mod macros_edges_w {
    #[macro_export]
    macro_rules! assert_filter_edges_results_w {
        // Default variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_filter_edges_results_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [graph, event_disk_graph]
            );
        };

        // Custom variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_filter_edges_results_w_variant!(
                    $init_fn,
                    $filter.clone(),
                    $w,
                    $expected,
                    $variant
                );
            )*
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_edges_results_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, graph) => {{
            let result = filter_edges_with(
                $filter.clone(),
                $init_fn(Graph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, result);
        }};

        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, event_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let graph = $init_fn(Graph::new());
                let path = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&graph, &path).unwrap();

                let result =
                    filter_edges_with($filter.clone(), dgs.into_graph().window($w.start, $w.end));
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_edges_results_pg_w {
        // Default case (both persistent variants)
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_filter_edges_results_pg_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [persistent_graph, persistent_disk_graph]
            );
        };

        // Custom variants case
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_filter_edges_results_pg_w_variant!($init_fn, $filter.clone(), $w, $expected, $variant);
            )*
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_edges_results_pg_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_graph) => {{
            let graph = $init_fn(PersistentGraph::new()).window($w.start, $w.end);
            let result = filter_edges_with($filter.clone(), graph);
            assert_eq!($expected, result);
        }};
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let graph = $init_fn(Graph::new());
                let tmp = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&graph, &tmp).unwrap();
                let windowed = dgs.into_persistent_graph().window($w.start, $w.end);
                let result = filter_edges_with($filter.clone(), windowed);
                assert_eq!($expected, result);
            }
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_edges_results_w {
        ($init_fn:ident, $filter:expr, $w:expr, $expected_results:expr) => {{
            let filter_results = search_edges_with(
                $filter.clone(),
                $init_fn(Graph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected_results, filter_results);

            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let graph = $init_fn(Graph::new());
                let path = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&graph, &path).unwrap();

                let search_results = search_edges_with(
                    $filter.clone(),
                    dgs.clone().into_graph().window($w.start, $w.end),
                );
                assert_eq!($expected_results, search_results);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_edges_results_w {
        ($init_fn:ident, $filter:expr, $w:expr, $expected_results:expr) => {};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_edges_results_pg_w {
        // Default variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr) => {
            assert_search_edges_results_pg_w!(
                $init_fn,
                $filter,
                $w,
                $expected,
                variants = [persistent_graph, persistent_disk_graph]
            );
        };

        // Custom variants
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, variants = [$($variant:ident),* $(,)?]) => {{
            $(
                assert_search_edges_results_pg_w_variant!(
                    $init_fn,
                    $filter.clone(),
                    $w,
                    $expected,
                    $variant
                );
            )*
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_edges_results_pg_w_variant {
        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_graph) => {{
            let results = search_edges_with(
                $filter.clone(),
                $init_fn(PersistentGraph::new()).window($w.start, $w.end),
            );
            assert_eq!($expected, results);
        }};

        ($init_fn:ident, $filter:expr, $w:expr, $expected:expr, persistent_disk_graph) => {{
            #[cfg(feature = "storage")]
            {
                use crate::disk_graph::DiskGraphStorage;
                use tempfile::TempDir;

                let g = $init_fn(Graph::new());
                let path = TempDir::new().unwrap();
                let dgs = DiskGraphStorage::from_graph(&g, &path).unwrap();

                let results = search_edges_with(
                    $filter.clone(),
                    dgs.into_persistent_graph().window($w.start, $w.end),
                );
                assert_eq!($expected, results);
            }
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_edges_results_pg_w {
        ($init_fn:ident, $filter:expr, $w:expr, $expected_results:expr) => {};
    }
}

mod test_helpers {
    #[cfg(feature = "search")]
    pub use crate::db::api::view::SearchableGraphOps;
    use crate::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::{
                internal::{InternalEdgeFilterOps, InternalNodeFilterOps},
                model::{AsEdgeFilter, AsNodeFilter},
            },
        },
        prelude::{
            EdgePropertyFilterOps, EdgeViewOps, GraphViewOps, NodePropertyFilterOps, NodeViewOps,
        },
    };

    pub(crate) fn filter_nodes_with<G, I: InternalNodeFilterOps>(filter: I, graph: G) -> Vec<String>
    where
        G: StaticGraphViewOps,
    {
        let mut results = graph
            .filter_nodes(filter)
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    #[cfg(feature = "search")]
    pub(crate) fn search_nodes_with<G, I: AsNodeFilter>(filter: I, graph: G) -> Vec<String>
    where
        G: StaticGraphViewOps,
    {
        graph.create_index_in_ram().unwrap();

        let mut results = graph
            .search_nodes(filter, 20, 0)
            .unwrap()
            .into_iter()
            .map(|nv| nv.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    pub(crate) fn filter_edges_with<G, I: InternalEdgeFilterOps>(filter: I, graph: G) -> Vec<String>
    where
        G: StaticGraphViewOps,
    {
        let mut results = graph
            .filter_edges(filter)
            .unwrap()
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    #[cfg(feature = "search")]
    pub(crate) fn search_edges_with<G, I: AsEdgeFilter>(filter: I, graph: G) -> Vec<String>
    where
        G: StaticGraphViewOps,
    {
        graph.create_index_in_ram().unwrap();

        let mut results = graph
            .search_edges(filter, 20, 0)
            .unwrap()
            .into_iter()
            .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}
