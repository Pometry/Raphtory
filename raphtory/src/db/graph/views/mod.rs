pub mod cached_view;
pub mod deletion_graph;
pub mod filter;
pub mod layer_graph;
pub mod node_subgraph;
pub mod valid_graph;
pub mod window_graph;

pub mod macros {
    #[macro_export]
    macro_rules! assert_filter_results {
        ($filter_fn:ident, $filter:expr, $expected_results:expr) => {{
            let filter_results = $filter_fn($filter.clone());
            assert_eq!($expected_results, filter_results);
        }};
    }

    #[macro_export]
    macro_rules! assert_filter_results_w {
        ($filter_fn:ident, $filter:expr, $window:expr, $expected_results:expr) => {{
            let filter_results = $filter_fn($filter.clone(), $window);
            assert_eq!($expected_results, filter_results);
        }};
    }

    #[macro_export]
    #[cfg(feature = "search")]
    macro_rules! assert_search_results {
        ($search_fn:ident, $filter:expr, $expected_results:expr) => {{
            let search_results = $search_fn($filter.clone());
            assert_eq!($expected_results, search_results);
        }};
    }

    #[macro_export]
    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_results {
        ($search_fn:ident, $filter:expr, $expected_results:expr) => {};
    }

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

#[cfg(test)]
mod test_helpers {
    use crate::{
        db::{
            api::view::StaticGraphViewOps,
            graph::views::filter::internal::{CreateNodeFilter, InternalEdgeFilterOps},
        },
        prelude::{
            EdgePropertyFilterOps, EdgeViewOps, GraphViewOps, NodePropertyFilterOps, NodeViewOps,
        },
    };

    pub(crate) fn filter_nodes_with<G, I: CreateNodeFilter>(filter: I, graph: G) -> Vec<String>
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
    mod indexed_search {
        use crate::{
            db::{
                api::view::StaticGraphViewOps,
                graph::views::filter::model::{AsEdgeFilter, AsNodeFilter},
            },
            prelude::{EdgeViewOps, NodeViewOps, SearchableGraphOps},
        };

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
    }

    #[cfg(feature = "search")]
    pub use indexed_search::*;
}
