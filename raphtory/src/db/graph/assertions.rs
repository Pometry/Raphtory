use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::views::filter::{
            internal::{CreateEdgeFilter, CreateNodeFilter},
            model::{AsEdgeFilter, AsNodeFilter},
        },
    },
    prelude::{
        EdgePropertyFilterOps, EdgeViewOps, Graph, GraphViewOps, NodePropertyFilterOps, NodeViewOps,
    },
};

#[cfg(feature = "storage")]
use {
    crate::db::api::storage::graph::storage_ops::disk_storage::IntoGraph,
    raphtory_storage::disk::DiskGraphStorage, tempfile::TempDir,
};

#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;

#[cfg(feature = "search")]
pub use crate::db::api::view::SearchableGraphOps;

pub enum TestGraphVariants {
    Graph,
    PersistentGraph,
    EventDiskGraph,
    PersistentDiskGraph,
}

pub enum TestVariants {
    All,
    EventOnly,
    PersistentOnly,
    NonDiskOnly,
    DiskOnly,
}

impl From<TestVariants> for Vec<TestGraphVariants> {
    fn from(variants: TestVariants) -> Self {
        use TestGraphVariants::*;
        match variants {
            TestVariants::All => {
                vec![Graph, PersistentGraph, EventDiskGraph, PersistentDiskGraph]
            }
            TestVariants::EventOnly => vec![Graph, EventDiskGraph],
            TestVariants::PersistentOnly => vec![PersistentGraph, PersistentDiskGraph],
            TestVariants::NonDiskOnly => vec![Graph, PersistentGraph],
            TestVariants::DiskOnly => vec![EventDiskGraph, PersistentDiskGraph],
        }
    }
}

pub trait GraphTransformer {
    type Return<G: StaticGraphViewOps>: StaticGraphViewOps;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G>;
}

pub trait ApplyFilter {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String>;
}

pub struct FilterNodes<F: AsNodeFilter + CreateNodeFilter + Clone>(F);

impl<F: AsNodeFilter + CreateNodeFilter + Clone> ApplyFilter for FilterNodes<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        let mut results = graph
            .filter_nodes(self.0.clone())
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct SearchNodes<F: AsNodeFilter + CreateNodeFilter + Clone>(F);

impl<F: AsNodeFilter + CreateNodeFilter + Clone> ApplyFilter for SearchNodes<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        #[cfg(feature = "search")]
        {
            let mut results = graph
                .search_nodes(self.0.clone(), 20, 0)
                .unwrap()
                .into_iter()
                .map(|nv| nv.name())
                .collect::<Vec<_>>();
            results.sort();
            return results;
        }
        #[cfg(not(feature = "search"))]
        Vec::<String>::new()
    }
}

pub struct FilterEdges<F: AsEdgeFilter + CreateEdgeFilter + Clone>(F);

impl<F: AsEdgeFilter + CreateEdgeFilter + Clone> ApplyFilter for FilterEdges<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        let mut results = graph
            .filter_edges(self.0.clone())
            .unwrap()
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct SearchEdges<F: AsEdgeFilter + CreateEdgeFilter + Clone>(F);

impl<F: AsEdgeFilter + CreateEdgeFilter + Clone> ApplyFilter for SearchEdges<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        #[cfg(feature = "search")]
        {
            let mut results = graph
                .search_edges(self.0.clone(), 20, 0)
                .unwrap()
                .into_iter()
                .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
                .collect::<Vec<_>>();
            results.sort();
            return results;
        }
        #[cfg(not(feature = "search"))]
        Vec::<String>::new()
    }
}

pub fn assert_filter_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl AsNodeFilter + CreateNodeFilter + Clone,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterNodes(filter),
    )
}

pub fn assert_search_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl AsNodeFilter + CreateNodeFilter + Clone,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    #[cfg(feature = "search")]
    {
        assert_results(
            init_graph,
            |graph: &Graph| graph.create_index_in_ram().unwrap(),
            transform,
            expected,
            variants.into(),
            SearchNodes(filter),
        )
    }
}

pub fn assert_filter_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl AsEdgeFilter + CreateEdgeFilter + Clone,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterEdges(filter),
    )
}

pub fn assert_search_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl AsEdgeFilter + CreateEdgeFilter + Clone,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    #[cfg(feature = "search")]
    {
        assert_results(
            init_graph,
            |graph: &Graph| graph.create_index_in_ram().unwrap(),
            transform,
            expected,
            variants.into(),
            SearchEdges(filter),
        )
    }
}

fn assert_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    pre_transform: impl Fn(&Graph) -> (),
    transform: impl GraphTransformer,
    expected: &[&str],
    variants: Vec<TestGraphVariants>,
    apply: impl ApplyFilter,
) {
    let graph = init_graph(Graph::new());
    for v in variants {
        match v {
            TestGraphVariants::Graph => {
                pre_transform(&graph);
                let graph = transform.apply(graph.clone());
                let result = apply.apply(graph);
                assert_eq!(expected, result);
            }
            TestGraphVariants::PersistentGraph => {
                pre_transform(&graph);
                let base = graph.persistent_graph();
                let graph = transform.apply(base);
                let result = apply.apply(graph);
                assert_eq!(expected, result);
            }
            TestGraphVariants::EventDiskGraph => {
                #[cfg(feature = "storage")]
                {
                    let tmp = TempDir::new().unwrap();
                    let graph = graph.persist_as_disk_graph(tmp.path()).unwrap();
                    pre_transform(&graph);
                    let graph = transform.apply(graph);
                    let result = apply.apply(graph);
                    assert_eq!(expected, result);
                }
            }
            TestGraphVariants::PersistentDiskGraph => {
                #[cfg(feature = "storage")]
                {
                    let tmp = TempDir::new().unwrap();
                    let graph = DiskGraphStorage::from_graph(&graph, &tmp).unwrap();
                    let graph = graph.into_graph();
                    pre_transform(&graph);
                    let graph = graph.persistent_graph();
                    let graph = transform.apply(graph);
                    let result = apply.apply(graph);
                    assert_eq!(expected, result);
                }
            }
        }
    }
}

pub fn filter_nodes(graph: &Graph, filter: impl CreateNodeFilter) -> Vec<String> {
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
pub fn search_nodes(graph: &Graph, filter: impl AsNodeFilter) -> Vec<String> {
    let mut results = graph
        .search_nodes(filter, 10, 0)
        .expect("Failed to search nodes")
        .into_iter()
        .map(|v| v.name())
        .collect::<Vec<_>>();
    results.sort();
    results
}

pub fn filter_edges(graph: &Graph, filter: impl CreateEdgeFilter) -> Vec<String> {
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
pub fn search_edges(graph: &Graph, filter: impl AsEdgeFilter) -> Vec<String> {
    let mut results = graph
        .search_edges(filter, 10, 0)
        .expect("Failed to filter edges")
        .into_iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect::<Vec<_>>();
    results.sort();
    results
}
