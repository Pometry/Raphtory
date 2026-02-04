use crate::{
    db::api::view::{filter_ops::Filter, StaticGraphViewOps},
    prelude::{EdgeViewOps, Graph, GraphViewOps, NodeViewOps},
};
use std::ops::Range;

#[cfg(feature = "search")]
pub use crate::db::api::view::SearchableGraphOps;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::graph::views::{
        filter::{model::TryAsCompositeFilter, CreateFilter},
        window_graph::WindowedGraph,
    },
    errors::GraphError,
    prelude::TimeOps,
};
use raphtory_api::core::Direction;

pub enum TestGraphVariants {
    Graph,
    PersistentGraph,
}

pub enum TestVariants {
    All,
    EventOnly,
    PersistentOnly,
}

impl From<TestVariants> for Vec<TestGraphVariants> {
    fn from(variants: TestVariants) -> Self {
        use TestGraphVariants::*;
        match variants {
            TestVariants::All => {
                vec![Graph, PersistentGraph]
            }
            TestVariants::EventOnly => vec![Graph],
            TestVariants::PersistentOnly => vec![PersistentGraph],
        }
    }
}

pub trait GraphTransformer {
    type Return<G: StaticGraphViewOps>: StaticGraphViewOps;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G>;
}

pub struct WindowGraphTransformer(pub Range<i64>);

impl GraphTransformer for WindowGraphTransformer {
    type Return<G: StaticGraphViewOps> = WindowedGraph<G>;
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Self::Return<G> {
        graph.window(self.0.start, self.0.end)
    }
}

pub trait ApplyFilter {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String>;
}

pub struct FilterNodes<F: TryAsCompositeFilter + CreateFilter + Clone>(F);

impl<F: TryAsCompositeFilter + CreateFilter + Clone> ApplyFilter for FilterNodes<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        let mut results = graph
            .filter(self.0.clone())
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct FilterNeighbours<F: TryAsCompositeFilter + CreateFilter + Clone>(F, String, Direction);

impl<F: TryAsCompositeFilter + CreateFilter + Clone> ApplyFilter for FilterNeighbours<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        let filter_applied = graph
            .node(self.1.clone())
            .unwrap()
            .filter(self.0.clone())
            .unwrap();

        let mut results = match self.2 {
            Direction::OUT => filter_applied.out_neighbours(),
            Direction::IN => filter_applied.in_neighbours(),
            Direction::BOTH => filter_applied.neighbours(),
        }
        .iter()
        .map(|n| n.name())
        .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct SearchNodes<F: TryAsCompositeFilter + CreateFilter + Clone>(F);

impl<F: TryAsCompositeFilter + CreateFilter + Clone> ApplyFilter for SearchNodes<F> {
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

pub struct FilterEdges<F: TryAsCompositeFilter + CreateFilter + Clone>(F);

impl<F: TryAsCompositeFilter + CreateFilter + Clone> ApplyFilter for FilterEdges<F> {
    fn apply<G: StaticGraphViewOps>(&self, graph: G) -> Vec<String> {
        let mut results = graph
            .filter(self.0.clone())
            .unwrap()
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct SearchEdges<F: TryAsCompositeFilter + CreateFilter + Clone>(F);

impl<F: TryAsCompositeFilter + CreateFilter + Clone> ApplyFilter for SearchEdges<F> {
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

#[track_caller]
pub fn assert_filter_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
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

fn assert_filter_err_contains<E>(err: GraphError, expected: E)
where
    E: AsRef<str>,
{
    match err {
        GraphError::InvalidFilter(msg) => {
            assert!(
                msg.contains(expected.as_ref()),
                "unexpected InvalidFilter message.\nexpected to contain: {}\nactual: {}",
                expected.as_ref(),
                msg
            );
        }
        other => panic!("expected InvalidFilter, got: {other:?}"),
    }
}

pub fn assert_filter_nodes_err(
    init_graph: fn(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
    expected: &str,
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    let graph = init_graph(Graph::new());
    let variants = variants.into();

    for v in variants {
        match v {
            TestGraphVariants::Graph => {
                let graph = transform.apply(graph.clone());
                let res = graph.filter(filter.clone());
                assert!(res.is_err(), "expected error, filter was accepted");
                assert_filter_err_contains(res.err().unwrap(), expected);
            }
            TestGraphVariants::PersistentGraph => {
                let base = graph.persistent_graph();
                let graph = transform.apply(base);
                let res = graph.filter(filter.clone());
                assert!(res.is_err(), "expected error, filter was accepted");
                assert_filter_err_contains(res.err().unwrap(), expected);
            }
        }
    }
}

#[track_caller]
pub fn assert_filter_neighbours_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    node_name: impl AsRef<str>,
    direction: Direction,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterNeighbours(filter, node_name.as_ref().to_string(), direction),
    )
}

#[track_caller]
pub fn assert_search_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
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

#[track_caller]
pub fn assert_filter_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
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

#[track_caller]
pub fn assert_search_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl TryAsCompositeFilter + CreateFilter + Clone,
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

#[track_caller]
fn assert_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    pre_transform: impl Fn(&Graph) -> (),
    transform: impl GraphTransformer,
    expected: &[&str],
    variants: Vec<TestGraphVariants>,
    apply: impl ApplyFilter,
) {
    fn sorted<I, S>(iter: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut v: Vec<String> = iter.into_iter().map(|s| s.as_ref().to_string()).collect();
        v.sort();
        v
    }

    let graph = init_graph(Graph::new());

    let expected = sorted(expected.iter());

    for v in variants {
        match v {
            TestGraphVariants::Graph => {
                pre_transform(&graph);
                let graph = transform.apply(graph.clone());
                let result = sorted(apply.apply(graph));
                assert_eq!(expected, result);
            }
            TestGraphVariants::PersistentGraph => {
                pre_transform(&graph);
                let base = graph.persistent_graph();
                let graph = transform.apply(base);
                let result = sorted(apply.apply(graph));
                assert_eq!(expected, result);
            }
        }
    }
}

pub fn filter_nodes(graph: &Graph, filter: impl CreateFilter) -> Vec<String> {
    let mut results = graph
        .filter(filter)
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect::<Vec<_>>();
    results.sort();
    results
}

#[cfg(feature = "search")]
pub fn search_nodes(graph: &Graph, filter: impl TryAsCompositeFilter) -> Vec<String> {
    let mut results = graph
        .search_nodes(filter, 10, 0)
        .expect("Failed to search nodes")
        .into_iter()
        .map(|v| v.name())
        .collect::<Vec<_>>();
    results.sort();
    results
}

pub fn filter_edges(graph: &Graph, filter: impl CreateFilter) -> Vec<String> {
    let mut results = graph
        .filter(filter)
        .unwrap()
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect::<Vec<_>>();
    results.sort();
    results
}

#[cfg(feature = "search")]
pub fn search_edges(graph: &Graph, filter: impl TryAsCompositeFilter) -> Vec<String> {
    let mut results = graph
        .search_edges(filter, 10, 0)
        .expect("Failed to filter edges")
        .into_iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect::<Vec<_>>();
    results.sort();
    results
}

pub type EdgeRow = (u64, u64, i64, String, i64);

pub fn assert_ok_or_missing_edges<T>(
    edges: &[EdgeRow],
    res: Result<T, GraphError>,
    on_ok: impl FnOnce(T),
) {
    match res {
        Ok(v) => on_ok(v),
        Err(GraphError::PropertyMissingError(name)) => {
            assert!(
                edges.is_empty(),
                "PropertyMissingError({name}) on non-empty graph"
            );
        }
        Err(err) => panic!("Unexpected error from filter: {err:?}"),
    }
}

pub fn assert_ok_or_missing_nodes<T>(
    nodes: &[(u64, Option<String>, Option<i64>)],
    res: Result<T, GraphError>,
    on_ok: impl FnOnce(T),
) {
    match res {
        Ok(v) => on_ok(v),

        Err(GraphError::PropertyMissingError(name)) => {
            let property_really_missing = match name.as_str() {
                "int_prop" => nodes.iter().all(|(_, _, iv)| iv.is_none()),
                "str_prop" => nodes.iter().all(|(_, sv, _)| sv.is_none()),
                _ => panic!("Unexpected property {name}"),
            };

            assert!(
                property_really_missing,
                "PropertyMissingError({name}) but at least one node had that property"
            );
        }

        Err(err) => panic!("Unexpected error from filter: {err:?}"),
    }
}
