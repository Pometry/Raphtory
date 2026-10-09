use raphtory::{
    db::{
        api::view::{
            filter_ops::{Filter, Select},
            internal::DynGraphArc,
            StaticGraphViewOps,
        },
        graph::views::{
            filter::{model::expr::FilterExpr, CreateFilter},
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{EdgeViewOps, Graph, GraphViewOps, NodeViewOps, TimeOps},
};
use raphtory_api::core::Direction;
use std::{ops::Range, sync::Arc};

pub enum TestGraphVariants {
    Graph,
    PersistentGraph,
}

impl From<TestGraphVariants> for Vec<TestGraphVariants> {
    fn from(val: TestGraphVariants) -> Self {
        vec![val]
    }
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

// The helpers below are called from hundreds of tests, each with its own graph and
// filter types. Erasing both (graph -> `DynGraphArc`, filter -> `FilterExpr`) means the
// filtered-graph view stack is compiled once here, not once per call site.
fn erase_graph<G: StaticGraphViewOps>(graph: G) -> DynGraphArc<'static> {
    Arc::new(graph)
}

pub trait ApplyFilter {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String>;
}

pub struct FilterNodes(FilterExpr);

impl ApplyFilter for FilterNodes {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String> {
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

pub struct SelectNodes(FilterExpr);

impl ApplyFilter for SelectNodes {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String> {
        let mut results = graph
            .nodes()
            .select(self.0.clone())
            .unwrap()
            .iter()
            .map(|n| n.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

pub struct FilterNeighbours(FilterExpr, String, Direction);

impl ApplyFilter for FilterNeighbours {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String> {
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

pub struct FilterEdges(FilterExpr);

impl ApplyFilter for FilterEdges {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String> {
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

pub struct SelectEdges(FilterExpr);

impl ApplyFilter for SelectEdges {
    fn apply(&self, graph: DynGraphArc<'static>) -> Vec<String> {
        let mut results = graph
            .edges()
            .select(self.0.clone())
            .unwrap()
            .into_iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }
}

#[track_caller]
pub fn assert_filter_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl Into<FilterExpr>,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterNodes(filter.into()),
    )
}

pub fn assert_select_nodes_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl Into<FilterExpr>,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        SelectNodes(filter.into()),
    )
}
#[track_caller]
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

#[track_caller]
pub fn assert_filter_nodes_err(
    init_graph: fn(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl Into<FilterExpr>,
    expected: &str,
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    let graph = init_graph(Graph::new());
    let variants = variants.into();
    let filter: FilterExpr = filter.into();

    for v in variants {
        match v {
            TestGraphVariants::Graph => {
                let graph = erase_graph(transform.apply(graph.clone()));
                let res = graph.filter(filter.clone());
                assert!(res.is_err(), "expected error, filter was accepted");
                assert_filter_err_contains(res.err().unwrap(), expected);
            }
            TestGraphVariants::PersistentGraph => {
                let base = graph.persistent_graph();
                let graph = erase_graph(transform.apply(base));
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
    filter: impl Into<FilterExpr>,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterNeighbours(filter.into(), node_name.as_ref().to_string(), direction),
    )
}

#[track_caller]
pub fn assert_filter_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl Into<FilterExpr>,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        FilterEdges(filter.into()),
    )
}

#[track_caller]
pub fn assert_select_edges_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    transform: impl GraphTransformer,
    filter: impl Into<FilterExpr>,
    expected: &[&str],
    variants: impl Into<Vec<TestGraphVariants>>,
) {
    assert_results(
        init_graph,
        |_graph: &Graph| (),
        transform,
        expected,
        variants.into(),
        SelectEdges(filter.into()),
    )
}

#[track_caller]
fn assert_results(
    init_graph: impl FnOnce(Graph) -> Graph,
    pre_transform: impl Fn(&Graph),
    transform: impl GraphTransformer,
    expected: &[&str],
    variants: Vec<TestGraphVariants>,
    apply: impl ApplyFilter,
) {
    let graph = init_graph(Graph::new());
    let expected = sorted(expected.iter());

    for v in variants {
        match v {
            TestGraphVariants::Graph => {
                pre_transform(&graph);
                let graph = erase_graph(transform.apply(graph.clone()));
                let result = sorted(apply.apply(graph));
                assert_eq!(expected, result);
            }
            TestGraphVariants::PersistentGraph => {
                pre_transform(&graph);
                let base = graph.persistent_graph();
                let graph = erase_graph(transform.apply(base));
                let result = sorted(apply.apply(graph));
                assert_eq!(expected, result);
            }
        }
    }
}

fn sorted<I, S>(iter: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut v: Vec<String> = iter.into_iter().map(|s| s.as_ref().to_string()).collect();
    v.sort();
    v
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

pub type EdgeRow = (u64, u64, i64, String, i64);

#[track_caller]
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

#[track_caller]
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
