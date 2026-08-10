use crate::{graphql_test, test_support::setup_with_graphs};
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_out_component() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::single_component_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // out component of a: nodes reachable following out-edges, keyed by distance
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a") {
                rows {
                  node { id }
                  entries {
                    columnName
                    value { ... on NodeStateProp { prop } }
                  }
                }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // a reaches b (1), c (2), d (3); source itself is not included
    let entry = |id: &str, distance| {
        json!({
            "node": { "id": id },
            "entries": [{ "columnName": "distance", "value": { "prop": distance } }]
        })
    };
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "outComponent": { "rows": [
                entry("b", 1),
                entry("c", 2),
                entry("d", 3),
            ] } } }
        })
    );
}

#[tokio::test]
async fn test_algorithm_out_component_filtered() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::single_component_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // composite filter with a node filter removing c; a can then only reach b
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a", filter: { node: { node: { field: NODE_NAME, where: { ne: { str: "c" } } } } }) {
                nodes { list { id } }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // with c removed, a only reaches b (d is now unreachable)
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "outComponent": {
                "nodes": { "list": [{ "id": "b" }] }
            } } }
        })
    );
}

#[tokio::test]
async fn test_algorithm_out_component_node_filter_composed() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graphql_test::star_test_graph())], tmp_dir.path()).await;

    // NodeFilter and: both clauses must apply. Dropping b AND c leaves only d
    // in a's out component (dropping just one would leave two nodes).
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a", filter: { node: {
                and: [
                  { node: { field: NODE_NAME, where: { ne: { str: "b" } } } },
                  { node: { field: NODE_NAME, where: { ne: { str: "c" } } } }
                ]
              } }) {
                nodes { list { id } }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "outComponent": {
                "nodes": { "list": [{ "id": "d" }] }
            } } }
        })
    );
}

#[tokio::test]
async fn test_algorithm_out_component_edge_filter_composed() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graphql_test::star_test_graph())], tmp_dir.path()).await;

    // EdgeFilter and: both clauses must apply. Dropping edges to b AND to c
    // leaves only a -> d, so a reaches only d.
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a", filter: { edge: {
                and: [
                  { dst: { node: { field: NODE_NAME, where: { ne: { str: "b" } } } } },
                  { dst: { node: { field: NODE_NAME, where: { ne: { str: "c" } } } } }
                ]
              } }) {
                nodes { list { id } }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "outComponent": {
                "nodes": { "list": [{ "id": "d" }] }
            } } }
        })
    );
}

#[tokio::test]
async fn test_algorithm_out_component_graph_filter_composed() {
    let graph = Graph::new();
    // edges at increasing times so a graph-view window changes reachability
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    graph.add_edge(3, "c", "d", NO_PROPS, None).unwrap();
    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // GraphFilter composes via nested `expr`: window [1,3) then a further before(2).
    // Only the a -> b edge (t=1) remains, so a reaches only b.
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a", filter: { graph: {
                window: { start: 1, end: 3, expr: { before: { time: 2 } } }
              } }) {
                nodes { list { id } }
              }
            }
          }
        }
        "#;
    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "outComponent": {
                "nodes": { "list": [{ "id": "b" }] }
            } } }
        })
    );
}

#[tokio::test]
async fn test_algorithm_out_component_filter_equivalence() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(
        &[("g", graphql_test::single_component_test_graph())],
        tmp_dir.path(),
    )
    .await;

    // filter passed as an algorithm argument
    let as_argument = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponent(node: "a", filter: { node: {
                node: { field: NODE_NAME, where: { ne: { str: "c" } } }
              } }) {
                rows {
                  node { id }
                  entries {
                    columnName
                    value { ... on NodeStateProp { prop } }
                  }
                }
              }
            }
          }
        }
        "#;
    // same filter applied to the graph view before calling the algorithm (no argument)
    let pre_filtered = r#"
        {
          graph(path: "g") {
            filter(expr: { node: {
                node: { field: NODE_NAME, where: { ne: { str: "c" } } }} }) {
              algorithm {
                outComponent(node: "a") {
                  rows {
                    node { id }
                    entries {
                      columnName
                      value { ... on NodeStateProp { prop } }
                    }
                  }
                }
              }
            }
          }
        }
        "#;

    let arg_res = setup.schema.execute(Request::new(as_argument)).await;
    assert_eq!(arg_res.errors, vec![], "{:?}", arg_res.errors);
    let pre_res = setup.schema.execute(Request::new(pre_filtered)).await;
    assert_eq!(pre_res.errors, vec![], "{:?}", pre_res.errors);

    // both routes reach the same result (b -> unwrap the identical outComponent payload)
    let arg_out = arg_res.data.into_json().unwrap()["graph"]["algorithm"]["outComponent"].clone();
    let pre_out =
        pre_res.data.into_json().unwrap()["graph"]["filter"]["algorithm"]["outComponent"].clone();
    assert_eq!(arg_out, pre_out);
    assert_eq!(
        arg_out,
        json!({
            "rows": [{
                "node": { "id": "b" },
                "entries": [{ "columnName": "distance", "value": { "prop": 1 } }]
            }]
        })
    );
}
