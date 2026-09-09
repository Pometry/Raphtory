use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_all_simple_paths() {
    let graph = Graph::new();
    // simple chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();

    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `path` column holds Nodes, not a Prop
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              allSimplePaths(source: "a", target: "c", limit: 10) {
                list {
                    id
                 }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // row order is not guaranteed
    let mut data = res.data.into_json().unwrap();
    let paths: Vec<_> = data["graph"]["algorithm"]["allSimplePaths"]
        .as_array_mut()
        .unwrap()
        .into_iter()
        .map(|path| {
            path["list"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(paths, [vec!["a", "c"], vec!["a", "b", "c"]]);
}

#[tokio::test]
async fn test_algorithm_all_simple_paths_max_len() {
    let graph = Graph::new();
    // simple chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();

    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `path` column holds Nodes, not a Prop
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              allSimplePaths(source: "a", target: "c", limit: 10, maxLen: 1) {
                list {
                    id
                 }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // row order is not guaranteed
    let mut data = res.data.into_json().unwrap();
    let paths: Vec<_> = data["graph"]["algorithm"]["allSimplePaths"]
        .as_array_mut()
        .unwrap()
        .into_iter()
        .map(|path| {
            path["list"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(paths, [vec!["a", "c"]]);
}

#[tokio::test]
async fn test_algorithm_all_simple_paths_offset() {
    let graph = Graph::new();
    // simple chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();

    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `path` column holds Nodes, not a Prop
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              allSimplePaths(source: "a", target: "c", limit: 10, offset: 1) {
                list {
                    id
                 }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // row order is not guaranteed
    let mut data = res.data.into_json().unwrap();
    let paths: Vec<_> = data["graph"]["algorithm"]["allSimplePaths"]
        .as_array_mut()
        .unwrap()
        .into_iter()
        .map(|path| {
            path["list"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(paths, [vec!["a", "b", "c"]]);
}

#[tokio::test]
async fn test_algorithm_all_simple_paths_limit() {
    let graph = Graph::new();
    // simple chain a -> b -> c
    graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
    graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();

    let graph: MaterializedGraph = graph.into();
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

    // The `path` column holds Nodes, not a Prop
    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              allSimplePaths(source: "a", target: "c", limit: 1) {
                list {
                    id
                 }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // row order is not guaranteed
    let mut data = res.data.into_json().unwrap();
    let paths: Vec<_> = data["graph"]["algorithm"]["allSimplePaths"]
        .as_array_mut()
        .unwrap()
        .into_iter()
        .map(|path| {
            path["list"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node["id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(paths, [vec!["a", "c"]]);
}
