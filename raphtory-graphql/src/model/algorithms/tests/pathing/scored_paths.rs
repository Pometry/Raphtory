use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

/// A warm-introduction graph: two routes from `me` to `john`, one through an ex-partner.
fn intro_graph() -> MaterializedGraph {
    let graph = Graph::new();
    for name in ["me", "jenny", "james", "john"] {
        graph
            .add_node(0, name, NO_PROPS, Some("person"), None)
            .unwrap();
    }
    graph
        .add_edge(1, "me", "jenny", NO_PROPS, Some("friend"))
        .unwrap();
    graph
        .add_edge(1, "jenny", "john", NO_PROPS, Some("ex_partner"))
        .unwrap();
    graph
        .add_edge(1, "me", "james", [("years", 4i64)], Some("colleague"))
        .unwrap();
    graph
        .add_edge(1, "james", "john", NO_PROPS, Some("friend"))
        .unwrap();
    graph.into()
}

#[tokio::test]
async fn test_algorithm_top_scoring_paths() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", intro_graph())], tmp_dir.path()).await;

    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              topScoringPaths(
                destination: "john"
                sources: ["me"]
                maxHops: 2
                direction: OUT
                scoring: {
                  layers: [
                    { name: "friend", weight: 5.0 }
                    { name: "ex_partner", weight: -10.0 }
                    {
                      name: "colleague"
                      weight: 3.0
                      properties: [{ name: "years", scale: 0.5 }]
                    }
                  ]
                }
              ) {
                score
                hops
                layers
                nodes { id }
              }
            }
          }
        }
        "#;

    let res = setup.schema.execute(Request::new(query)).await;
    assert_eq!(res.errors, vec![], "{:?}", res.errors);
    // The route through the ex-partner still exists, it just ranks last.
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": {
                "algorithm": {
                    "topScoringPaths": [
                        {
                            "score": 10.0,
                            "hops": 2,
                            "layers": ["colleague", "friend"],
                            "nodes": [{ "id": "me" }, { "id": "james" }, { "id": "john" }]
                        },
                        {
                            "score": -5.0,
                            "hops": 2,
                            "layers": ["friend", "ex_partner"],
                            "nodes": [{ "id": "me" }, { "id": "jenny" }, { "id": "john" }]
                        }
                    ]
                }
            }
        })
    );
}

#[tokio::test]
async fn test_algorithm_top_scoring_paths_top_k() {
    let tmp_dir = tempdir().unwrap();
    let setup = setup_with_graphs(&[("g", intro_graph())], tmp_dir.path()).await;

    let query = r#"
        {
          graph(path: "g") {
            algorithm {
              topScoringPaths(
                destination: "john"
                maxHops: 2
                topK: 1
                direction: OUT
                scoring: {
                  layers: [
                    { name: "friend", weight: 5.0 }
                    { name: "ex_partner", weight: -10.0 }
                    { name: "colleague", weight: 3.0 }
                  ]
                }
              ) {
                score
                nodes { id }
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
            "graph": {
                "algorithm": {
                    "topScoringPaths": [{
                        "score": 8.0,
                        "nodes": [{ "id": "me" }, { "id": "james" }, { "id": "john" }]
                    }]
                }
            }
        })
    );
}
