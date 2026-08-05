#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use async_graphql::Request;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_pagerank() {
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "c", "a", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                count
                nodes { list { name } }
                columns {
                  name
                  values {
                    __typename
                    ... on NodeStateProp { prop }
                  }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // in a 3-cycle all nodes have the same rank of 1/3
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "pagerank": {
                            "count": 3,
                            "nodes": {
                                "list": [
                                    { "name": "a" },
                                    { "name": "b" },
                                    { "name": "c" }
                                ]
                            },
                            "columns": [
                                {
                                    "name": "pagerank_score",
                                    "values": [
                                        { "__typename": "NodeStateProp", "prop": 0.3333333333333333 },
                                        { "__typename": "NodeStateProp", "prop": 0.3333333333333333 },
                                        { "__typename": "NodeStateProp", "prop": 0.3333333333333333 }
                                    ]
                                }
                            ]
                        }
                    }
                }
            })
        );
    }
}
