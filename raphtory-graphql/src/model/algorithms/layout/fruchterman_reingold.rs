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
    async fn test_algorithm_fruchterman_reingold() {
        let graph = Graph::new();
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              fruchtermanReingold(iterCount: 1) {
                columnNames
                nodes {
                  list { id }
                }
                columns {
                 name
                 values { ... on NodeStateProp { prop } }
                }
              }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // layout positions are non-deterministic (random init, no seed), so assert on shape:
        // two coordinate columns "0" (x) and "1" (y), each with one float per node.
        let data = res.data.into_json().unwrap();
        let fr = &data["graph"]["algorithm"]["fruchtermanReingold"];
        assert_eq!(fr["columnNames"], json!(["0", "1"]));
        assert_eq!(
            fr["nodes"]["list"],
            json!([{ "id": "a" }, { "id": "b" }, { "id": "c" }])
        );
        let columns = fr["columns"].as_array().unwrap();
        assert_eq!(columns.len(), 2);
        for column in columns {
            let values = column["values"].as_array().unwrap();
            assert_eq!(values.len(), 3);
            assert!(values.iter().all(|v| v["prop"].is_number()));
        }
    }
}
