#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use async_graphql::Request;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::{AdditionOps, Graph},
    };
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_balance() {
        let graph = Graph::new();
        graph
            .add_edge(1, "a", "b", [("weight", 5.0)], None)
            .unwrap();
        graph
            .add_edge(2, "c", "a", [("weight", 3.0)], None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              balance(name: "weight", direction: BOTH) {
                nodes { list { id } }
                columns { name values { ... on NodeStateProp { prop } } }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // BOTH: a = in 3 - out 5 = -2, b = +5, c = -3
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "balance": {
                    "nodes": { "list": [{ "id": "a" }, { "id": "b" }, { "id": "c" }] },
                    "columns": [{
                        "name": "balance",
                        "values": [{ "prop": -2.0 }, { "prop": 5.0 }, { "prop": -3.0 }]
                    }]
                } } }
            })
        );
    }
}
