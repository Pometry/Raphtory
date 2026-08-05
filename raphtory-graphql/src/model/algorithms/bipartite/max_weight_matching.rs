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
    async fn test_algorithm_max_weight_matching() {
        let graph = Graph::new();
        // path a-b-c-d: the max weight matching picks a-b (5) and c-d (4) over b-c (3)
        graph
            .add_edge(1, "a", "b", [("weight", 5.0)], None)
            .unwrap();
        graph
            .add_edge(1, "b", "c", [("weight", 3.0)], None)
            .unwrap();
        graph
            .add_edge(1, "c", "d", [("weight", 4.0)], None)
            .unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              maxWeightMatching(weightProp: "weight") {
                count
                edges { list { src { id } dst { id } } }
                dstOfA: dst(src: "a") { id }
                srcOfD: src(dst: "d") { id }
                hasAB: contains(src: "a", dst: "b")
                hasBC: contains(src: "b", dst: "c")
                edgeForA: edgeForSrc(src: "a") { src { id } dst { id } }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // the matching is backed by a HashMap, so edge order is not guaranteed
        let mut data = res.data.into_json().unwrap();
        data["graph"]["algorithm"]["maxWeightMatching"]["edges"]["list"]
            .as_array_mut()
            .unwrap()
            .sort_by_key(|edge| edge["src"]["id"].as_str().unwrap().to_string());
        // picks a-b and c-d (total weight 9) over the single b-c edge (weight 3)
        assert_eq!(
            data,
            json!({
                "graph": { "algorithm": { "maxWeightMatching": {
                    "count": 2,
                    "edges": { "list": [
                        { "src": { "id": "a" }, "dst": { "id": "b" } },
                        { "src": { "id": "c" }, "dst": { "id": "d" } }
                    ] },
                    "dstOfA": { "id": "b" },
                    "srcOfD": { "id": "c" },
                    "hasAB": true,
                    "hasBC": false,
                    "edgeForA": { "src": { "id": "a" }, "dst": { "id": "b" } }
                } } }
            })
        );
    }
}
