use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_local_temporal_three_node_motifs() {
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
              localTemporalThreeNodeMotifs(delta: 10) {
                columnNames
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
    // each node gets a 40d motif-count vector; in this triangle each participates in motif 35
    let motif_counter = {
        let mut v = vec![0; 40];
        v[35] = 1;
        v
    };
    let row = |id: &str| {
        json!({
            "node": { "id": id },
            "entries": [{ "columnName": "motif_counter", "value": { "prop": motif_counter } }]
        })
    };
    assert_eq!(
        res.data.into_json().unwrap(),
        json!({
            "graph": { "algorithm": { "localTemporalThreeNodeMotifs": {
                "columnNames": ["motif_counter"],
                "rows": [row("a"), row("b"), row("c")]
            } } }
        })
    );
}
