use crate::test_support::setup_with_graphs;
use async_graphql::Request;
use raphtory::{
    db::api::view::MaterializedGraph,
    prelude::{AdditionOps, Graph, NO_PROPS},
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_algorithm_fast_rp() {
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
          fastRp(embeddingDim: 4, normalizationStrength: 1.0, iterWeights: [1.0, 1.0], seed: 42, threads: 1) {
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
    // The exact numbers cannot be pinned: fast_rp seeds each node's random
    // vector from the storage-internal node id, so the same graph and seed
    // yield different embeddings on different storage backends. This test
    // covers the resolver plumbing: every node comes back with a non-trivial
    // embedding of the requested dimension under the right column name.
    let data = res.data.into_json().unwrap();
    let result = &data["graph"]["algorithm"]["fastRp"];
    assert_eq!(result["columnNames"], json!(["embedding_state"]));
    let rows = result["rows"].as_array().unwrap();
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row["node"]["id"].as_str().unwrap());
        let entries = row["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["columnName"], "embedding_state");
        let embedding = entries[0]["value"]["prop"].as_array().unwrap();
        assert_eq!(embedding.len(), 4, "embeddingDim: 4 was requested");
        assert!(
            embedding.iter().any(|v| v.as_f64().unwrap() != 0.0),
            "embedding is all zeros for node {:?}",
            row["node"]["id"]
        );
    }
    ids.sort_unstable();
    assert_eq!(ids, ["a", "b", "c"]);
}
