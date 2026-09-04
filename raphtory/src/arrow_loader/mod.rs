use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;

pub mod dataframe;
pub mod df_loaders;
mod layer_col;
pub mod node_col;
pub mod prop_handler;

#[cfg(test)]
mod test {
    use crate::{
        arrow_loader::{
            dataframe::{DFChunk, DFView},
            df_loaders::{
                edges::{load_edges_from_df_prefetch, ColumnNames},
                nodes::load_nodes_from_df,
            },
        },
        errors::{GraphError, LoadError},
        prelude::*,
    };
    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use itertools::Itertools;
    use raphtory_api::core::{
        entities::GID,
        storage::{arc_str::ArcStr, timeindex::AsTime},
    };
    use std::{sync::Arc, vec::IntoIter};

    #[test]
    fn load_edges_from_pretend_df() {
        let df = DFView {
            names: ["src", "dst", "time", "prop1", "prop2"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: vec![
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(UInt64Array::from(vec![1u64])),
                        Arc::new(UInt64Array::from(vec![2u64])),
                        Arc::new(Int64Array::from(vec![1i64])),
                        Arc::new(Float64Array::from(vec![1.0f64])),
                        Arc::new(StringArray::from(vec!["a"])),
                    ],
                }),
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(UInt64Array::from(vec![Some(2), Some(3)])),
                        Arc::new(UInt64Array::from(vec![Some(3), Some(4)])),
                        Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
                        Arc::new(Float64Array::from(vec![Some(2.0), Some(3.0)])),
                        Arc::new(StringArray::from(vec![Some("b"), Some("c")])),
                    ],
                }),
            ]
            .into_iter(),
            num_rows: Some(3),
        };
        let graph = Graph::new();
        let layer_name: Option<&str> = None;
        let layer_col: Option<&str> = None;
        let secondary_index: Option<&str> = None;

        load_edges_from_df_prefetch(
            df,
            ColumnNames::new("time", secondary_index, "src", "dst", layer_col),
            true,
            &["prop1", "prop2"],
            &[],
            None,
            layer_name,
            &graph,
            false,
        )
        .expect("failed to load edges from pretend df");

        let mut actual = graph
            .edges()
            .iter()
            .map(|e| {
                (
                    e.src().id(),
                    e.dst().id(),
                    e.latest_time().map(|t| t.t()),
                    e.properties()
                        .temporal()
                        .get("prop1")
                        .and_then(|v| v.latest()),
                    e.properties()
                        .temporal()
                        .get("prop2")
                        .and_then(|v| v.latest()),
                )
            })
            .collect::<Vec<_>>();

        actual.sort_by(|(l_src, l_dst, l_t, ..), (r_src, r_dst, r_t, ..)| {
            (l_src, l_dst, l_t).cmp(&(r_src, r_dst, r_t))
        });

        assert_eq!(
            actual,
            vec![
                (
                    GID::U64(1),
                    GID::U64(2),
                    Some(1),
                    Some(Prop::F64(1.0)),
                    Some(Prop::str("a"))
                ),
                (
                    GID::U64(2),
                    GID::U64(3),
                    Some(2),
                    Some(Prop::F64(2.0)),
                    Some(Prop::str("b"))
                ),
                (
                    GID::U64(3),
                    GID::U64(4),
                    Some(3),
                    Some(Prop::F64(3.0)),
                    Some(Prop::str("c"))
                ),
            ]
        );
    }

    #[test]
    fn load_nodes_from_pretend_df() {
        let df = DFView {
            names: ["id", "name", "time", "node_type"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: vec![
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(UInt64Array::from(vec![Some(1)])),
                        Arc::new(StringArray::from(vec![Some("a")])),
                        Arc::new(Int64Array::from(vec![Some(1)])),
                        Arc::new(StringArray::from(vec![Some("atype")])),
                    ],
                }),
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(UInt64Array::from(vec![Some(2)])),
                        Arc::new(StringArray::from(vec![Some("b")])),
                        Arc::new(Int64Array::from(vec![Some(2)])),
                        Arc::new(StringArray::from(vec![Some("btype")])),
                    ],
                }),
            ]
            .into_iter(),
            num_rows: Some(2),
        };
        let graph = Graph::new();
        let secondary_index: Option<&str> = None;

        load_nodes_from_df(
            df,
            "time",
            secondary_index,
            "id",
            &["name"],
            &[],
            None,
            Some("node_type"),
            None,
            &graph,
            true,
            None,
            None,
            None,
        )
        .expect("failed to load nodes from pretend df");

        let mut actual = graph
            .nodes()
            .iter()
            .map(|v| {
                (
                    v.id(),
                    v.latest_time().map(|t| t.t()),
                    v.properties()
                        .temporal()
                        .get("name")
                        .and_then(|v| v.latest()),
                    v.node_type(),
                )
            })
            .collect::<Vec<_>>();

        actual.sort_by(|(l_n, l_t, ..), (r_n, r_t, ..)| (l_n, l_t).cmp(&(r_n, r_t)));

        assert_eq!(
            actual,
            vec![
                (
                    GID::U64(1),
                    Some(1),
                    Some(Prop::str("a")),
                    Some(ArcStr::from("node_type"))
                ),
                (
                    GID::U64(2),
                    Some(2),
                    Some(Prop::str("b")),
                    Some(ArcStr::from("node_type"))
                ),
            ]
        );
    }

    /// Builds a node dataframe with an `id`/`node_type`/`time` column per chunk.
    fn node_type_df(
        chunks: Vec<(Vec<u64>, Vec<Option<&str>>, Vec<i64>)>,
    ) -> DFView<IntoIter<Result<DFChunk, GraphError>>> {
        let num_rows = chunks.iter().map(|(ids, ..)| ids.len()).sum();

        DFView {
            names: ["id", "node_type", "time"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: chunks
                .into_iter()
                .map(|(ids, node_types, times)| {
                    Ok(DFChunk {
                        chunk: vec![
                            Arc::new(UInt64Array::from(ids)),
                            Arc::new(StringArray::from(node_types)),
                            Arc::new(Int64Array::from(times)),
                        ],
                    })
                })
                .collect::<Vec<_>>()
                .into_iter(),
            num_rows: Some(num_rows),
        }
    }

    fn load_nodes_with_type_col(
        graph: &Graph,
        df: DFView<IntoIter<Result<DFChunk, GraphError>>>,
    ) -> Result<(), GraphError> {
        load_nodes_from_df(
            df,
            "time",
            None,
            "id",
            &[],
            &[],
            None,
            None,
            Some("node_type"),
            graph,
            true,
            None,
            None,
            None,
        )
    }

    fn node_types(graph: &Graph) -> Vec<(GID, Option<ArcStr>)> {
        graph
            .nodes()
            .iter()
            .map(|n| (n.id(), n.node_type()))
            .sorted_by_key(|(id, _)| id.to_str().to_string())
            .collect()
    }

    #[test]
    fn node_ids_repeated_across_chunks_keep_their_type() {
        let graph = Graph::new();

        load_nodes_with_type_col(
            &graph,
            node_type_df(vec![
                (vec![1, 2], vec![Some("a"), Some("b")], vec![1, 1]),
                (vec![1, 2], vec![Some("a"), Some("b")], vec![2, 2]),
            ]),
        )
        .expect("failed to load nodes");

        assert_eq!(
            node_types(&graph),
            vec![
                (GID::U64(1), Some(ArcStr::from("a"))),
                (GID::U64(2), Some(ArcStr::from("b"))),
            ]
        );

        // Both chunks' updates land even though only the first wrote the node id/type.
        let mut history = graph
            .nodes()
            .iter()
            .map(|n| {
                (
                    n.id(),
                    n.history().iter().map(|t| t.t()).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();

        history.sort();
        assert_eq!(
            history,
            vec![(GID::U64(1), vec![1, 2]), (GID::U64(2), vec![1, 2])]
        );
    }

    #[test]
    fn conflicting_node_type_within_a_chunk_is_rejected() {
        let graph = Graph::new();

        let err = load_nodes_with_type_col(
            &graph,
            node_type_df(vec![(vec![1, 1], vec![Some("a"), Some("b")], vec![1, 2])]),
        )
        .expect_err("expected a conflicting node type error");

        assert!(
            matches!(
                &err,
                GraphError::LoadError {
                    source: LoadError::ConflictingNodeType { gid, .. }
                } if gid == &GID::U64(1)
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn conflicting_node_type_across_chunks_is_rejected() {
        let graph = Graph::new();

        let err = load_nodes_with_type_col(
            &graph,
            node_type_df(vec![
                (vec![1], vec![Some("a")], vec![1]),
                (vec![1], vec![Some("b")], vec![2]),
            ]),
        )
        .expect_err("expected a conflicting node type error");

        assert!(
            matches!(
                &err,
                GraphError::LoadError {
                    source: LoadError::ConflictingNodeType { existing, new, .. }
                } if existing == "a" && new == "b"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn str_node_ids_are_cached_across_chunks() {
        let graph = Graph::new();

        let df = DFView {
            names: ["id", "node_type", "time"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: vec![
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(StringArray::from(vec!["t1", "t2"])),
                        Arc::new(Int64Array::from(vec![1i64, 1])),
                    ],
                }),
                Ok(DFChunk {
                    chunk: vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(StringArray::from(vec!["t1", "t2"])),
                        Arc::new(Int64Array::from(vec![2i64, 2])),
                    ],
                }),
            ]
            .into_iter(),
            num_rows: Some(4),
        };

        load_nodes_with_type_col(&graph, df).expect("failed to load str nodes");

        assert_eq!(
            node_types(&graph),
            vec![
                (GID::Str("a".to_string()), Some(ArcStr::from("t1"))),
                (GID::Str("b".to_string()), Some(ArcStr::from("t2"))),
            ]
        );
    }

    #[test]
    fn load_nodes_keeps_existing_type_already_on_graph() {
        let graph = Graph::new();
        graph
            .add_node(1, 1u64, NO_PROPS, Some("Person"), None)
            .expect("failed to add node");

        load_nodes_with_type_col(
            &graph,
            node_type_df(vec![(vec![1], vec![Some("Person")], vec![2])]),
        )
        .expect("reloading an existing typed node should succeed");

        assert_eq!(
            node_types(&graph),
            vec![(GID::U64(1), Some(ArcStr::from("Person")))]
        );
        assert_eq!(
            graph
                .node(1u64)
                .unwrap()
                .history()
                .iter()
                .map(|t| t.t())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn load_nodes_rejects_conflicting_type_already_on_graph() {
        let graph = Graph::new();
        graph
            .add_node(1, 1u64, NO_PROPS, Some("Person"), None)
            .expect("failed to add node");

        let err = load_nodes_with_type_col(
            &graph,
            node_type_df(vec![(vec![1], vec![Some("Company")], vec![2])]),
        )
        .expect_err("expected a conflicting node type error");

        assert!(
            matches!(
                &err,
                GraphError::LoadError {
                    source: LoadError::ConflictingNodeType { existing, new, .. }
                } if existing == "Person" && new == "Company"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn load_nodes_assigns_type_to_untyped_existing_node() {
        let graph = Graph::new();
        graph
            .add_edge(1, 1u64, 2u64, NO_PROPS, None)
            .expect("failed to add edge");

        assert_eq!(graph.node(1u64).unwrap().node_type(), None);

        load_nodes_with_type_col(
            &graph,
            node_type_df(vec![(vec![1], vec![Some("Person")], vec![2])]),
        )
        .expect("first type assignment on an untyped node should succeed");

        assert_eq!(
            graph.node(1u64).unwrap().node_type(),
            Some(ArcStr::from("Person"))
        );
    }
}

pub(crate) static LOAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|idx| format!("PS Bulk Load Thread-{idx}"))
        .build()
        .unwrap()
});
