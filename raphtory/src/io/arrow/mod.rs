pub mod dataframe;
pub mod df_loaders;
mod layer_col;
pub mod node_col;
pub mod prop_handler;

#[cfg(test)]
mod test {
    use crate::{
        io::arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::*,
        },
        prelude::*,
    };
    use arrow_array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
    use std::sync::Arc;

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
            num_rows: 3,
        };
        let graph = Graph::new();
        let layer_name: Option<&str> = None;
        let layer_col: Option<&str> = None;
        load_edges_from_df(
            df,
            "time",
            "src",
            "dst",
            &["prop1", "prop2"],
            &[],
            None,
            layer_name,
            layer_col,
            &graph,
        )
        .expect("failed to load edges from pretend df");

        let mut actual = graph
            .edges()
            .iter()
            .map(|e| {
                (
                    e.src().id(),
                    e.dst().id(),
                    e.latest_time(),
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
            num_rows: 2,
        };
        let graph = Graph::new();

        load_nodes_from_df(
            df,
            "time",
            "id",
            &["name"],
            &[],
            None,
            Some("node_type"),
            None,
            &graph,
        )
        .expect("failed to load nodes from pretend df");

        let mut actual = graph
            .nodes()
            .iter()
            .map(|v| {
                (
                    v.id(),
                    v.latest_time(),
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
}
