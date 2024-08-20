pub mod dataframe;
pub mod df_loaders;
mod prop_handler;

#[cfg(test)]
mod test {
    use crate::{
        io::arrow::{
            dataframe::{DFChunk, DFView},
            df_loaders::*,
        },
        prelude::*,
    };
    use polars_arrow::array::{PrimitiveArray, Utf8Array};
    use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};

    #[test]
    fn load_edges_from_pretend_df() {
        let df = DFView {
            names: vec!["src", "dst", "time", "prop1", "prop2"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: vec![
                Ok(DFChunk {
                    chunk: vec![
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(1)])),
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(2)])),
                        Box::new(PrimitiveArray::<i64>::from(vec![Some(1)])),
                        Box::new(PrimitiveArray::<f64>::from(vec![Some(1.0)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("a")])),
                    ],
                }),
                Ok(DFChunk {
                    chunk: vec![
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(2), Some(3)])),
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(3), Some(4)])),
                        Box::new(PrimitiveArray::<i64>::from(vec![Some(2), Some(3)])),
                        Box::new(PrimitiveArray::<f64>::from(vec![Some(2.0), Some(3.0)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("b"), Some("c")])),
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
            Some(&*vec!["prop1", "prop2"]),
            None,
            None,
            layer_name,
            layer_col,
            &graph,
        )
        .expect("failed to load edges from pretend df");

        let actual = graph
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
            names: vec!["id", "name", "time", "node_type"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            chunks: vec![
                Ok(DFChunk {
                    chunk: vec![
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(1)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("a")])),
                        Box::new(PrimitiveArray::<i64>::from(vec![Some(1)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("atype")])),
                    ],
                }),
                Ok(DFChunk {
                    chunk: vec![
                        Box::new(PrimitiveArray::<u64>::from(vec![Some(2)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("b")])),
                        Box::new(PrimitiveArray::<i64>::from(vec![Some(2)])),
                        Box::new(Utf8Array::<i32>::from(vec![Some("btype")])),
                    ],
                }),
            ]
            .into_iter(),
            num_rows: 2,
        };
        let graph = Graph::new();

        load_nodes_from_df(
            df,
            "id",
            "time",
            Some(&*vec!["name"]),
            None,
            None,
            Some("node_type"),
            None,
            &graph,
        )
        .expect("failed to load nodes from pretend df");

        let actual = graph
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
