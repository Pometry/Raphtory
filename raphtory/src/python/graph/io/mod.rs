pub mod dataframe;
pub mod df_loaders;
pub mod panda_loaders;
pub mod parquet_loaders;
mod prop_handler;

#[cfg(test)]
mod test {
    use crate::{
        core::ArcStr,
        prelude::*,
        python::graph::io::{
            dataframe::PretendDF,
            df_loaders::{load_edges_from_df, load_nodes_from_df},
        },
    };
    use polars_arrow::array::{PrimitiveArray, Utf8Array};

    #[test]
    fn load_edges_from_pretend_df() {
        let df = PretendDF {
            names: vec!["src", "dst", "time", "prop1", "prop2"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            arrays: vec![
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(1)])),
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(2)])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(1)])),
                    Box::new(PrimitiveArray::<f64>::from(vec![Some(1.0)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("a")])),
                ],
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(2), Some(3)])),
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(3), Some(4)])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(2), Some(3)])),
                    Box::new(PrimitiveArray::<f64>::from(vec![Some(2.0), Some(3.0)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("b"), Some("c")])),
                ],
            ],
        };
        let graph = Graph::new();
        let layer: Option<&str> = None;
        let layer_in_df: bool = true;
        load_edges_from_df(
            &df,
            5,
            "src",
            "dst",
            "time",
            Some(vec!["prop1", "prop2"]),
            None,
            None,
            layer,
            layer_in_df,
            &graph.0,
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
                (1, 2, Some(1), Some(Prop::F64(1.0)), Some(Prop::str("a"))),
                (2, 3, Some(2), Some(Prop::F64(2.0)), Some(Prop::str("b"))),
                (3, 4, Some(3), Some(Prop::F64(3.0)), Some(Prop::str("c"))),
            ]
        );
    }

    #[test]
    fn load_nodes_from_pretend_df() {
        let df = PretendDF {
            names: vec!["id", "name", "time", "node_type"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            arrays: vec![
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(1)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("a")])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(1)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("atype")])),
                ],
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(2)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("b")])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(2)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("btype")])),
                ],
            ],
        };
        let graph = Graph::new();

        load_nodes_from_df(
            &df,
            3,
            "id",
            "time",
            Some(vec!["name"]),
            None,
            None,
            Some("node_type"),
            false,
            &graph.0,
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
                    1,
                    Some(1),
                    Some(Prop::str("a")),
                    Some(ArcStr::from("node_type"))
                ),
                (
                    2,
                    Some(2),
                    Some(Prop::str("b")),
                    Some(ArcStr::from("node_type"))
                ),
            ]
        );
    }
}
