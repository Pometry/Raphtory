pub mod dataframe;
pub mod loaders;
mod prop_handler;

#[cfg(test)]
mod test {
    use crate::{prelude::*, python::graph::pandas::load_vertices_from_df};

    use super::{load_edges_from_df, PretendDF};
    use arrow2::array::{PrimitiveArray, Utf8Array};
    use crate::python::graph::pandas::dataframe::PretendDF;
    use crate::python::graph::pandas::loaders::{load_edges_from_df, load_vertices_from_df};

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
            &graph,
        )
            .expect("failed to load edges from pretend df");

        let actual = graph
            .edges()
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
    fn load_vertices_from_pretend_df() {
        let df = PretendDF {
            names: vec!["id", "name", "time"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            arrays: vec![
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(1)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("a")])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(1)])),
                ],
                vec![
                    Box::new(PrimitiveArray::<u64>::from(vec![Some(2)])),
                    Box::new(Utf8Array::<i32>::from(vec![Some("b")])),
                    Box::new(PrimitiveArray::<i64>::from(vec![Some(2)])),
                ],
            ],
        };
        let graph = Graph::new();

        load_vertices_from_df(&df, 3, "id", "time", Some(vec!["name"]), None, None, &graph)
            .expect("failed to load vertices from pretend df");

        let actual = graph
            .vertices()
            .iter()
            .map(|v| {
                (
                    v.id(),
                    v.latest_time(),
                    v.properties()
                        .temporal()
                        .get("name")
                        .and_then(|v| v.latest()),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                (1, Some(1), Some(Prop::str("a"))),
                (2, Some(2), Some(Prop::str("b"))),
            ]
        );
    }
}