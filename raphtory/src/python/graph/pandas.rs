use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    ffi,
    types::{NativeType, Offset},
};
use pyo3::{
    create_exception, exceptions::PyException, ffi::Py_uintptr_t, prelude::*, types::PyDict,
};

use crate::{core::utils::errors::GraphError, prelude::*};

fn i64_opt_into_u64_opt(x: Option<&i64>) -> Option<u64> {
    x.map(|x| (*x).try_into().unwrap())
}

pub(crate) fn process_pandas_py_df<'a>(df: &PyAny, py: Python<'a>) -> PyResult<PretendDF> {
    let globals = PyDict::new(py);

    globals.set_item("df", df)?;
    let locals = PyDict::new(py);
    py.run(
        r#"import pyarrow as pa; pa_table = pa.Table.from_pandas(df)"#,
        Some(globals),
        Some(locals),
    )?;

    if let Some(table) = locals.get_item("pa_table") {
        let rb = table.call_method0("to_batches")?.extract::<Vec<&PyAny>>()?;
        let names = if let Some(batch0) = rb.get(0) {
            let schema = batch0.getattr("schema")?;
            schema.getattr("names")?.extract::<Vec<String>>()?
        } else {
            vec![]
        };

        let arrays = rb
            .iter()
            .map(|rb| {
                (0..names.len())
                    .map(|i| {
                        let array = rb.call_method1("column", (i,))?;
                        let arr = array_to_rust(array)?;
                        Ok::<Box<dyn Array>, PyErr>(arr)
                    })
                    .collect::<Result<Vec<_>, PyErr>>()
            })
            .collect::<Result<Vec<_>, PyErr>>()?;

        let df = PretendDF { names, arrays };
        Ok(df)
    } else {
        return Err(GraphLoadException::new_err(
            "Failed to load graph, could not convert pandas dataframe to arrow table".to_string(),
        ));
    }
}

pub(crate) fn load_vertices_from_df<'a>(
    df: &'a PretendDF,
    vertex_id: &str,
    time: &str,
    props: Option<Vec<&str>>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let prop_iter = props
        .unwrap_or_default()
        .into_iter()
        .map(|name| lift_property(name, &df))
        .reduce(combine_prop_iters)
        .unwrap_or_else(|| Box::new(std::iter::repeat(vec![])));

    if let (Some(vertex_id), Some(time)) = (df.iter_col::<u64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.map(|i| i.copied()).zip(time);
        load_vertices_from_num_iter(graph, iter, prop_iter)?;
    } else if let (Some(vertex_id), Some(time)) =
        (df.iter_col::<i64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.map(i64_opt_into_u64_opt).zip(time);
        load_vertices_from_num_iter(graph, iter, prop_iter)?;
    } else if let (Some(vertex_id), Some(time)) =
        (df.utf8::<i32>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.into_iter().zip(time);
        for ((vertex_id, time), props) in iter.zip(prop_iter) {
            if let (Some(vertex_id), Some(time)) = (vertex_id, time) {
                graph.add_vertex(*time, vertex_id, props)?;
            }
        }
    } else if let (Some(vertex_id), Some(time)) =
        (df.utf8::<i64>(vertex_id), df.iter_col::<i64>(time))
    {
        let iter = vertex_id.into_iter().zip(time);
        for ((vertex_id, time), props) in iter.zip(prop_iter) {
            if let (Some(vertex_id), Some(time)) = (vertex_id, time) {
                graph.add_vertex(*time, vertex_id, props)?;
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "vertex id column must be either u64 or text, time column must be i64".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn load_edges_from_df<'a>(
    df: &'a PretendDF,
    src: &str,
    dst: &str,
    time: &str,
    props: Option<Vec<&str>>,
    graph: &Graph,
) -> Result<(), GraphError> {
    let prop_iter = props
        .unwrap_or_default()
        .into_iter()
        .map(|name| lift_property(name, &df))
        .reduce(combine_prop_iters)
        .unwrap_or_else(|| Box::new(std::iter::repeat(vec![])));

    if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<u64>(src),
        df.iter_col::<u64>(dst),
        df.iter_col::<i64>(time),
    ) {
        let triplets = src
            .map(|i| i.copied())
            .zip(dst.map(|i| i.copied()))
            .zip(time);
        load_edges_from_num_iter(&graph, triplets, prop_iter)?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.iter_col::<i64>(src),
        df.iter_col::<i64>(dst),
        df.iter_col::<i64>(time),
    ) {
        let triplets = src
            .map(i64_opt_into_u64_opt)
            .zip(dst.map(i64_opt_into_u64_opt))
            .zip(time);
        load_edges_from_num_iter(&graph, triplets, prop_iter)?;
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i32>(src),
        df.utf8::<i32>(dst),
        df.iter_col::<i64>(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
        for (((src, dst), time), props) in triplets.zip(prop_iter) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.add_edge(*time, src, dst, props, None)?;
            }
        }
    } else if let (Some(src), Some(dst), Some(time)) = (
        df.utf8::<i64>(src),
        df.utf8::<i64>(dst),
        df.iter_col::<i64>(time),
    ) {
        let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
        for (((src, dst), time), props) in triplets.zip(prop_iter) {
            if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                graph.add_edge(*time, src, dst, props, None)?;
            }
        }
    } else {
        return Err(GraphError::LoadFailure(
            "source and target columns must be either u64 or text, time column must be i64"
                .to_string(),
        ));
    }
    Ok(())
}

fn lift_property<'a: 'b, 'b>(
    name: &'a str,
    df: &'b PretendDF,
) -> Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> {
    if let Some(col) = df.iter_col::<f64>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.iter_col::<f32>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.iter_col::<i64>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.iter_col::<u64>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.iter_col::<u32>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.iter_col::<i32>(name) {
        iter_as_prop(name, col)
    } else if let Some(col) = df.bool(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::Bool(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Some(col) = df.utf8::<i32>(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::str(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Some(col) = df.utf8::<i64>(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::str(v)))
                .collect::<Vec<_>>()
        }))
    } else {
        Box::new(std::iter::repeat(Vec::with_capacity(0)))
    }
}

fn iter_as_prop<
    'a: 'b,
    'b,
    T: Into<Prop> + Copy + 'static,
    I: Iterator<Item = Option<&'b T>> + 'a,
>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Vec<(&str, Prop)>> + '_> {
    Box::new(is.map(move |val| {
        val.into_iter()
            .map(|v| (name, (*v).into()))
            .collect::<Vec<_>>()
    }))
}

fn combine_prop_iters<
    'a,
    I1: Iterator<Item = Vec<(&'a str, Prop)>> + 'a,
    I2: Iterator<Item = Vec<(&'a str, Prop)>> + 'a,
>(
    i1: I1,
    i2: I2,
) -> Box<dyn Iterator<Item = Vec<(&'a str, Prop)>> + 'a> {
    Box::new(i1.zip(i2).map(|(mut v1, v2)| {
        v1.extend(v2);
        v1
    }))
}

fn load_edges_from_num_iter<
    'a,
    S: AsRef<str>,
    I: Iterator<Item = ((Option<u64>, Option<u64>), Option<&'a i64>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
>(
    graph: &Graph,
    edges: I,
    props: PI,
) -> Result<(), GraphError> {
    for (((src, dst), time), edge_props) in edges.zip(props) {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            graph.add_edge(*time, src, dst, edge_props, None)?;
        }
    }
    Ok(())
}

fn load_vertices_from_num_iter<
    'a,
    S: AsRef<str>,
    I: Iterator<Item = (Option<u64>, Option<&'a i64>)>,
    PI: Iterator<Item = Vec<(S, Prop)>>,
>(
    graph: &Graph,
    vertices: I,
    props: PI,
) -> Result<(), GraphError> {
    for ((vertex, time), edge_props) in vertices.zip(props) {
        if let (Some(v), Some(t), props) = (vertex, time, edge_props) {
            graph.add_vertex(*t, v, props)?;
        }
    }
    Ok(())
}

pub(crate) struct PretendDF {
    names: Vec<String>,
    arrays: Vec<Vec<Box<dyn Array>>>,
}

impl PretendDF {
    fn iter_col<T: NativeType>(&self, name: &str) -> Option<impl Iterator<Item = Option<&T>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?;

        let iter = self
            .arrays
            .iter()
            .map(move |arr| {
                let arr = &arr[idx];
                let arr = arr.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                arr.iter()
            })
            .flatten();

        Some(iter)
    }

    fn utf8<O: Offset>(&self, name: &str) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;
        // test that it's actually a utf8 array
        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<Utf8Array<O>>()?;

        let iter = self
            .arrays
            .iter()
            .map(move |arr| {
                let arr = &arr[idx];
                let arr = arr.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
                arr.iter()
            })
            .flatten();

        Some(iter)
    }

    fn bool(&self, name: &str) -> Option<impl Iterator<Item = Option<bool>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<BooleanArray>()?;

        let iter = self
            .arrays
            .iter()
            .map(move |arr| {
                let arr = &arr[idx];
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                arr.iter()
            })
            .flatten();

        Some(iter)
    }
}

pub fn array_to_rust(obj: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref())
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;
        let array = ffi::import_array_from_c(*array, field.data_type)
            .map_err(|e| ArrowErrorException::new_err(format!("{:?}", e)))?;
        Ok(array)
    }
}

pub type ArrayRef = Box<dyn Array>;

create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, GraphLoadException, PyException);

#[cfg(test)]
mod test {
    use crate::{prelude::*, python::graph::pandas::load_vertices_from_df};

    use super::{load_edges_from_df, PretendDF};
    use arrow2::array::{PrimitiveArray, Utf8Array};

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

        load_edges_from_df(
            &df,
            "src",
            "dst",
            "time",
            Some(vec!["prop1", "prop2"]),
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
                    e.property("prop1", false),
                    e.property("prop2", false),
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

        load_vertices_from_df(&df, "id", "time", Some(vec!["name"]), &graph)
            .expect("failed to load vertices from pretend df");

        let actual = graph
            .vertices()
            .iter()
            .map(|v| {
                (
                    v.id(),
                    v.latest_time(),
                    v.property("name".to_owned(), false),
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
