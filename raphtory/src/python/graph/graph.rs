//! Defines the `Graph` struct, which represents a raphtory graph in memory.
//!
//! This is the base class used to create a temporal graph, add vertices and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use crate::{
    core::utils::errors::GraphError,
    prelude::*,
    python::{
        graph::views::graph_view::PyGraphView,
        utils::{PyInputVertex, PyTime},
    },
};
use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray, Utf8Array},
    ffi,
    types::NativeType,
};
use pyo3::{
    create_exception, exceptions::PyException, ffi::Py_uintptr_t, prelude::*, types::PyDict,
};

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};

/// A temporal graph.
#[derive(Clone)]
#[pyclass(name="Graph", extends=PyGraphView)]
pub struct PyGraph {
    pub(crate) graph: Graph,
}

impl Debug for PyGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.graph)
    }
}

impl From<Graph> for PyGraph {
    fn from(value: Graph) -> Self {
        Self { graph: value }
    }
}

impl IntoPy<PyObject> for Graph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, (PyGraph::from(self.clone()), PyGraphView::from(self)))
            .unwrap() // I think this only fails if we are out of memory? Seems to be unavoidable if we want to create an actual graph.
            .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for Graph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract()
    }
}

impl PyGraph {
    pub fn py_from_db_graph(db_graph: Graph) -> PyResult<Py<PyGraph>> {
        Python::with_gil(|py| {
            Py::new(
                py,
                (PyGraph::from(db_graph.clone()), PyGraphView::from(db_graph)),
            )
        })
    }
}

/// A temporal graph.
#[pymethods]
impl PyGraph {
    #[new]
    pub fn py_new() -> (Self, PyGraphView) {
        let graph = Graph::new();
        (
            Self {
                graph: graph.clone(),
            },
            PyGraphView::from(graph),
        )
    }

    /// Adds a new vertex with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the vertex.
    ///    id (str or int): The id of the vertex.
    ///    properties (dict): The properties of the vertex.
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, id, properties=None))]
    pub fn add_vertex(
        &self,
        timestamp: PyTime,
        id: PyInputVertex,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        self.graph
            .add_vertex(timestamp, id, properties.unwrap_or_default())
    }

    /// Adds properties to an existing vertex.
    ///
    /// Arguments:
    ///     id (str or int): The id of the vertex.
    ///     properties (dict): The properties of the vertex.
    ///
    /// Returns:
    ///    None
    pub fn add_vertex_properties(
        &self,
        id: PyInputVertex,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.add_vertex_properties(id, properties)
    }

    /// Adds properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the temporal property.
    ///    properties (dict): The temporal properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_property(
        &self,
        timestamp: PyTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        self.graph.add_properties(timestamp, properties)
    }

    /// Adds static properties to the graph.
    ///
    /// Arguments:
    ///     properties (dict): The static properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_static_property(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.graph.add_static_properties(properties)
    }

    /// Adds a new edge with the given source and destination vertices and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (int): The timestamp of the edge.
    ///    src (str or int): The id of the source vertex.
    ///    dst (str or int): The id of the destination vertex.
    ///    properties (dict): The properties of the edge, as a dict of string and properties
    ///    layer (str): The layer of the edge.
    ///
    /// Returns:
    ///   None
    #[pyo3(signature = (timestamp, src, dst, properties=None, layer=None))]
    pub fn add_edge(
        &self,
        timestamp: PyTime,
        src: PyInputVertex,
        dst: PyInputVertex,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.graph
            .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer)
    }

    /// Adds properties to an existing edge.
    ///
    /// Arguments:
    ///    src (str or int): The id of the source vertex.
    ///    dst (str or int): The id of the destination vertex.
    ///    properties (dict): The properties of the edge, as a dict of string and properties
    ///    layer (str): The layer of the edge.
    ///
    /// Returns:
    ///  None
    #[pyo3(signature = (src, dst, properties, layer=None))]
    pub fn add_edge_properties(
        &self,
        src: PyInputVertex,
        dst: PyInputVertex,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.graph.add_edge_properties(src, dst, properties, layer)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Loads a graph from the given path.
    ///
    /// Arguments:
    ///   path (str): The path to the graph.
    ///
    /// Returns:
    ///  Graph: The loaded graph.
    #[staticmethod]
    pub fn load_from_file(path: &str) -> Result<Graph, GraphError> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), path].iter().collect();
        Graph::load_from_file(file_path)
    }

    /// Saves the graph to the given path.
    ///
    /// Arguments:
    ///  path (str): The path to the graph.
    ///
    /// Returns:
    /// None
    pub fn save_to_file(&self, path: &str) -> Result<(), GraphError> {
        self.graph.save_to_file(Path::new(path))
    }

    #[staticmethod]
    #[pyo3(signature = (edges_df, src = "source", dst = "destination", time = "time", props = None, vertex_df = None, vertex_col = None, vertex_time_col = None, vertex_props = None))]
    fn load_from_pandas(
        edges_df: &PyAny,
        src: &str,
        dst: &str,
        time: &str,
        props: Option<Vec<&str>>,
        vertex_df: Option<&PyAny>,
        vertex_col: Option<&str>,
        vertex_time_col: Option<&str>,
        vertex_props: Option<Vec<&str>>,
    ) -> Result<Graph, GraphError> {
        let graph = PyGraph{graph: Graph::new()};
        graph.load_edges_from_pandas(edges_df, src, dst, time, props)?;
        if let (Some(vertex_df), Some(vertex_col), Some(vertex_time_col)) =
            (vertex_df, vertex_col, vertex_time_col)
        {
            graph.load_vertices_from_pandas(
                vertex_df,
                vertex_col,
                vertex_time_col,
                vertex_props,
            )?;
        }
        Ok(graph.graph)
    }


    #[pyo3(signature = (vertices_df, vertex_col = "id", time_col = "time", props = None))]
    fn load_vertices_from_pandas(&self, vertices_df: &PyAny, vertex_col: &str, time_col: &str, props: Option<Vec<&str>>) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let df = process_pandas_py_df(vertices_df, py)?;
            load_vertices_from_df(&df, vertex_col, time_col, props, graph)
                .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }

    #[pyo3(signature = (edge_df, src_col = "source", dst_col = "destination", time_col = "time", props = None))]
    fn load_edges_from_pandas(
        &self,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
        props: Option<Vec<&str>>,
    ) -> Result<(), GraphError> {
        let graph = &self.graph;
        Python::with_gil(|py| {
            let df = process_pandas_py_df(edge_df, py)?;
            load_edges_from_df(&df, src_col, dst_col, time_col, props, graph)
                .map_err(|e| GraphLoadException::new_err(format!("{:?}", e)))?;

            Ok::<(), PyErr>(())
        })
        .map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))?;
        Ok(())
    }

}

fn i64_opt_into_u64_opt(x: Option<&i64>) -> Option<u64> {
    x.map(|x| (*x).try_into().unwrap())
}

fn process_pandas_py_df<'a>(df: &PyAny, py: Python<'a>) -> PyResult<PretendDF> {
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

fn load_vertices_from_df<'a>(
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
    } else if let (Some(vertex_id), Some(time)) = (df.utf8(vertex_id), df.iter_col::<i64>(time)) {
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

fn load_edges_from_df<'a>(
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
    } else if let (Some(src), Some(dst), Some(time)) =
        (df.utf8(src), df.utf8(dst), df.iter_col::<i64>(time))
    {
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
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::F64(*v)))
                .collect::<Vec<_>>()
        }))
    } else if let Some(col) = df.iter_col::<i64>(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::I64(*v)))
                .collect::<Vec<_>>()
        }))
    } else if let Some(col) = df.bool(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::Bool(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Some(col) = df.utf8(name) {
        Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::str(v)))
                .collect::<Vec<_>>()
        }))
    } else {
        Box::new(std::iter::repeat(Vec::with_capacity(0)))
    }
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

struct PretendDF {
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

    fn utf8(&self, name: &str) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;
        // test that it's actually a utf8 array
        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()?;

        let iter = self
            .arrays
            .iter()
            .map(move |arr| {
                let arr = &arr[idx];
                let arr = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                arr.iter()
            })
            .flatten();

        Some(iter)
    }

    fn bool(&self, name: &str) -> Option<impl Iterator<Item = Option<bool>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;
        // test that it's actually a utf8 array
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
