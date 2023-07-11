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
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
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

    // Loads a graph from Polars DataFrame.
    // pub fn load_from_polars(df: PyDataFrame) -> Result<Graph, GraphError> {
    //     let graph = Graph::new();
    //     let rs_df = df.0;
    //     Ok(graph)
    // }

    #[staticmethod]
    #[pyo3(signature = (df, src = "source", dst = "destination", time = "time", props = None))]
    fn load_from_polars(
        df: PyDataFrame,
        src: &str,
        dst: &str,
        time: &str,
        props: Option<Vec<&str>>,
    ) -> Result<Graph, GraphError> {
        let graph = Graph::new();
        let rs_df = df.0.clone();
        let src = rs_df
            .column(src)
            .map_err(|_| GraphError::LoadFailure(format!("column: [{src}] not found")))?;

        let dst = rs_df
            .column(dst)
            .map_err(|_| GraphError::LoadFailure(format!("column: [{dst}] not found")))?;

        let time = rs_df
            .column(time)
            .map_err(|_| GraphError::LoadFailure(format!("column: [{time}] not found")))?;

        let prop_iter = props
            .unwrap_or_default()
            .into_iter()
            .map(|name| lift_property(name, &df))
            .reduce(combine_prop_iters)
            .unwrap_or_else(|| Box::new(std::iter::repeat(vec![])));

        if let (Ok(src), Ok(dst), Ok(time)) = (src.u64(), dst.u64(), time.i64()) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            load_from_num_iter(&graph, triplets, prop_iter)?;
        } else if let (Ok(src), Ok(dst), Ok(time)) = (src.i64(), dst.i64(), time.i64()) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            load_from_num_iter(&graph, triplets, prop_iter)?;
        } else if let (Ok(src), Ok(dst), Ok(time)) = (src.utf8(), dst.utf8(), time.i64()) {
            let triplets = src.into_iter().zip(dst.into_iter()).zip(time.into_iter());
            for ((src, dst), time) in triplets {
                if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
                    graph.add_edge(time, src, dst, NO_PROPS, None)?;
                }
            }
        } else {
            return Err(GraphError::LoadFailure(
                "source and target columns must be either u64 or text, time column must be i64"
                    .to_string(),
            ));
        }
        Ok(graph)
    }
}

fn lift_property<'a, 'b>(
    name: &'a str,
    df: &'b PyDataFrame,
) -> Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b> {
    let df = &df.0;
    let col = df.column(name).unwrap();
    let name = col.name();
    if let Ok(col) = col.f64() {
        Box::new(col.into_iter().map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::F64(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Ok(col) = col.i64() {
        Box::new(col.into_iter().map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::I64(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Ok(col) = col.bool() {
        Box::new(col.into_iter().map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::Bool(v)))
                .collect::<Vec<_>>()
        }))
    } else if let Ok(col) = col.utf8() {
        Box::new(col.into_iter().map(move |val| {
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

fn load_from_num_iter<
    'a,
    T: TryInto<u64>,
    I: Iterator<Item = ((Option<T>, Option<T>), Option<i64>)>,
    PI: Iterator<Item = Vec<(&'a str, Prop)>>,
>(
    graph: &Graph,
    edges: I,
    props: PI,
) -> Result<(), GraphError> {
    for (((src, dst), time), edge_props) in edges.zip(props) {
        if let (Some(src), Some(dst), Some(time)) = (src, dst, time) {
            let src: u64 = src.try_into().map_err(|_| {
                GraphError::LoadFailure("source column must be convertible to long".to_string())
            })?;
            let dst: u64 = dst.try_into().map_err(|_| {
                GraphError::LoadFailure("target column must be convertible to long".to_string())
            })?;

            graph.add_edge(time, src, dst, edge_props, None)?;
        }
    }
    Ok(())
}
