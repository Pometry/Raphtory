use std::{io::Write, sync::Arc};

use crate::{
    arrow::{
        graph_impl::{DiskGraph, ParquetLayerCols},
        query::{ast::Query, executors::rayon2, state::StaticGraphHopState, NodeSource},
        Error,
    },
    arrow2::{
        array::StructArray,
        datatypes::{ArrowDataType as DataType, Field},
    },
    core::{
        entities::{nodes::node_ref::NodeRef, VID},
        utils::errors::GraphError,
    },
    db::{
        api::view::{DynamicGraph, IntoDynamic},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, TimeOps},
    python::{
        graph::{edge::PyDirection, graph::PyGraph, views::graph_view::PyGraphView},
        types::repr::StructReprBuilder,
        utils::errors::adapt_err_value,
    },
};
use itertools::Itertools;
/// A columnar temporal graph.
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyDict, PyList, PyString},
};
use pometry_storage::GID;

use super::pandas::dataframe::{process_pandas_py_df, PretendDF};

impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        adapt_err_value(&value)
    }
}

#[derive(Clone)]
#[pyclass(name = "ArrowGraph", extends = PyGraphView)]
pub struct PyArrowGraph {
    pub graph: DiskGraph,
}

impl<G> AsRef<G> for PyArrowGraph
where
    DiskGraph: AsRef<G>,
{
    fn as_ref(&self) -> &G {
        self.graph.as_ref()
    }
}

impl From<DiskGraph> for PyArrowGraph {
    fn from(value: DiskGraph) -> Self {
        Self { graph: value }
    }
}

impl From<PyArrowGraph> for DiskGraph {
    fn from(value: PyArrowGraph) -> Self {
        value.graph
    }
}

impl From<PyArrowGraph> for DynamicGraph {
    fn from(value: PyArrowGraph) -> Self {
        value.graph.into_dynamic()
    }
}

impl IntoPy<PyObject> for DiskGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(
            py,
            (PyArrowGraph::from(self.clone()), PyGraphView::from(self)),
        )
        .unwrap()
        .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for DiskGraph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let py_graph: PyRef<PyArrowGraph> = ob.extract()?;
        Ok(py_graph.graph.clone())
    }
}

impl<'a> FromPyObject<'a> for ParquetLayerCols<'a> {
    fn extract(obj: &'a PyAny) -> PyResult<Self> {
        let dict = obj.downcast::<PyDict>()?;
        Ok(ParquetLayerCols {
            parquet_dir: dict
                .get_item("parquet_dir")
                .and_then(|item| {
                    item.expect("parquet_dir is required")
                        .extract::<&PyString>()
                })
                .and_then(|s| s.to_str())?,
            layer: dict
                .get_item("layer")
                .and_then(|item| item.expect("layer is required").extract::<&PyString>())
                .and_then(|s| s.to_str())?,
            src_col: dict
                .get_item("src_col")
                .and_then(|item| item.expect("src_col is required").extract::<&PyString>())
                .and_then(|s| s.to_str())?,
            dst_col: dict
                .get_item("dst_col")
                .and_then(|item| item.expect("dst_col is required").extract::<&PyString>())
                .and_then(|s| s.to_str())?,
            time_col: dict
                .get_item("time_col")
                .and_then(|item| item.expect("time_col is required").extract::<&PyString>())
                .and_then(|s| s.to_str())?,
        })
    }
}

pub struct ParquetLayerColsList<'a>(pub Vec<ParquetLayerCols<'a>>);

impl<'a> FromPyObject<'a> for ParquetLayerColsList<'a> {
    fn extract(obj: &'a PyAny) -> PyResult<Self> {
        let list = obj.downcast::<PyList>()?;
        let mut cols_list = Vec::new();

        for item in list.iter() {
            let cols = ParquetLayerCols::extract(item)?;
            cols_list.push(cols);
        }

        Ok(ParquetLayerColsList(cols_list))
    }
}

#[pymethods]
impl PyGraph {
    /// save graph in arrow format and memory map the result
    pub fn persist_as_arrow(&self, graph_dir: &str) -> Result<DiskGraph, Error> {
        self.graph.persist_as_arrow(graph_dir)
    }
}

#[pymethods]
impl PyArrowGraph {
    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, src_col, dst_col, time_col))]
    pub fn load_from_pandas(
        graph_dir: &str,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
    ) -> Result<DiskGraph, GraphError> {
        let graph: Result<DiskGraph, PyErr> = Python::with_gil(|py| {
            let size: usize = py
                .eval(
                    "index.__len__()",
                    Some([("index", edge_df.getattr("index")?)].into_py_dict(py)),
                    None,
                )?
                .extract()?;

            let cols_to_check = vec![src_col, dst_col, time_col];

            let df_columns: Vec<String> = edge_df.getattr("columns")?.extract()?;
            let df_columns: Vec<&str> = df_columns.iter().map(|x| x.as_str()).collect();

            let df = process_pandas_py_df(edge_df, py, size, df_columns)?;

            df.check_cols_exist(&cols_to_check)?;
            let graph = Self::from_pandas(graph_dir, df, src_col, dst_col, time_col)?;

            Ok::<_, PyErr>(graph)
        });

        graph.map_err(|e| {
            GraphError::LoadFailure(format!(
                "Failed to load graph {e:?} from pandas data frames"
            ))
        })
    }

    #[staticmethod]
    fn load_from_dir(graph_dir: &str) -> Result<DiskGraph, GraphError> {
        DiskGraph::load_from_dir(graph_dir).map_err(|err| {
            GraphError::LoadFailure(format!("Failed to load graph {err:?} from dir {graph_dir}"))
        })
    }

    #[staticmethod]
    #[pyo3(signature = (graph_dir, layer_parquet_cols, node_properties, chunk_size, t_props_chunk_size, read_chunk_size, concurrent_files, num_threads))]
    fn load_from_parquets(
        graph_dir: &str,
        layer_parquet_cols: ParquetLayerColsList,
        node_properties: Option<&str>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
    ) -> Result<DiskGraph, GraphError> {
        let graph = Self::from_parquets(
            graph_dir,
            layer_parquet_cols.0,
            node_properties,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
        );
        graph.map_err(|e| {
            GraphError::LoadFailure(format!("Failed to load graph {e:?} from parquet files"))
        })
    }

    fn __repr__(&self) -> String {
        StructReprBuilder::new("ArrowGraph")
            .add_field("number_of_nodes", self.graph.count_nodes())
            .add_field(
                "number_of_temporal_edges",
                self.graph.count_temporal_edges(),
            )
            .add_field("earliest_time", self.graph.earliest_time())
            .add_field("latest_time", self.graph.latest_time())
            .finish()
    }
}

impl PyArrowGraph {
    fn from_pandas(
        graph_dir: &str,
        df: PretendDF,
        src: &str,
        dst: &str,
        time: &str,
    ) -> Result<DiskGraph, GraphError> {
        let src_col_idx = df.names.iter().position(|x| x == src).unwrap();
        let dst_col_idx = df.names.iter().position(|x| x == dst).unwrap();
        let time_col_idx = df.names.iter().position(|x| x == time).unwrap();

        let chunk_size = df
            .arrays
            .first()
            .map(|arr| arr.len())
            .ok_or_else(|| GraphError::LoadFailure("Empty pandas dataframe".to_owned()))?;

        let t_props_chunk_size = chunk_size;

        let names = df.names.clone();

        let edge_lists = df
            .arrays
            .into_iter()
            .map(|arr| {
                let fields = arr
                    .iter()
                    .zip(names.iter())
                    .map(|(arr, col_name)| {
                        Field::new(col_name, arr.data_type().clone(), arr.null_count() > 0)
                    })
                    .collect_vec();
                let s_array = StructArray::new(DataType::Struct(fields), arr, None);
                s_array
            })
            .collect::<Vec<_>>();

        DiskGraph::load_from_edge_lists(
            &edge_lists,
            chunk_size,
            t_props_chunk_size,
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
        )
        .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }

    fn from_parquets(
        graph_dir: &str,
        layer_parquet_cols: Vec<ParquetLayerCols>,
        node_properties: Option<&str>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
    ) -> Result<DiskGraph, GraphError> {
        DiskGraph::load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            node_properties,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
        )
        .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[pyclass(name = "State")]
pub struct PyState(State);

#[pymethods]
impl PyState {
    #[staticmethod]
    fn count() -> Self {
        PyState(State::Count(0))
    }

    #[staticmethod]
    fn no_state() -> Self {
        PyState(State::NoState)
    }

    #[staticmethod]
    fn path() -> Self {
        PyState(State::Path(rpds::List::new_sync()))
    }

    #[staticmethod]
    fn path_window(keep_path: bool, start_t: Option<i64>, duration: Option<i64>) -> Self {
        PyState(State::PathWindow {
            start_t,
            duration,
            path: if keep_path {
                Some(rpds::List::new_sync())
            } else {
                None
            },
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum State {
    NoState,
    Count(usize),
    Path(rpds::ListSync<VID>),
    PathWindow {
        start_t: Option<i64>,
        duration: Option<i64>,
        path: Option<rpds::ListSync<VID>>,
    },
}

impl StaticGraphHopState for PyState {
    fn start<'a, G: GraphViewOps<'a>>(&self, node: &NodeView<&'a G>) -> Self {
        match self {
            PyState(State::NoState) => PyState(State::NoState),
            PyState(State::Count(_)) => PyState(State::Count(0)),
            PyState(State::Path(path)) => PyState(State::Path(path.push_front(node.node))),
            PyState(State::PathWindow {
                start_t,
                duration,
                path,
            }) => PyState(State::PathWindow {
                start_t: start_t
                    .or_else(|| node.earliest_time())
                    .map(|t| t.saturating_sub(1)),
                duration: *duration,
                path: path.as_ref().map(|path| path.push_front(node.node)),
            }),
        }
    }

    fn hop_with_state<'a, G: GraphViewOps<'a>>(
        &self,
        node: &NodeView<&'a G>,
        edge: &EdgeView<&'a G>,
    ) -> Option<Self> {
        match self {
            PyState(State::NoState) => Some(PyState(State::NoState)),
            PyState(State::Count(count)) => Some(PyState(State::Count(count + 1))),
            PyState(State::Path(path)) => Some(PyState(State::Path(path.push_front(node.node)))),
            PyState(State::PathWindow {
                start_t,
                duration,
                path,
            }) => {
                let s = start_t.as_ref()?.saturating_add(1);
                let duration = duration.as_ref()?;
                let w = s..(duration.saturating_add(s));
                let ts = edge.window(w.start, w.end);
                let earliest = ts.earliest_time()?;

                let new_path = path.as_ref().map(|path| path.push_front(node.node));
                Some(PyState(State::PathWindow {
                    start_t: Some(earliest),
                    duration: Some(*duration),
                    path: new_path,
                }))
            }
        }
    }
}
#[pyclass(name = "Query")]
pub struct PyGraphQuery {
    query: Query<PyState>,
    source: Arc<NodeSource>,
}

#[pymethods]
impl PyGraphQuery {
    #[new]
    pub fn new() -> Self {
        Self {
            query: Query::new(),
            source: Arc::new(NodeSource::All),
        }
    }

    #[staticmethod]
    pub fn from_node_ids(ids: Vec<NodeRef>) -> PyResult<Self> {
        let internal_nodes = ids.iter().all(|node| match node {
            NodeRef::Internal(_) => true,
            _ => false,
        });

        let external_nodes = ids.iter().all(|node| match node {
            NodeRef::External(_) | NodeRef::ExternalStr(_) => true,
            _ => false,
        });

        if !internal_nodes && !external_nodes {
            return Err(PyErr::new::<PyAny, _>(
                "All node ids must be either internal or external",
            ));
        }

        if internal_nodes {
            let internal = ids
                .into_iter()
                .filter_map(|id| match id {
                    NodeRef::Internal(id) => Some(id),
                    _ => None,
                })
                .collect::<Vec<_>>();

            Ok(Self {
                query: Query::new(),
                source: Arc::new(NodeSource::NodeIds(internal)),
            })
        } else {
            let external = ids
                .into_iter()
                .filter_map(|id| match id {
                    NodeRef::External(id) => Some(GID::I64(id as i64)),
                    NodeRef::ExternalStr(id) => Some(GID::Str(id.to_string())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok(Self {
                query: Query::new(),
                source: Arc::new(NodeSource::ExternalIds(external)),
            })
        }
    }

    pub fn out(&self, layer: Option<&str>) -> Self {
        Self {
            query: self.query.clone().out(layer.unwrap_or("_default")),
            source: self.source.clone(),
        }
    }

    pub fn into(&self, layer: Option<&str>) -> Self {
        Self {
            query: self.query.clone().into(layer.unwrap_or("_default")),
            source: self.source.clone(),
        }
    }

    pub fn var_hop(&self, dir: PyDirection, layer: Option<&str>, limit: Option<usize>) -> Self {
        Self {
            query: self
                .query
                .clone()
                .vhop(dir.into(), layer.unwrap_or("_default"), limit),
            source: self.source.clone(),
        }
    }

    pub fn hop(&self, dir: PyDirection, layer: Option<&str>, limit: Option<usize>) -> Self {
        Self {
            query: self
                .query
                .clone()
                .hop(dir.into(), layer.unwrap_or("_default"), false, limit),
            source: self.source.clone(),
        }
    }

    pub fn print(&self) -> Self {
        Self {
            query: self.query.clone().print(),
            source: self.source.clone(),
        }
    }

    pub fn void(&self) -> Self {
        Self {
            query: self.query.clone().void(),
            source: self.source.clone(),
        }
    }

    pub fn write_to(&self, dir: &str) -> Self {
        Self {
            query: self.query.clone().path(dir, |mut writer, state| {
                serde_json::to_writer(&mut writer, &state).unwrap();
                writer.write(b"\n").unwrap();
            }),

            source: self.source.clone(),
        }
    }

    pub fn run(&self, graph: DiskGraph, state: PyState) -> PyResult<()> {
        rayon2::execute_static_graph(
            self.query.clone(),
            self.source.as_ref().clone(),
            graph,
            state,
        )
        .map_err(|err| PyErr::new::<PyAny, _>(format!("Failed to run query: {err:?}")))?;

        Ok(())
    }

    pub fn run_to_vec(
        &self,
        graph: DiskGraph,
        state: PyState,
    ) -> PyResult<Vec<(Vec<NodeView<DiskGraph>>, NodeView<DiskGraph>)>> {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = self.query.clone().channel([sender]);

        rayon2::execute_static_graph(query, self.source.as_ref().clone(), graph.clone(), state)
            .map_err(|err| PyErr::new::<PyAny, _>(format!("Failed to run query: {err:?}")))?;

        Ok(receiver
            .into_iter()
            .map(|(state, node_id)| {
                let path = match state {
                    PyState(State::Path(path)) => path.into_iter().cloned().collect::<Vec<_>>(),
                    PyState(State::PathWindow { path, .. }) => path
                        .map(|path| path.into_iter().cloned().collect::<Vec<_>>())
                        .unwrap_or(Vec::with_capacity(0)),
                    _ => Vec::with_capacity(0),
                };

                let node_name = NodeView::new_internal(graph.clone(), node_id);
                (
                    path.into_iter()
                        .map(|node| NodeView::new_internal(graph.clone(), node))
                        .collect(),
                    node_name,
                )
            })
            .collect())
    }
}
