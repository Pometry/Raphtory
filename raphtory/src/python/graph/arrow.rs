use std::num::NonZeroUsize;

use arrow2::{
    array::StructArray,
    datatypes::{DataType, Field},
};
use itertools::Itertools;
/// A columnar temporal graph.
use pyo3::{prelude::*, types::IntoPyDict};

use crate::{
    arrow::{graph_impl::Graph2, Error},
    core::utils::errors::GraphError,
    db::api::view::{DynamicGraph, IntoDynamic},
    python::{
        graph::{graph::PyGraph, views::graph_view::PyGraphView},
        utils::errors::adapt_err_value,
    },
};

use super::pandas::dataframe::{process_pandas_py_df, PretendDF};

impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        adapt_err_value(&value)
    }
}

#[derive(Clone)]
#[pyclass(name="ArrowGraph", extends=PyGraphView)]
pub struct PyArrowGraph {
    pub graph: Graph2,
}

impl From<Graph2> for PyArrowGraph {
    fn from(value: Graph2) -> Self {
        Self { graph: value }
    }
}

impl From<PyArrowGraph> for Graph2 {
    fn from(value: PyArrowGraph) -> Self {
        value.graph
    }
}

impl From<PyArrowGraph> for DynamicGraph {
    fn from(value: PyArrowGraph) -> Self {
        value.graph.into_dynamic()
    }
}

impl IntoPy<PyObject> for Graph2 {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(
            py,
            (PyArrowGraph::from(self.clone()), PyGraphView::from(self)),
        )
        .unwrap()
        .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for Graph2 {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let py_graph: PyRef<PyArrowGraph> = ob.extract()?;
        Ok(py_graph.graph.clone())
    }
}

#[pymethods]
impl PyGraph {
    /// save graph in arrow format and memory map the result
    pub fn persist_as_arrow(&self, graph_dir: &str) -> Result<Graph2, Error> {
        self.graph.persist_as_arrow(graph_dir)
    }
}

#[pymethods]
impl PyArrowGraph {
    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, src_col, dst_col, time_col))]
    pub fn from_pandas(
        graph_dir: &str,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
    ) -> Result<Graph2, GraphError> {
        let graph: Result<Graph2, PyErr> = Python::with_gil(|py| {
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
            let graph = Self::from_arrow(graph_dir, df, src_col, dst_col, time_col)?;

            Ok::<_, PyErr>(graph)
        });

        graph.map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?}")))
    }

    #[staticmethod]
    fn open_path(graph_dir: &str) -> Result<Graph2, GraphError> {
        Graph2::open_path(graph_dir)
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }
}

impl PyArrowGraph {
    fn from_arrow(
        graph_dir: &str,
        df: PretendDF,
        src: &str,
        dst: &str,
        time: &str,
    ) -> Result<Graph2, GraphError> {
        let src_col_idx = df.names.iter().position(|x| x == src).unwrap();
        let dst_col_idx = df.names.iter().position(|x| x == dst).unwrap();
        let time_col_idx = df.names.iter().position(|x| x == time).unwrap();

        let num_threads =
            std::thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap());

        let chunk_size = df
            .arrays
            .first()
            .map(|arr| arr.len())
            .ok_or_else(|| GraphError::LoadFailure("empty pandas dataframe".to_owned()))?;

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

        Graph2::from_edge_lists(
            &edge_lists,
            num_threads,
            chunk_size,
            t_props_chunk_size,
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
        )
        .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }
}
