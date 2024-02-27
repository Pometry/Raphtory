use std::num::NonZeroUsize;

use arrow2::{
    array::StructArray,
    datatypes::{DataType, Field},
};
use itertools::Itertools;
/// A columnar temporal graph.
use pyo3::{prelude::*, types::IntoPyDict};

use crate::{
    arrow::graph_impl::Graph2,
    core::utils::errors::GraphError,
    db::api::view::{DynamicGraph, IntoDynamic},
    python::graph::views::graph_view::PyGraphView,
};

use super::pandas::dataframe::{process_pandas_py_df, PretendDF};

#[derive(Clone)]
#[pyclass(name = "ArrowGraph", extends = PyGraphView)]
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
impl PyArrowGraph {
    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, src_col, dst_col, time_col))]
    pub fn load_from_pandas(
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
            let graph = Self::from_df(graph_dir, df, src_col, dst_col, time_col)?;

            Ok::<_, PyErr>(graph)
        });

        graph.map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?} from pandas data frames")))
    }

    #[staticmethod]
    fn load_from_dir(graph_dir: &str) -> Result<Graph2, GraphError> {
        Graph2::open_path(graph_dir)
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?} from dir {graph_dir}")))
    }

    #[staticmethod]
    #[pyo3(signature = (graph_dir, parquet_dir, src_col, src_hash_col, dst_col, dst_hash_col, time_col))]
    fn load_from_parquets(
        graph_dir: &str,
        parquet_dir: &str,
        src_col: &str,
        src_hash_col: &str,
        dst_col: &str,
        dst_hash_col: &str,
        time_col: &str,
    ) -> Result<Graph2, GraphError> {
        let graph: Result<Graph2, PyErr> = Python::with_gil(|py| {
            let graph = Self::from_pf(graph_dir, parquet_dir, src_col, src_hash_col, dst_col, dst_hash_col, time_col)?;
            Ok::<_, PyErr>(graph)
        });
        graph.map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?} from parquet files at {parquet_dir}")))
    }
}

impl PyArrowGraph {
    fn from_df(
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

        let node_chunk_size = df
            .arrays
            .first()
            .map(|arr| arr.len())
            .ok_or_else(|| GraphError::LoadFailure("Empty pandas dataframe".to_owned()))?;

        let edge_chunk_size = node_chunk_size;
        let t_props_chunk_size = node_chunk_size * 100;

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
            node_chunk_size,
            edge_chunk_size,
            t_props_chunk_size,
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
        )
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }

    fn from_pf(
        graph_dir: &str,
        parquet_dir: &str,
        src_col: &str,
        src_hash_col: &str,
        dst_col: &str,
        dst_hash_col: &str,
        time_col: &str,
    ) -> Result<Graph2, GraphError> {
        let chunk_size = 268_435_456;
        let num_threads = 4;
        let t_props_chunk_size = chunk_size / 8;

        let read_chunk_size = Some(4_000_000);
        let concurrent_files = Some(1);

        Graph2::load_from_dir(
            graph_dir,
            parquet_dir,
            src_col,
            src_hash_col,
            dst_col,
            dst_hash_col,
            time_col,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
        )
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }
}
