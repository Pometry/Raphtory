use std::collections::HashMap;
use std::num::NonZeroUsize;

use arrow2::{
    array::StructArray,
    datatypes::{DataType, Field},
};
use itertools::Itertools;
/// A columnar temporal graph.
use pyo3::{exceptions::{PyTypeError, PyValueError}, prelude::*, types::{IntoPyDict, PyDict, PyList, PyString}};

use crate::{
    arrow::graph_impl::{ArrowGraph, ParquetLayerCols},
    core::utils::errors::GraphError,
    db::api::view::{DynamicGraph, IntoDynamic},
    python::graph::views::graph_view::PyGraphView,
};

use super::pandas::dataframe::{process_pandas_py_df, PretendDF};

#[derive(Clone)]
#[pyclass(name = "ArrowGraph", extends = PyGraphView)]
pub struct PyArrowGraph {
    pub graph: ArrowGraph,
}

impl From<ArrowGraph> for PyArrowGraph {
    fn from(value: ArrowGraph) -> Self {
        Self { graph: value }
    }
}

impl From<PyArrowGraph> for ArrowGraph {
    fn from(value: PyArrowGraph) -> Self {
        value.graph
    }
}

impl From<PyArrowGraph> for DynamicGraph {
    fn from(value: PyArrowGraph) -> Self {
        value.graph.into_dynamic()
    }
}

impl IntoPy<PyObject> for ArrowGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(
            py,
            (PyArrowGraph::from(self.clone()), PyGraphView::from(self)),
        )
            .unwrap()
            .into_py(py)
    }
}

impl<'source> FromPyObject<'source> for ArrowGraph {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let py_graph: PyRef<PyArrowGraph> = ob.extract()?;
        Ok(py_graph.graph.clone())
    }
}

impl<'a> FromPyObject<'a> for ParquetLayerCols<'a> {
    fn extract(obj: &'a PyAny) -> PyResult<Self> {
        let dict = obj.downcast::<PyDict>()?;
        Ok(ParquetLayerCols {
            parquet_dir: dict.get_item("parquet_dir").and_then(|item| item.expect("parquet_dir is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            layer: dict.get_item("layer").and_then(|item| item.expect("layer is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            src_col: dict.get_item("src_col").and_then(|item| item.expect("src_col is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            src_hash_col: dict.get_item("src_hash_col").and_then(|item| item.expect("src_hash_col is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            dst_col: dict.get_item("dst_col").and_then(|item| item.expect("dst_col is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            dst_hash_col: dict.get_item("dst_hash_col").and_then(|item| item.expect("dst_hash_col is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
            time_col: dict.get_item("time_col").and_then(|item| item.expect("time_col is required").extract::<&PyString>()).and_then(|s| s.to_str())?,
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
impl PyArrowGraph {
    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, src_col, dst_col, time_col))]
    pub fn load_from_pandas(
        graph_dir: &str,
        edge_df: &PyAny,
        src_col: &str,
        dst_col: &str,
        time_col: &str,
    ) -> Result<ArrowGraph, GraphError> {
        let graph: Result<ArrowGraph, PyErr> = Python::with_gil(|py| {
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

        graph.map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?} from pandas data frames")))
    }

    #[staticmethod]
    fn load_from_dir(graph_dir: &str) -> Result<ArrowGraph, GraphError> {
        ArrowGraph::load_from_dir(graph_dir)
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?} from dir {graph_dir}")))
    }

    #[staticmethod]
    #[pyo3(signature = (graph_dir, layer_parquet_cols, chunk_size, t_props_chunk_size, read_chunk_size, concurrent_files, num_threads))]
    fn load_from_parquets(
        graph_dir: &str,
        layer_parquet_cols: ParquetLayerColsList,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
    ) -> Result<ArrowGraph, GraphError> {
        let graph: Result<ArrowGraph, PyErr> = Python::with_gil(|py: Python<'_>| {
            let graph = Self::from_parquets(
                graph_dir, 
                layer_parquet_cols.0,
                chunk_size, 
                t_props_chunk_size,
                read_chunk_size,
                concurrent_files,
                num_threads,
            )?;
            Ok::<_, PyErr>(graph)
        });
        graph.map_err(|e| GraphError::LoadFailure(format!("Failed to load graph {e:?} from parquet files")))
    }
}

impl PyArrowGraph {
    fn from_pandas(
        graph_dir: &str,
        df: PretendDF,
        src: &str,
        dst: &str,
        time: &str,
    ) -> Result<ArrowGraph, GraphError> {
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

        ArrowGraph::load_from_edge_lists(
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

    fn from_parquets(
        graph_dir: &str,
        layer_parquet_cols: Vec<ParquetLayerCols>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
    ) -> Result<ArrowGraph, GraphError> {
        ArrowGraph::load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
        )
            .map_err(|err| GraphError::LoadFailure(format!("Failed to load graph {err:?}")))
    }
}
