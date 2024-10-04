use super::io::pandas_loaders::*;
use crate::{
    arrow2::{
        array::StructArray,
        datatypes::{ArrowDataType as DataType, Field},
    },
    core::utils::errors::GraphError,
    db::graph::views::deletion_graph::PersistentGraph,
    disk_graph::{graph_impl::ParquetLayerCols, DiskGraphStorage},
    io::{
        arrow::dataframe::{DFChunk, DFView},
        parquet_loaders::{
            get_parquet_file_paths, process_parquet_file_to_df, read_parquet_file,
            read_struct_arrays,
        },
    },
    prelude::Graph,
    python::{graph::graph::PyGraph, types::repr::StructReprBuilder},
};
use itertools::Itertools;
use pometry_storage::{graph::load_node_const_properties, RAError};
/// A columnar temporal graph.
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use std::path::{Path, PathBuf};

#[derive(Clone)]
#[pyclass(name = "DiskGraphStorage")]
pub struct PyDiskGraph {
    pub graph: DiskGraphStorage,
}

impl<G> AsRef<G> for PyDiskGraph
where
    DiskGraphStorage: AsRef<G>,
{
    fn as_ref(&self) -> &G {
        self.graph.as_ref()
    }
}

impl From<DiskGraphStorage> for PyDiskGraph {
    fn from(value: DiskGraphStorage) -> Self {
        Self { graph: value }
    }
}

impl From<PyDiskGraph> for DiskGraphStorage {
    fn from(value: PyDiskGraph) -> Self {
        value.graph
    }
}

impl IntoPy<PyObject> for DiskGraphStorage {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyDiskGraph::from(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for DiskGraphStorage {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let py_graph: PyRef<PyDiskGraph> = ob.extract()?;
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
    /// save graph in disk_graph format and memory map the result
    pub fn persist_as_disk_graph(
        &self,
        graph_dir: PathBuf,
    ) -> Result<DiskGraphStorage, GraphError> {
        self.graph.persist_as_disk_graph(graph_dir)
    }
}

#[pymethods]
impl PyDiskGraph {
    pub fn graph_dir(&self) -> &Path {
        self.graph.graph_dir()
    }

    pub fn to_events(&self) -> Graph {
        self.graph.clone().into_graph()
    }

    pub fn to_persistent(&self) -> PersistentGraph {
        self.graph.clone().into_persistent_graph()
    }

    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, time_col, src_col, dst_col))]
    pub fn load_from_pandas(
        graph_dir: PathBuf,
        edge_df: &PyAny,
        time_col: &str,
        src_col: &str,
        dst_col: &str,
    ) -> Result<DiskGraphStorage, GraphError> {
        let graph: Result<DiskGraphStorage, GraphError> = Python::with_gil(|py| {
            let cols_to_check = vec![src_col, dst_col, time_col];

            let df_columns: Vec<String> = edge_df.getattr("columns")?.extract()?;
            let df_columns: Vec<&str> = df_columns.iter().map(|x| x.as_str()).collect();

            let df_view = process_pandas_py_df(edge_df, py, df_columns)?;
            df_view.check_cols_exist(&cols_to_check)?;
            let src_index = df_view.get_index(src_col)?;
            let dst_index = df_view.get_index(dst_col)?;
            let time_index = df_view.get_index(time_col)?;

            let mut chunks_iter = df_view.chunks.peekable();
            let chunk_size = if let Some(result) = chunks_iter.peek() {
                match result {
                    Ok(df) => df.chunk.len(),
                    Err(e) => {
                        return Err(GraphError::LoadFailure(format!(
                            "Failed to load graph {e:?}"
                        )))
                    }
                }
            } else {
                return Err(GraphError::LoadFailure("No chunks available".to_string()));
            };

            let edge_lists = chunks_iter
                .map_ok(|df| {
                    let fields = df
                        .chunk
                        .iter()
                        .zip(df_view.names.iter())
                        .map(|(arr, col_name)| {
                            Field::new(col_name, arr.data_type().clone(), arr.null_count() > 0)
                        })
                        .collect_vec();
                    let s_array = StructArray::new(DataType::Struct(fields), df.chunk, None);
                    s_array
                })
                .collect::<Result<Vec<_>, GraphError>>()?;

            let graph = DiskGraphStorage::load_from_edge_lists(
                &edge_lists,
                chunk_size,
                chunk_size,
                graph_dir,
                time_index,
                src_index,
                dst_index,
            )?;
            Ok::<_, GraphError>(graph)
        });

        graph.map_err(|e| {
            GraphError::LoadFailure(format!(
                "Failed to load graph {e:?} from pandas data frames"
            ))
        })
    }

    #[staticmethod]
    fn load_from_dir(graph_dir: PathBuf) -> Result<DiskGraphStorage, GraphError> {
        DiskGraphStorage::load_from_dir(&graph_dir).map_err(|err| {
            GraphError::LoadFailure(format!(
                "Failed to load graph {err:?} from dir {}",
                graph_dir.display()
            ))
        })
    }

    #[staticmethod]
    #[pyo3(
        signature = (graph_dir, layer_parquet_cols, node_properties, chunk_size, t_props_chunk_size, read_chunk_size, concurrent_files, num_threads, node_type_col)
    )]
    fn load_from_parquets(
        graph_dir: PathBuf,
        layer_parquet_cols: ParquetLayerColsList,
        node_properties: Option<PathBuf>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
        node_type_col: Option<&str>,
    ) -> Result<DiskGraphStorage, GraphError> {
        DiskGraphStorage::load_from_parquets(
            graph_dir,
            layer_parquet_cols.0,
            node_properties,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
            node_type_col,
        )
        .map_err(|err| {
            GraphError::LoadFailure(format!("Failed to load graph from parquet files: {err:?}"))
        })
    }

    pub fn load_node_const_properties(
        &self,
        location: &str,
        col_names: Option<Vec<&str>>,
        chunk_size: Option<usize>,
    ) -> Result<DiskGraphStorage, GraphError> {
        let path = PathBuf::from_str(location).unwrap();
        let chunks = read_struct_arrays(&path, col_names.as_deref())?;
        let _ =
            load_node_const_properties(chunk_size.unwrap_or(200_000), self.graph_dir(), chunks)?;
        Self::load_from_dir(self.graph_dir().to_string_lossy().as_ref())
    }

    /// Merge this graph with another `DiskGraph`. Note that both graphs should have nodes that are
    /// sorted by their global ids or the resulting graph will be nonsense!
    fn merge_by_sorted_gids(
        &self,
        other: &Self,
        graph_dir: PathBuf,
    ) -> Result<DiskGraphStorage, GraphError> {
        self.graph.merge_by_sorted_gids(&other.graph, graph_dir)
    }

    fn __repr__(&self) -> String {
        StructReprBuilder::new("DiskGraph")
            .add_field("number_of_nodes", self.graph.inner.num_nodes())
            .add_field(
                "number_of_temporal_edges",
                self.graph.inner.count_temporal_edges(),
            )
            .add_field("earliest_time", self.graph.inner.earliest())
            .add_field("latest_time", self.graph.inner.latest())
            .finish()
    }
}
