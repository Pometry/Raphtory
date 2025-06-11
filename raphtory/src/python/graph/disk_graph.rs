//! A columnar temporal graph.
//!
use super::io::pandas_loaders::*;
use crate::{
    arrow2::{
        array::StructArray,
        datatypes::{ArrowDataType as DataType, Field},
    },
    db::{
        api::storage::graph::storage_ops::disk_storage::IntoGraph,
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    io::parquet_loaders::read_struct_arrays,
    prelude::Graph,
    python::{graph::graph::PyGraph, types::repr::StructReprBuilder},
};
use itertools::Itertools;
use pometry_storage::{
    graph::{load_node_const_properties, TemporalGraph},
    RAError,
};
use pyo3::{exceptions::PyRuntimeError, prelude::*, pybacked::PyBackedStr, types::PyDict};
use raphtory_storage::disk::{DiskGraphStorage, ParquetLayerCols};
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(Clone)]
#[pyclass(name = "DiskGraphStorage", frozen, module = "raphtory")]
pub struct PyDiskGraph(pub DiskGraphStorage);

impl<G> AsRef<G> for PyDiskGraph
where
    DiskGraphStorage: AsRef<G>,
{
    fn as_ref(&self) -> &G {
        self.0.as_ref()
    }
}

impl From<DiskGraphStorage> for PyDiskGraph {
    fn from(value: DiskGraphStorage) -> Self {
        Self(value)
    }
}

impl From<PyDiskGraph> for DiskGraphStorage {
    fn from(value: PyDiskGraph) -> Self {
        value.0
    }
}

struct PyParquetLayerCols {
    parquet_dir: PyBackedStr,
    layer: PyBackedStr,
    src_col: PyBackedStr,
    dst_col: PyBackedStr,
    time_col: PyBackedStr,
    exclude_edge_props: Vec<PyBackedStr>,
}

impl PyParquetLayerCols {
    pub fn as_deref(&self) -> ParquetLayerCols {
        ParquetLayerCols {
            parquet_dir: self.parquet_dir.deref(),
            layer: self.layer.deref(),
            src_col: self.src_col.deref(),
            dst_col: self.dst_col.deref(),
            time_col: self.time_col.deref(),
            exclude_edge_props: self.exclude_edge_props.iter().map(|s| s.deref()).collect(),
        }
    }
}

impl<'a> FromPyObject<'a> for PyParquetLayerCols {
    fn extract_bound(obj: &Bound<'a, PyAny>) -> PyResult<Self> {
        let dict = obj.downcast::<PyDict>()?;
        Ok(PyParquetLayerCols {
            parquet_dir: dict
                .get_item("parquet_dir")?
                .ok_or(PyRuntimeError::new_err("parquet_dir is required"))?
                .extract::<PyBackedStr>()?,
            layer: dict
                .get_item("layer")?
                .ok_or(PyRuntimeError::new_err("layer is  required"))?
                .extract::<PyBackedStr>()?,
            src_col: dict
                .get_item("src_col")?
                .ok_or(PyRuntimeError::new_err("src_col is required"))?
                .extract::<PyBackedStr>()?,
            dst_col: dict
                .get_item("dst_col")?
                .ok_or(PyRuntimeError::new_err("dst_col is required"))?
                .extract::<PyBackedStr>()?,
            time_col: dict
                .get_item("time_col")?
                .ok_or(PyRuntimeError::new_err("time_col is required"))?
                .extract::<PyBackedStr>()?,
            exclude_edge_props: match dict.get_item("exclude_edge_props")? {
                None => Ok(vec![]),
                Some(item) => item
                    .try_iter()?
                    .map(|v| v.and_then(|v| v.extract::<PyBackedStr>()))
                    .collect::<PyResult<Vec<_>>>(),
            }?,
        })
    }
}

#[pymethods]
impl PyGraph {
    /// save graph in disk_graph format and memory map the result
    ///
    /// Arguments:
    ///     graph_dir (str | PathLike): folder where the graph will be saved
    ///
    /// Returns:
    ///     DiskGraphStorage: the persisted graph storage
    pub fn persist_as_disk_graph(&self, graph_dir: PathBuf) -> Result<PyDiskGraph, GraphError> {
        Ok(PyDiskGraph(DiskGraphStorage::from_graph(
            &self.graph,
            &graph_dir,
        )?))
    }
}

#[pymethods]
impl PyDiskGraph {
    pub fn graph_dir(&self) -> &Path {
        self.0.graph_dir()
    }

    pub fn to_events(&self) -> Graph {
        self.0.clone().into_graph()
    }

    pub fn to_persistent(&self) -> PersistentGraph {
        self.0.clone().into_persistent_graph()
    }

    #[staticmethod]
    #[pyo3(signature = (graph_dir, edge_df, time_col, src_col, dst_col))]
    pub fn load_from_pandas(
        graph_dir: PathBuf,
        edge_df: &Bound<PyAny>,
        time_col: &str,
        src_col: &str,
        dst_col: &str,
    ) -> Result<PyDiskGraph, GraphError> {
        let cols_to_check = vec![src_col, dst_col, time_col];

        let df_columns: Vec<String> = edge_df.getattr("columns")?.extract()?;
        let df_columns: Vec<&str> = df_columns.iter().map(|x| x.as_str()).collect();

        let df_view = process_pandas_py_df(edge_df, df_columns)?;
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

        Ok(PyDiskGraph(graph))
    }

    #[staticmethod]
    fn load_from_dir(graph_dir: PathBuf) -> Result<PyDiskGraph, GraphError> {
        DiskGraphStorage::load_from_dir(&graph_dir)
            .map_err(|err| {
                GraphError::LoadFailure(format!(
                    "Failed to load graph {err:?} from dir {}",
                    graph_dir.display()
                ))
            })
            .map(PyDiskGraph)
    }

    #[staticmethod]
    #[pyo3(
        signature = (graph_dir, layer_parquet_cols, node_properties=None, chunk_size=10_000_000, t_props_chunk_size=10_000_000, num_threads=4, node_type_col=None, node_id_col=None)
    )]
    fn load_from_parquets(
        graph_dir: PathBuf,
        layer_parquet_cols: Vec<PyParquetLayerCols>,
        node_properties: Option<PathBuf>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        num_threads: usize,
        node_type_col: Option<&str>,
        node_id_col: Option<&str>,
    ) -> Result<PyDiskGraph, GraphError> {
        let layer_cols = layer_parquet_cols
            .iter()
            .map(|layer| layer.as_deref())
            .collect();
        DiskGraphStorage::load_from_parquets(
            graph_dir,
            layer_cols,
            node_properties,
            chunk_size,
            t_props_chunk_size,
            num_threads,
            node_type_col,
            node_id_col,
        )
        .map_err(|err| {
            GraphError::LoadFailure(format!("Failed to load graph from parquet files: {err:?}"))
        })
        .map(PyDiskGraph)
    }

    #[pyo3(signature = (location, col_names=None, chunk_size=None))]
    pub fn load_node_const_properties(
        &self,
        location: PathBuf,
        col_names: Option<Vec<PyBackedStr>>,
        chunk_size: Option<usize>,
    ) -> Result<PyDiskGraph, GraphError> {
        let col_names = convert_py_prop_args(col_names.as_deref());
        let chunks = read_struct_arrays(&location, col_names.as_deref())?;
        let _ =
            load_node_const_properties(chunk_size.unwrap_or(200_000), self.graph_dir(), chunks)?;
        Self::load_from_dir(self.graph_dir().to_path_buf())
    }

    #[pyo3(signature=(location, col_name, chunk_size=None))]
    pub fn load_node_types(
        &self,
        location: PathBuf,
        col_name: &str,
        chunk_size: Option<usize>,
    ) -> Result<PyDiskGraph, GraphError> {
        let mut cloned = self.clone();
        let chunks = read_struct_arrays(&location, Some(&[col_name]))?.map(|chunk| match chunk {
            Ok(chunk) => {
                let (_, cols, _) = chunk.into_data();
                cols.into_iter().next().ok_or(RAError::EmptyChunk)
            }
            Err(err) => Err(err),
        });
        cloned
            .0
            .load_node_types_from_arrays(chunks, chunk_size.unwrap_or(1_000_000))?;
        Ok(cloned)
    }

    #[pyo3(signature = (location, chunk_size=20_000_000))]
    pub fn append_node_temporal_properties(
        &self,
        location: &str,
        chunk_size: usize,
    ) -> Result<PyDiskGraph, GraphError> {
        let path = PathBuf::from_str(location).unwrap();
        let chunks = read_struct_arrays(&path, None)?;
        let mut graph = TemporalGraph::new(self.0.inner.graph_dir())?;
        graph.load_temporal_node_props_from_chunks(chunks, chunk_size, false)?;
        Self::load_from_dir(self.graph_dir().to_path_buf())
    }

    /// Merge this graph with another `DiskGraph`. Note that both graphs should have nodes that are
    /// sorted by their global ids or the resulting graph will be nonsense!
    fn merge_by_sorted_gids(
        &self,
        other: &Self,
        graph_dir: PathBuf,
    ) -> Result<PyDiskGraph, GraphError> {
        Ok(PyDiskGraph(
            self.0.merge_by_sorted_gids(&other.0, graph_dir)?,
        ))
    }

    fn __repr__(&self) -> String {
        StructReprBuilder::new("DiskGraph")
            .add_field("number_of_nodes", self.0.inner.num_nodes())
            .add_field(
                "number_of_temporal_edges",
                self.0.inner.count_temporal_edges(),
            )
            .add_field("earliest_time", self.0.inner.earliest())
            .add_field("latest_time", self.0.inner.latest())
            .finish()
    }
}
