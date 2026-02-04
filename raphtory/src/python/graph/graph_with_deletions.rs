//! Defines the `PersistentGraph` class, which represents a raphtory graph in memory.
//! Unlike in the `Graph` which has event semantics, `PersistentGraph` has edges that persist until explicitly deleted.
//!
//! This is the base class used to create a temporal graph, add nodes and edges,
//! create windows, and query the graph with a variety of algorithms.
//! It is a wrapper around a set of shards, which are the actual graph data structures.
//! In Python, this class wraps around the rust graph.
use super::graph::{PyGraph, PyGraphEncoder};
use crate::{
    db::{
        api::mutation::{AdditionOps, PropertyAdditionOps},
        graph::{edge::EdgeView, node::NodeView, views::deletion_graph::PersistentGraph},
    },
    errors::GraphError,
    io::{arrow::df_loaders::edges::ColumnNames, parquet_loaders::*},
    prelude::{DeletionOps, GraphViewOps, ImportOps, ParquetEncoder},
    python::{
        graph::{
            edge::PyEdge,
            io::arrow_loaders::{
                convert_py_prop_args, convert_py_schema, is_csv_path,
                load_edge_deletions_from_arrow_c_stream, load_edge_deletions_from_csv_path,
                load_edge_metadata_from_arrow_c_stream, load_edge_metadata_from_csv_path,
                load_edges_from_arrow_c_stream, load_edges_from_csv_path,
                load_graph_props_from_arrow_c_stream, load_node_metadata_from_arrow_c_stream,
                load_node_metadata_from_csv_path, load_nodes_from_arrow_c_stream,
                load_nodes_from_csv_path, CsvReadOptions,
            },
            node::PyNode,
            views::graph_view::PyGraphView,
        },
        utils::PyNodeRef,
    },
    serialise::StableEncode,
};
use pyo3::{exceptions::PyValueError, prelude::*, pybacked::PyBackedStr, Borrowed};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GID},
        storage::arc_str::ArcStr,
    },
    python::timeindex::EventTimeComponent,
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    path::PathBuf,
    sync::Arc,
};

#[cfg(feature = "search")]
use crate::{prelude::IndexMutationOps, python::graph::index::PyIndexSpec};

/// A temporal graph that allows edges and nodes to be deleted.
#[derive(Clone)]
#[pyclass(name = "PersistentGraph", extends = PyGraphView, frozen, module="raphtory")]
pub struct PyPersistentGraph {
    pub(crate) graph: PersistentGraph,
}

impl_serialise!(PyPersistentGraph, graph: PersistentGraph, "PersistentGraph");

impl Debug for PyPersistentGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.graph)
    }
}

impl From<PersistentGraph> for PyPersistentGraph {
    fn from(value: PersistentGraph) -> Self {
        Self { graph: value }
    }
}

impl<'py> IntoPyObject<'py> for PersistentGraph {
    type Target = PyPersistentGraph;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            (
                PyPersistentGraph::from(self.clone()),
                PyGraphView::from(self),
            ),
        )
    }
}

impl<'py> FromPyObject<'_, 'py> for PersistentGraph {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let binding = ob.cast::<PyPersistentGraph>()?;
        let g = binding.get();
        Ok(g.graph.clone())
    }
}

impl PyPersistentGraph {
    pub fn py_from_db_graph(db_graph: PersistentGraph) -> PyResult<Py<PyPersistentGraph>> {
        Python::attach(|py| {
            Py::new(
                py,
                (
                    PyPersistentGraph::from(db_graph.clone()),
                    PyGraphView::from(db_graph),
                ),
            )
        })
    }
}

/// A temporal graph that allows edges and nodes to be deleted.
#[pymethods]
impl PyPersistentGraph {
    #[new]
    #[pyo3(signature = (path = None))]
    pub fn py_new(path: Option<PathBuf>) -> Result<(Self, PyGraphView), GraphError> {
        let graph = match path {
            Some(path) => PersistentGraph::new_at_path(&path)?,
            None => PersistentGraph::new(),
        };
        Ok((
            Self {
                graph: graph.clone(),
            },
            PyGraphView::from(graph),
        ))
    }

    #[staticmethod]
    pub fn load(path: PathBuf) -> Result<PersistentGraph, GraphError> {
        PersistentGraph::load_from_path(&path)
    }

    /// Trigger a flush of the underlying storage if disk storage is enabled
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    pub fn flush(&self) -> Result<(), GraphError> {
        self.graph.flush()
    }

    fn __reduce__(&self) -> Result<(PyGraphEncoder, (Vec<u8>,)), GraphError> {
        let state = self.graph.encode_to_bytes()?;
        Ok((PyGraphEncoder, (state,)))
    }

    /// Persist graph to parquet files
    ///
    /// Arguments:
    ///     graph_dir (str | PathLike): the folder where the graph will be persisted as parquet
    ///
    /// Returns:
    ///     None:
    pub fn to_parquet(&self, graph_dir: PathBuf) -> Result<(), GraphError> {
        self.graph.encode_parquet(graph_dir)
    }

    /// Adds a new node with the given id and properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str | int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional) : The optional string which will be used as a node type.
    ///    event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, event_id = None))]
    pub fn add_node(
        &self,
        timestamp: EventTimeComponent,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<NodeView<'static, PersistentGraph>, GraphError> {
        match event_id {
            None => self
                .graph
                .add_node(timestamp, id, properties.unwrap_or_default(), node_type),
            Some(event_id) => self.graph.add_node(
                (timestamp, event_id),
                id,
                properties.unwrap_or_default(),
                node_type,
            ),
        }
    }

    /// Creates a new node with the given id and properties to the graph. It fails if the node already exists.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the node.
    ///    id (str | int): The id of the node.
    ///    properties (PropInput, optional): The properties of the node.
    ///    node_type (str, optional) : The optional string which will be used as a node type.
    ///    event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///   MutableNode: the newly created node.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, event_id = None))]
    pub fn create_node(
        &self,
        timestamp: EventTimeComponent,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<NodeView<'static, PersistentGraph>, GraphError> {
        match event_id {
            None => {
                self.graph
                    .create_node(timestamp, id, properties.unwrap_or_default(), node_type)
            }
            Some(event_id) => self.graph.create_node(
                (timestamp, event_id),
                id,
                properties.unwrap_or_default(),
                node_type,
            ),
        }
    }

    /// Adds properties to the graph.
    ///
    /// Arguments:
    ///    timestamp (TimeInput): The timestamp of the temporal property.
    ///    properties (dict): The temporal properties of the graph.
    ///    event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, properties, event_id = None))]
    pub fn add_properties(
        &self,
        timestamp: EventTimeComponent,
        properties: HashMap<String, Prop>,
        event_id: Option<usize>,
    ) -> Result<(), GraphError> {
        match event_id {
            None => self.graph.add_properties(timestamp, properties),
            Some(event_id) => self.graph.add_properties((timestamp, event_id), properties),
        }
    }

    /// Adds metadata to the graph.
    ///
    /// Arguments:
    ///     metadata (dict): The static properties of the graph.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    pub fn add_metadata(&self, metadata: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.graph.add_metadata(metadata)
    }

    /// Updates metadata of the graph.
    ///
    /// Arguments:
    ///     metadata (dict): The static properties of the graph.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    pub fn update_metadata(&self, metadata: HashMap<String, Prop>) -> Result<(), GraphError> {
        self.graph.update_metadata(metadata)
    }

    /// Adds a new edge with the given source and destination nodes and properties to the graph.
    ///
    /// Arguments:
    ///     timestamp (int): The timestamp of the edge.
    ///     src (str | int): The id of the source node.
    ///     dst (str | int): The id of the destination node.
    ///     properties (PropInput, optional): The properties of the edge, as a dict of string and properties.
    ///     layer (str, optional): The layer of the edge.
    ///     event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None, event_id = None))]
    pub fn add_edge(
        &self,
        timestamp: EventTimeComponent,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<EdgeView<PersistentGraph>, GraphError> {
        match event_id {
            None => self
                .graph
                .add_edge(timestamp, src, dst, properties.unwrap_or_default(), layer),
            Some(event_id) => self.graph.add_edge(
                (timestamp, event_id),
                src,
                dst,
                properties.unwrap_or_default(),
                layer,
            ),
        }
    }

    /// Deletes an edge given the timestamp, src and dst nodes and layer (optional).
    ///
    /// Arguments:
    ///   timestamp (int): The timestamp of the edge.
    ///   src (str | int): The id of the source node.
    ///   dst (str | int): The id of the destination node.
    ///   layer (str, optional): The layer of the edge.
    ///   event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///   MutableEdge: The deleted edge
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (timestamp, src, dst, layer=None, event_id = None))]
    pub fn delete_edge(
        &self,
        timestamp: EventTimeComponent,
        src: GID,
        dst: GID,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<EdgeView<PersistentGraph>, GraphError> {
        match event_id {
            None => self.graph.delete_edge(timestamp, src, dst, layer),
            Some(event_id) => self
                .graph
                .delete_edge((timestamp, event_id), src, dst, layer),
        }
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the node with the specified id
    ///
    /// Arguments:
    ///     id (str | int): the node id
    ///
    /// Returns:
    ///     Optional[MutableNode]: The node with the specified id, or None if the node does not exist
    pub fn node(&self, id: PyNodeRef) -> Option<NodeView<'static, PersistentGraph>> {
        self.graph.node(id)
    }

    //FIXME: This is reimplemented here to get mutable views. If we switch the underlying graph to enum dispatch, this won't be necessary!
    /// Gets the edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     Optional[MutableEdge]: The edge with the specified source and destination nodes, or None if the edge does not exist
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: PyNodeRef, dst: PyNodeRef) -> Option<EdgeView<PersistentGraph>> {
        self.graph.edge(src, dst)
    }

    /// Import a single node into the graph.
    ///
    /// This function takes a node object and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the node even if it already exists in the graph.
    ///
    /// Arguments:
    ///     node (Node): A node object representing the node to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the node. Defaults to False.
    ///
    /// Returns:
    ///     Node: A Node object if the node was successfully imported, and an error otherwise.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, merge = false))]
    pub fn import_node(
        &self,
        node: PyNode,
        merge: bool,
    ) -> Result<NodeView<'static, PersistentGraph>, GraphError> {
        self.graph.import_node(&node.node, merge)
    }

    /// Import a single node into the graph with new id.
    ///
    /// This function takes a node object, a new node id and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the node even if it already exists in the graph.
    ///
    /// Arguments:
    ///     node (Node): A node object representing the node to be imported.
    ///     new_id (str|int): The new node id.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the node. Defaults to False.
    ///
    /// Returns:
    ///     Node: A Node object if the node was successfully imported, and an error otherwise.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (node, new_id, merge = false))]
    pub fn import_node_as(
        &self,
        node: PyNode,
        new_id: GID,
        merge: bool,
    ) -> Result<NodeView<'static, PersistentGraph>, GraphError> {
        self.graph.import_node_as(&node.node, new_id, merge)
    }

    /// Import multiple nodes into the graph.
    ///
    /// This function takes a vector of node objects and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the nodes even if they already exist in the graph.
    ///
    /// Arguments:
    ///     nodes (List[Node]):  A vector of node objects representing the nodes to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the nodes. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (nodes, merge = false))]
    pub fn import_nodes(&self, nodes: Vec<PyNode>, merge: bool) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes(node_views, merge)
    }

    /// Import multiple nodes into the graph with new ids.
    ///
    /// This function takes a vector of node objects, a list of new node ids and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the nodes even if they already exist in the graph.
    ///
    /// Arguments:
    ///     nodes (List[Node]):  A vector of node objects representing the nodes to be imported.
    ///     new_ids (List[str|int]): A list of node IDs to use for the imported nodes.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the nodes. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (nodes, new_ids, merge = false))]
    pub fn import_nodes_as(
        &self,
        nodes: Vec<PyNode>,
        new_ids: Vec<GID>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let node_views = nodes.iter().map(|node| &node.node);
        self.graph.import_nodes_as(node_views, new_ids, merge)
    }

    /// Import a single edge into the graph.
    ///
    /// This function takes an edge object and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edge even if it already exists in the graph.
    ///
    /// Arguments:
    ///     edge (Edge): An edge object representing the edge to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edge. Defaults to False.
    ///
    /// Returns:
    ///     Edge: The imported edge.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, merge = false))]
    pub fn import_edge(
        &self,
        edge: PyEdge,
        merge: bool,
    ) -> Result<EdgeView<PersistentGraph>, GraphError> {
        self.graph.import_edge(&edge.edge, merge)
    }

    /// Import a single edge into the graph with new id.
    ///
    /// This function takes a edge object, a new edge id and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edge even if it already exists in the graph.
    ///
    /// Arguments:
    ///     edge (Edge): A edge object representing the edge to be imported.
    ///     new_id (tuple) : The ID of the new edge. It's a tuple of the source and destination node ids.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edge. Defaults to False.
    ///
    /// Returns:
    ///     Edge: The imported edge.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edge, new_id, merge = false))]
    pub fn import_edge_as(
        &self,
        edge: PyEdge,
        new_id: (GID, GID),
        merge: bool,
    ) -> Result<EdgeView<PersistentGraph>, GraphError> {
        self.graph.import_edge_as(&edge.edge, new_id, merge)
    }

    /// Import multiple edges into the graph.
    ///
    /// This function takes a vector of edge objects and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edges even if they already exist in the graph.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A vector of edge objects representing the edges to be imported.
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edges. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edges, merge = false))]
    pub fn import_edges(&self, edges: Vec<PyEdge>, merge: bool) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges(edge_views, merge)
    }

    /// Import multiple edges into the graph with new ids.
    ///
    /// This function takes a vector of edge objects, a list of new edge ids and an optional boolean flag. If the flag is set to true,
    /// the function will merge the import of the edges even if they already exist in the graph.
    ///
    /// Arguments:
    ///     edges (List[Edge]): A vector of edge objects representing the edges to be imported.
    ///     new_ids (list[Tuple[GID, GID]]): The new edge ids
    ///     merge (bool): An optional boolean flag indicating whether to merge the import of the edges. Defaults to False.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (edges, new_ids, merge = false))]
    pub fn import_edges_as(
        &self,
        edges: Vec<PyEdge>,
        new_ids: Vec<(GID, GID)>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let edge_views = edges.iter().map(|edge| &edge.edge);
        self.graph.import_edges_as(edge_views, new_ids, merge)
    }

    //******  Saving And Loading  ******//

    // Alternative constructors are tricky, see: https://gist.github.com/redshiftzero/648e4feeff3843ffd9924f13625f839c

    /// Returns all the node types in the graph.
    ///
    /// Returns:
    ///     list[str]: A list of node types
    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.graph.get_all_node_types()
    }

    /// Get event graph
    ///
    /// Returns:
    ///     Graph: the graph with event semantics applied
    pub fn event_graph(&self) -> PyResult<Py<PyGraph>> {
        PyGraph::py_from_db_graph(self.graph.event_graph())
    }

    /// Get persistent graph
    ///
    /// Returns:
    ///     PersistentGraph: the graph with persistent semantics applied
    pub fn persistent_graph(&self) -> PyResult<Py<PyPersistentGraph>> {
        PyPersistentGraph::py_from_db_graph(self.graph.persistent_graph())
    }

    /// Load nodes into the graph from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// a path to a CSV or Parquet file, or a directory containing multiple CSV or Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing the nodes.
    ///     time (str): The column name for the timestamps.
    ///     id (str): The column name for the node IDs.
    ///     node_type (str, optional): A value to use as the node type for all nodes. Cannot be used in combination with node_type_col. Defaults to None.
    ///     node_type_col (str, optional): The node type column name in a dataframe. Cannot be used in combination with node_type. Defaults to None.
    ///     properties (List[str], optional): List of node property column names. Defaults to None.
    ///     metadata (List[str], optional): List of node metadata column names. Defaults to None.
    ///     shared_metadata (PropInput, optional): A dictionary of metadata properties that will be added to every node. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     csv_options (dict[str, str | bool], optional): A dictionary of CSV reading options such as delimiter, comment, escape, quote, and terminator characters, as well as allow_truncated_rows and has_header flags. Defaults to None.
    ///     event_id (str, optional): The column name for the secondary index.
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (data, time, id, node_type = None, node_type_col = None, properties = None, metadata= None, shared_metadata = None, schema = None, csv_options = None, event_id = None)
    )]
    fn load_nodes(
        &self,
        data: &Bound<PyAny>,
        time: &str,
        id: &str,

        node_type: Option<&str>,
        node_type_col: Option<&str>,
        properties: Option<Vec<PyBackedStr>>,
        metadata: Option<Vec<PyBackedStr>>,
        shared_metadata: Option<HashMap<String, Prop>>,
        schema: Option<Bound<PyAny>>,
        csv_options: Option<CsvReadOptions>,
        event_id: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let metadata = convert_py_prop_args(metadata.as_deref()).unwrap_or_default();
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_nodes_from_arrow_c_stream(
                &self.graph,
                data,
                time,
                id,
                node_type,
                node_type_col,
                &properties,
                &metadata,
                shared_metadata.as_ref(),
                column_schema,
                event_id,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;
            let is_csv = is_csv_path(&path)?;

            // fail before loading anything at all to avoid loading partial data
            if !is_csv && csv_options.is_some() {
                return Err(GraphError::from(PyValueError::new_err(format!(
                    "CSV options were passed but no CSV files were detected at {}.",
                    path.display()
                ))));
            }

            // wrap in Arc to avoid cloning the entire schema for Parquet, CSV, and inner loops in CSV path
            let arced_schema = column_schema.map(Arc::new);

            // if-if instead of if-else to support directories with mixed parquet and CSV files
            if is_parquet {
                load_nodes_from_parquet(
                    &self.graph,
                    path.as_path(),
                    time,
                    event_id,
                    id,
                    node_type,
                    node_type_col,
                    &properties,
                    &metadata,
                    shared_metadata.as_ref(),
                    None,
                    true,
                    arced_schema.clone(),
                )?;
            }
            if is_csv {
                load_nodes_from_csv_path(
                    &self.graph,
                    &path,
                    time,
                    id,
                    node_type,
                    node_type_col,
                    &properties,
                    &metadata,
                    shared_metadata.as_ref(),
                    csv_options.as_ref(),
                    arced_schema,
                    event_id,
                )?;
            }
            if !is_parquet && !is_csv {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet or CSV file, a directory containing Parquet or CSV files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Load edges into the graph from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// a path to a CSV or Parquet file, or a directory containing multiple CSV or Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node IDs.
    ///     dst (str): The column name for the destination node IDs.
    ///     properties (List[str], optional): List of edge property column names. Defaults to None.
    ///     metadata (List[str], optional): List of edge metadata column names. Defaults to None.
    ///     shared_metadata (PropInput, optional): A dictionary of metadata properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): A value to use as the layer for all edges. Cannot be used in combination with layer_col. Defaults to None.
    ///     layer_col (str, optional): The edge layer column name in a dataframe. Cannot be used in combination with layer. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     csv_options (dict[str, str | bool], optional): A dictionary of CSV reading options such as delimiter, comment, escape, quote, and terminator characters, as well as allow_truncated_rows and has_header flags. Defaults to None.
    ///     event_id (str, optional): The column name for the secondary index.
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (data, time, src, dst, properties = None, metadata = None, shared_metadata = None, layer = None, layer_col = None, schema = None, csv_options = None, event_id = None)
    )]
    fn load_edges(
        &self,
        data: &Bound<PyAny>,
        time: &str,
        src: &str,
        dst: &str,

        properties: Option<Vec<PyBackedStr>>,
        metadata: Option<Vec<PyBackedStr>>,
        shared_metadata: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
        schema: Option<Bound<PyAny>>,
        csv_options: Option<CsvReadOptions>,
        event_id: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let metadata = convert_py_prop_args(metadata.as_deref()).unwrap_or_default();
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_edges_from_arrow_c_stream(
                &self.graph,
                data,
                time,
                src,
                dst,
                &properties,
                &metadata,
                shared_metadata.as_ref(),
                layer,
                layer_col,
                column_schema,
                event_id,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;
            let is_csv = is_csv_path(&path)?;

            // fail before loading anything at all to avoid loading partial data
            if !is_csv && csv_options.is_some() {
                return Err(GraphError::from(PyValueError::new_err(format!(
                    "CSV options were passed but no CSV files were detected at {}.",
                    path.display()
                ))));
            }

            // wrap in Arc to avoid cloning the entire schema for Parquet, CSV, and inner loops in CSV path
            let arced_schema = column_schema.map(Arc::new);

            // if-if instead of if-else to support directories with mixed parquet and CSV files
            if is_parquet {
                load_edges_from_parquet(
                    &self.graph,
                    &path,
                    ColumnNames::new(time, event_id, src, dst, layer_col),
                    true,
                    &properties,
                    &metadata,
                    shared_metadata.as_ref(),
                    layer,
                    None,
                    arced_schema.clone(),
                )?;
            }
            if is_csv {
                load_edges_from_csv_path(
                    &self.graph,
                    &path,
                    time,
                    src,
                    dst,
                    &properties,
                    &metadata,
                    shared_metadata.as_ref(),
                    layer,
                    layer_col,
                    csv_options.as_ref(),
                    arced_schema.clone(),
                    event_id,
                )?;
            }
            if !is_parquet && !is_csv {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet or CSV file, a directory containing Parquet or CSV files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Load edge deletions into the graph from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// a path to a CSV or Parquet file, or a directory containing multiple CSV or Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing the edges.
    ///     time (str): The column name for the update timestamps.
    ///     src (str): The column name for the source node ids.
    ///     dst (str): The column name for the destination node ids.
    ///     layer (str, optional): A value to use as the layer for all edges. Cannot be used in combination with layer_col. Defaults to None.
    ///     layer_col (str, optional): The edge layer col name in the data source. Cannot be used in combination with layer. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     csv_options (dict[str, str | bool], optional): A dictionary of CSV reading options such as delimiter, comment, escape, quote, and terminator characters, as well as allow_truncated_rows and has_header flags. Defaults to None.
    ///     event_id (str, optional): The column name for the secondary index.
    ///
    /// Returns:
    ///     None: This function does not return a value, if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(signature = (data, time, src, dst, layer = None, layer_col = None, schema = None, csv_options = None, event_id = None))]
    fn load_edge_deletions(
        &self,
        data: &Bound<PyAny>,
        time: &str,
        src: &str,
        dst: &str,

        layer: Option<&str>,
        layer_col: Option<&str>,
        schema: Option<Bound<PyAny>>,
        csv_options: Option<CsvReadOptions>,
        event_id: Option<&str>,
    ) -> Result<(), GraphError> {
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_edge_deletions_from_arrow_c_stream(
                &self.graph,
                data,
                time,
                event_id,
                src,
                dst,
                layer,
                layer_col,
                column_schema,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;
            let is_csv = is_csv_path(&path)?;

            // fail before loading anything at all to avoid loading partial data
            if !is_csv && csv_options.is_some() {
                return Err(GraphError::from(PyValueError::new_err(format!(
                    "CSV options were passed but no CSV files were detected at {}.",
                    path.display()
                ))));
            }

            // wrap in Arc to avoid cloning the entire schema for Parquet, CSV, and inner loops in CSV path
            let arced_schema = column_schema.map(Arc::new);

            // if-if instead of if-else to support directories with mixed parquet and CSV files
            if is_parquet {
                load_edge_deletions_from_parquet(
                    &self.graph,
                    path.as_path(),
                    ColumnNames::new(time, event_id, src, dst, layer_col),
                    layer,
                    true,
                    None,
                    arced_schema.clone(),
                )?;
            }
            if is_csv {
                load_edge_deletions_from_csv_path(
                    &self.graph,
                    &path,
                    time,
                    src,
                    dst,
                    layer,
                    layer_col,
                    csv_options.as_ref(),
                    arced_schema,
                    event_id,
                )?;
            }
            if !is_parquet && !is_csv {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet or CSV file, a directory containing Parquet or CSV files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Load node metadata into the graph from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// a path to a CSV or Parquet file, or a directory containing multiple CSV or Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing node information.
    ///     id(str): The column name for the node IDs.
    ///     node_type (str, optional): A value to use as the node type for all nodes. Cannot be used in combination with node_type_col. Defaults to None.
    ///     node_type_col (str, optional): The node type column name in a dataframe. Cannot be used in combination with node_type. Defaults to None.
    ///     metadata (List[str], optional): List of node metadata column names. Defaults to None.
    ///     shared_metadata (PropInput, optional): A dictionary of metadata properties that will be added to every node. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     csv_options (dict[str, str | bool], optional): A dictionary of CSV reading options such as delimiter, comment, escape, quote, and terminator characters, as well as allow_truncated_rows and has_header flags. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (data, id, node_type = None, node_type_col = None, metadata = None, shared_metadata = None, schema = None, csv_options = None)
    )]
    fn load_node_metadata(
        &self,
        data: &Bound<PyAny>,
        id: &str,
        node_type: Option<&str>,
        node_type_col: Option<&str>,
        metadata: Option<Vec<PyBackedStr>>,
        shared_metadata: Option<HashMap<String, Prop>>,
        schema: Option<Bound<PyAny>>,
        csv_options: Option<CsvReadOptions>,
    ) -> Result<(), GraphError> {
        let metadata = convert_py_prop_args(metadata.as_deref()).unwrap_or_default();
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_node_metadata_from_arrow_c_stream(
                &self.graph,
                data,
                id,
                node_type,
                node_type_col,
                &metadata,
                shared_metadata.as_ref(),
                column_schema,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;
            let is_csv = is_csv_path(&path)?;

            // fail before loading anything at all to avoid loading partial data
            if !is_csv && csv_options.is_some() {
                return Err(GraphError::from(PyValueError::new_err(format!(
                    "CSV options were passed but no CSV files were detected at {}.",
                    path.display()
                ))));
            }

            // wrap in Arc to avoid cloning the entire schema for Parquet, CSV, and inner loops in CSV path
            let arced_schema = column_schema.map(Arc::new);

            // if-if instead of if-else to support directories with mixed parquet and CSV files
            if is_parquet {
                load_node_metadata_from_parquet(
                    &self.graph,
                    path.as_path(),
                    id,
                    node_type,
                    node_type_col,
                    None,
                    None,
                    &metadata,
                    shared_metadata.as_ref(),
                    None,
                    arced_schema.clone(),
                )?;
            }
            if is_csv {
                load_node_metadata_from_csv_path(
                    &self.graph,
                    &path,
                    id,
                    node_type,
                    node_type_col,
                    &metadata,
                    shared_metadata.as_ref(),
                    csv_options.as_ref(),
                    arced_schema,
                )?;
            }
            if !is_parquet && !is_csv {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet or CSV file, a directory containing Parquet or CSV files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Load edge metadata into the graph from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// a path to a CSV or Parquet file, or a directory containing multiple CSV or Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing edge information.
    ///     src (str): The column name for the source node.
    ///     dst (str): The column name for the destination node.
    ///     metadata (List[str], optional): List of edge metadata column names. Defaults to None.
    ///     shared_metadata (PropInput, optional): A dictionary of metadata properties that will be added to every edge. Defaults to None.
    ///     layer (str, optional): The edge layer name. Defaults to None.
    ///     layer_col (str, optional): The edge layer column name in a dataframe. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     csv_options (dict[str, str | bool], optional): A dictionary of CSV reading options such as delimiter, comment, escape, quote, and terminator characters, as well as allow_truncated_rows and has_header flags. Defaults to None.
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (data, src, dst, metadata = None, shared_metadata = None, layer = None, layer_col = None, schema = None, csv_options = None)
    )]
    fn load_edge_metadata(
        &self,
        data: &Bound<PyAny>,
        src: &str,
        dst: &str,
        metadata: Option<Vec<PyBackedStr>>,
        shared_metadata: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        layer_col: Option<&str>,
        schema: Option<Bound<PyAny>>,
        csv_options: Option<CsvReadOptions>,
    ) -> Result<(), GraphError> {
        let metadata = convert_py_prop_args(metadata.as_deref()).unwrap_or_default();
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_edge_metadata_from_arrow_c_stream(
                &self.graph,
                data,
                src,
                dst,
                &metadata,
                shared_metadata.as_ref(),
                layer,
                layer_col,
                column_schema,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;
            let is_csv = is_csv_path(&path)?;

            // fail before loading anything at all to avoid loading partial data
            if !is_csv && csv_options.is_some() {
                return Err(GraphError::from(PyValueError::new_err(format!(
                    "CSV options were passed but no CSV files were detected at {}.",
                    path.display()
                ))));
            }

            // wrap in Arc to avoid cloning the entire schema for Parquet, CSV, and inner loops in CSV path
            let arced_schema = column_schema.map(Arc::new);

            // if-if instead of if-else to support directories with mixed parquet and CSV files
            if is_parquet {
                load_edge_metadata_from_parquet(
                    &self.graph,
                    path.as_path(),
                    src,
                    dst,
                    &metadata,
                    shared_metadata.as_ref(),
                    layer,
                    layer_col,
                    None,
                    arced_schema.clone(),
                    true,
                )?;
            }
            if is_csv {
                load_edge_metadata_from_csv_path(
                    &self.graph,
                    &path,
                    src,
                    dst,
                    &metadata,
                    shared_metadata.as_ref(),
                    layer,
                    layer_col,
                    csv_options.as_ref(),
                    arced_schema,
                )?;
            }
            if !is_parquet && !is_csv {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet or CSV file, a directory containing Parquet or CSV files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Load graph properties from any data source that supports the ArrowStreamExportable protocol (by providing an __arrow_c_stream__() method),
    /// or a path to a Parquet file, or a directory containing multiple Parquet files.
    /// The following are known to support the ArrowStreamExportable protocol: Pandas dataframes, FireDucks(.pandas) dataframes,
    /// Polars dataframes, Arrow tables, DuckDB (e.g. DuckDBPyRelation obtained from running an SQL query).
    ///
    /// Arguments:
    ///     data (Any): The data source containing graph properties.
    ///     time (str): The column name for the update timestamps.
    ///     properties (List[str], optional): List of temporal property column names. Defaults to None.
    ///     metadata (List[str], optional): List of constant property column names. Defaults to None.
    ///     schema (list[tuple[str, DataType | PropType | str]] | dict[str, DataType | PropType | str], optional): A list of (column_name, column_type) tuples or dict of {"column_name": column_type} to cast columns to. Defaults to None.
    ///     event_id (str, optional): The column name for the secondary index.
    ///
    /// Returns:
    ///     None: This function does not return a value if the operation is successful.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    #[pyo3(
        signature = (data, time, properties = None, metadata = None, schema = None, event_id = None)
    )]
    fn load_graph_properties(
        &self,
        data: &Bound<PyAny>,
        time: &str,
        properties: Option<Vec<PyBackedStr>>,
        metadata: Option<Vec<PyBackedStr>>,
        schema: Option<Bound<PyAny>>,
        event_id: Option<&str>,
    ) -> Result<(), GraphError> {
        let properties = convert_py_prop_args(properties.as_deref()).unwrap_or_default();
        let metadata = convert_py_prop_args(metadata.as_deref()).unwrap_or_default();
        let column_schema = convert_py_schema(schema)?;
        if data.hasattr("__arrow_c_stream__")? {
            load_graph_props_from_arrow_c_stream(
                &self.graph,
                data,
                time,
                event_id,
                Some(&properties),
                Some(&metadata),
                column_schema,
            )
        } else if let Ok(path) = data.extract::<PathBuf>() {
            // extracting PathBuf handles Strings too
            let is_parquet = is_parquet_path(&path)?;

            // wrap in Arc to avoid cloning the entire schema for Parquet and inner loops
            let arced_schema = column_schema.map(Arc::new);

            if is_parquet {
                load_graph_props_from_parquet(
                    &self.graph,
                    path.as_path(),
                    time,
                    event_id,
                    &properties,
                    &metadata,
                    None,
                    arced_schema,
                )?;
            } else {
                return Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' contains invalid path. Paths must either point to a Parquet file, or a directory containing Parquet files")));
            }
            Ok(())
        } else {
            Err(GraphError::PythonError(PyValueError::new_err("Argument 'data' invalid. Valid data sources are: a single Parquet file, a directory containing Parquet files, and objects that implement an __arrow_c_stream__ method.")))
        }
    }

    /// Create graph index
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "search")]
    fn create_index(&self) -> Result<(), GraphError> {
        self.graph.create_index()
    }

    /// Create graph index with the provided index spec.
    /// Arguments:
    ///     py_spec: - The specification for the in-memory index to be created.
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "search")]
    fn create_index_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph.create_index_with_spec(py_spec.spec.clone())
    }

    /// Creates a graph index in memory (RAM).
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "search")]
    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        self.graph.create_index_in_ram()
    }

    /// Creates a graph index in memory (RAM) with the provided index spec.
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    ///
    /// Arguments:
    ///     py_spec: The specification for the in-memory index to be created.
    ///
    ///  Arguments:
    ///     py_spec (IndexSpec): The specification for the in-memory index to be created.
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "search")]
    fn create_index_in_ram_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph
            .create_index_in_ram_with_spec(py_spec.spec.clone())
    }
}
