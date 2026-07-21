use crate::{
    client::{
        op::{
            AddEdges as AddEdgesOp, AddNodes as AddNodesOp, EdgeAddition, NodeAddition, Op, WriteOp,
        },
        remote_graph::RemoteGraph,
        ClientError,
    },
    python::client::{
        remote_edge::PyRemoteEdge,
        remote_edges::PyRemoteEdges,
        remote_history::PyRemoteEventTime,
        remote_metadata::{PyRemoteMetadata, PyRemoteProperties},
        remote_node::PyRemoteNode,
        remote_nodes::PyRemoteNodes,
        remote_schema::PyRemoteGraphSchema,
        PyEdgeAddition, PyNodeAddition,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::EventTime,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
#[pyclass(name = "RemoteGraph", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteGraph {
    pub(crate) graph: Arc<RemoteGraph>,
}

#[pymethods]
impl PyRemoteGraph {
    /// Restrict the graph to a time window `[start, end)`.
    ///
    /// Lazy: builds up a read expression on the returned `RemoteGraph` without
    /// firing an RPC. Terminals invoked on child references (e.g.
    /// `rg.window(0, 10).node("ben").degree()`) evaluate under the accumulated
    /// view chain.
    ///
    /// Arguments:
    ///     start (int): inclusive start of the window
    ///     end (int): exclusive end of the window
    ///
    /// Returns:
    ///     RemoteGraph: a new remote graph view restricted to the window
    pub fn window(&self, start: i64, end: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.window(start, end)),
        }
    }

    /// Return a filtered graph view. Mirrors the local
    /// `Graph.filter(FilterExpr)`: pass a node filter to keep matching nodes
    /// (edges survive only if both endpoints do), or an edge filter to keep
    /// matching edges (nodes remain even if all their edges drop). Lazy — no
    /// RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteGraph: a new filtered graph view.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter` or `EdgeFilter`.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteGraph> {
        // Dispatch matches the local unified `Graph.filter`: node filters
        // route to the server `filterNodes` field, edge filters to
        // `filterEdges`. Try node first; fall back to edge.
        if let Ok(node) = filter.try_as_node_filter() {
            let gql = node
                .try_into()
                .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
            return Ok(PyRemoteGraph {
                graph: Arc::new(self.graph.filter_nodes(gql)),
            });
        }
        let edge = filter
            .try_as_edge_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql = edge
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteGraph {
            graph: Arc::new(self.graph.filter_edges(gql)),
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.layer(name)),
        }
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.at(time)),
        }
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.before(time)),
        }
    }

    /// Restrict to events at or after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.after(time)),
        }
    }

    /// Restrict to the latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.latest()),
        }
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.snapshot_latest()),
        }
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.snapshot_at(time)),
        }
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_layer(name)),
        }
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_window(start, end)),
        }
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_start(start)),
        }
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_end(end)),
        }
    }

    /// Restrict to the "valid" subgraph (event-graph filter). Lazy — no RPC.
    pub fn valid(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.valid()),
        }
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.default_layer()),
        }
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.layers(names)),
        }
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_layers(names)),
        }
    }

    /// Restrict to a subgraph induced by the given node ids. Lazy — no RPC.
    pub fn subgraph(&self, nodes: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.subgraph(nodes)),
        }
    }

    /// Restrict to nodes matching one of the given node types. Lazy — no RPC.
    pub fn subgraph_node_types(&self, node_types: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.subgraph_node_types(node_types)),
        }
    }

    /// Exclude the given nodes from the view. Lazy — no RPC.
    pub fn exclude_nodes(&self, nodes: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_nodes(nodes)),
        }
    }

    /// Terminal: total node count under the current view. Fires one RPC.
    pub fn count_nodes(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_nodes().await })
    }

    /// Terminal: total edge count under the current view. Fires one RPC.
    pub fn count_edges(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_edges().await })
    }

    /// Earliest event time under the current view. `None` if the view has no
    /// events. Property — attribute access fires one RPC.
    #[getter]
    pub fn earliest_time(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.earliest_time().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// Latest event time under the current view. Property — fires one RPC.
    #[getter]
    pub fn latest_time(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.latest_time().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// View start bound. `None` for an unbounded view. Property — fires one RPC.
    #[getter]
    pub fn start(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.start().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// View end bound. `None` for an unbounded view. Property — fires one RPC.
    #[getter]
    pub fn end(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.end().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// Terminal: graph creation timestamp. Fires one RPC.
    pub fn created(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.created().await })
    }

    /// Terminal: last time this graph was opened. Fires one RPC.
    pub fn last_opened(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.last_opened().await })
    }

    /// Terminal: last time this graph was updated. Fires one RPC.
    pub fn last_updated(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.last_updated().await })
    }

    /// List of unique layer names present in this graph. Property — fires one RPC.
    #[getter]
    pub fn unique_layers(&self) -> Result<Vec<String>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.unique_layers().await })
    }

    /// Terminal: earliest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub fn earliest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.earliest_edge_time().await })
    }

    /// Terminal: latest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub fn latest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.latest_edge_time().await })
    }

    /// Terminal: does the graph have a node with this id? Fires one RPC.
    pub fn has_node(&self, id: GID) -> Result<bool, ClientError> {
        let graph = Arc::clone(&self.graph);
        let id_str = id.to_string();
        execute_async_task(move || async move { graph.has_node(id_str).await })
    }

    /// Terminal: does the graph have an edge `(src, dst)`? Fires one RPC.
    #[pyo3(signature = (src, dst))]
    pub fn has_edge(&self, src: GID, dst: GID) -> Result<bool, ClientError> {
        let graph = Arc::clone(&self.graph);
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        execute_async_task(move || async move { graph.has_edge(src_str, dst_str).await })
    }

    /// Terminal: total temporal-edge count (edge updates) under the current
    /// view. Fires one RPC.
    pub fn count_temporal_edges(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_temporal_edges().await })
    }

    /// Terminal: the graph's name. Fires one RPC.
    pub fn name(&self) -> Result<String, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.name().await })
    }

    /// Terminal: the graph's full path. Fires one RPC.
    pub fn path(&self) -> Result<String, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.path().await })
    }

    /// Terminal: the parent namespace of the graph path. Fires one RPC.
    pub fn namespace(&self) -> Result<String, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.namespace().await })
    }

    /// Gets a remote node with the specified id.
    ///
    /// Inherits any view chain built up on the parent `RemoteGraph` (e.g. after
    /// `rg.window(...)`) so subsequent terminals like `degree()` evaluate under
    /// the same view context.
    ///
    /// Fires one RPC — a `hasNode` check against the current view chain.
    /// Raises `NotFound` if the node isn't visible under the current view.
    ///
    /// Arguments:
    ///     id (str | int): the node id
    ///
    /// Returns:
    ///     RemoteNode: the remote node reference
    pub fn node(&self, id: GID) -> Result<PyRemoteNode, ClientError> {
        let graph = Arc::clone(&self.graph);
        let id_str = id.to_string();
        let node = execute_async_task(move || async move { graph.node(id_str).await })?;
        Ok(PyRemoteNode::new(node))
    }

    /// Gets a remote edge with the specified source and destination nodes.
    ///
    /// Fires one RPC — a `hasEdge` check against the current view chain.
    /// Raises `NotFound` if the edge isn't visible under the current view.
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge reference
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: GID, dst: GID) -> Result<PyRemoteEdge, ClientError> {
        let graph = Arc::clone(&self.graph);
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let edge = execute_async_task(move || async move { graph.edge(src_str, dst_str).await })?;
        Ok(PyRemoteEdge::new(edge))
    }

    /// The collection of all nodes in this graph under the current view.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteNodes: a handle to the nodes collection.
    #[getter]
    pub fn nodes(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.graph.nodes())
    }

    /// The collection of all edges in this graph under the current view.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteEdges: a handle to the edges collection.
    #[getter]
    pub fn edges(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.graph.edges())
    }

    /// The non-temporal metadata container of this graph. Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteMetadata: a handle to the metadata container.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadata {
        PyRemoteMetadata::new(self.graph.metadata())
    }

    /// The full properties container of this graph (temporal + metadata).
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteProperties: a handle to the properties container.
    #[getter]
    pub fn properties(&self) -> PyRemoteProperties {
        PyRemoteProperties::new(self.graph.properties())
    }

    /// Fetch the graph's schema — node types, edge layers, and their
    /// observed property/metadata schemas. Fires one RPC and materializes
    /// the entire tree eagerly.
    ///
    /// Returns:
    ///   RemoteGraphSchema: the full schema descriptor.
    pub fn schema(&self) -> Result<PyRemoteGraphSchema, ClientError> {
        let graph = Arc::clone(&self.graph);
        let schema = execute_async_task(move || async move { graph.schema().await })?;
        Ok(schema.into())
    }

    /// Return the nodes that are common neighbours of the given ids
    /// (set intersection). Fires one RPC.
    ///
    /// Ids that don't exist in the current view are silently dropped
    /// server-side — the intersection is taken over the ids that do exist.
    /// So `shared_neighbours(["a", "z"])` where `"z"` is missing returns
    /// `a`'s neighbours (not an empty set).
    ///
    /// Arguments:
    ///     ids (list[str]): node ids to intersect neighbours of.
    ///
    /// Returns:
    ///     list[RemoteNode]: the shared neighbours. Empty if `ids` is empty
    ///         or none of the ids exist in the current view.
    pub fn shared_neighbours(&self, ids: Vec<String>) -> Result<Vec<PyRemoteNode>, ClientError> {
        let graph = Arc::clone(&self.graph);
        let nodes = execute_async_task(move || async move { graph.shared_neighbours(ids).await })?;
        Ok(nodes.into_iter().map(PyRemoteNode::new).collect())
    }

    /// Batch add node updates to the remote graph
    ///
    /// Arguments:
    ///     updates (List[RemoteNodeAddition]): The list of updates you want to apply to the remote graph
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (updates))]
    pub fn add_nodes(&self, updates: Vec<PyNodeAddition>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddNodes(AddNodesOp {
            path: self.graph.path.clone(),
            nodes: updates.into_iter().map(NodeAddition::from).collect(),
        }));
        let graph = Arc::clone(&self.graph);
        let task = move || async move { graph.transport.execute(&op).await };
        execute_async_task(task)?;
        Ok(())
    }

    /// Batch add edge updates to the remote graph
    ///
    /// Arguments:
    ///     updates (List[RemoteEdgeAddition]): The list of updates you want to apply to the remote graph
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (updates))]
    pub fn add_edges(&self, updates: Vec<PyEdgeAddition>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddEdges(AddEdgesOp {
            path: self.graph.path.clone(),
            edges: updates.into_iter().map(EdgeAddition::from).collect(),
        }));
        let graph = Arc::clone(&self.graph);
        let task = move || async move { graph.transport.execute(&op).await };
        execute_async_task(task)?;
        Ok(())
    }

    /// Adds a new node with the given id and properties to the remote graph.
    ///
    /// Arguments:
    ///     timestamp (int | str | datetime): The timestamp of the node.
    ///     id (str | int): The id of the node.
    ///     properties (dict, optional): The properties of the node.
    ///     node_type (str, optional): The optional string which will be used as a node type
    ///     layer (str, optional): The optional layer where the node update should be written
    ///
    /// Returns:
    ///     RemoteNode: the new remote node
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, layer = None))]
    pub fn add_node(
        &self,
        timestamp: EventTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<PyRemoteNode, ClientError> {
        let graph = Arc::clone(&self.graph);
        let node_type = node_type.map(|s| s.to_string());
        let layer = layer.map(|s| s.to_string());

        let node = execute_async_task(move || async move {
            graph
                .add_node(timestamp, id, properties, node_type, layer)
                .await
        })?;

        Ok(PyRemoteNode::new(node))
    }

    /// Create a new node with the given id and properties to the remote graph and fail if the node already exists.
    ///
    /// Arguments:
    ///     timestamp (int | str | datetime): The timestamp of the node.
    ///     id (str | int): The id of the node.
    ///     properties (dict, optional): The properties of the node.
    ///     node_type (str, optional): The optional string which will be used as a node type
    ///
    /// Returns:
    ///     RemoteNode: the new remote node
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn create_node(
        &self,
        timestamp: EventTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<PyRemoteNode, ClientError> {
        let graph = Arc::clone(&self.graph);
        let node_type = node_type.map(|s| s.to_string());

        let node = execute_async_task(move || async move {
            graph
                .create_node(timestamp, id, properties, node_type)
                .await
        })?;

        Ok(PyRemoteNode::new(node))
    }

    /// Adds properties to the remote graph.
    ///
    /// Arguments:
    ///     timestamp (int | str | datetime): The timestamp of the temporal property.
    ///     properties (dict): The temporal properties of the graph.
    ///
    /// Returns:
    ///     None:
    pub fn add_property(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.add_property(timestamp, properties).await })
    }

    /// Adds metadata to the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The metadata of the graph.
    ///
    /// Returns:
    ///     None:
    pub fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.add_metadata(properties).await })
    }

    /// Updates metadata on the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The metadata of the graph.
    ///
    /// Returns:
    ///     None:
    pub fn update_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.update_metadata(properties).await })
    }

    /// Adds a new edge with the given source and destination nodes and properties to the remote graph.
    ///
    /// Arguments:
    ///     timestamp (int | str | datetime): The timestamp of the edge.
    ///     src (str | int): The id of the source node.
    ///     dst (str | int): The id of the destination node.
    ///     properties (dict, optional): The properties of the edge, as a dict of string and properties.
    ///     layer (str, optional): The layer of the edge.
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None))]
    pub fn add_edge(
        &self,
        timestamp: EventTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, ClientError> {
        let graph = Arc::clone(&self.graph);
        let layer = layer.map(|s| s.to_string());

        let edge = execute_async_task(move || async move {
            graph.add_edge(timestamp, src, dst, properties, layer).await
        })?;

        Ok(PyRemoteEdge::new(edge))
    }

    /// Deletes an edge in the remote graph, given the timestamp, src and dst nodes and layer (optional)
    ///
    /// Arguments:
    ///     timestamp (int): The timestamp of the edge.
    ///     src (str | int): The id of the source node.
    ///     dst (str | int): The id of the destination node.
    ///     layer (str, optional): The layer of the edge.
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge
    #[pyo3(signature = (timestamp, src, dst, layer=None))]
    pub fn delete_edge(
        &self,
        timestamp: EventTime,
        src: GID,
        dst: GID,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, ClientError> {
        let graph = Arc::clone(&self.graph);
        let layer = layer.map(|s| s.to_string());

        let edge = execute_async_task(move || async move {
            graph.delete_edge(timestamp, src, dst, layer).await
        })?;

        Ok(PyRemoteEdge::new(edge))
    }
}
