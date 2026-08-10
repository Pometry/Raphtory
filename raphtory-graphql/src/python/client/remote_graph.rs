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
    utils::time::InputTime,
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
    ///     start (int | str | datetime): inclusive start of the window
    ///     end (int | str | datetime): exclusive end of the window
    ///
    /// Returns:
    ///     RemoteGraph: a new remote graph view restricted to the window
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteGraph {
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
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteGraph {
            graph: Arc::new(self.graph.filter(tree)?),
        })
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.layer(name)),
        }
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteGraph: a new view snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.at(time)),
        }
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.before(time)),
        }
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.after(time)),
        }
    }

    /// Restrict to the latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteGraph: a new view of the latest state.
    pub fn latest(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.latest()),
        }
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteGraph: a new view snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.snapshot_latest()),
        }
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteGraph: a new view snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.snapshot_at(time)),
        }
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_layer(name)),
        }
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_window(start, end)),
        }
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_start(start)),
        }
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.shrink_end(end)),
        }
    }

    /// Restrict to the "valid" subgraph (event-graph filter). Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to the valid subgraph.
    pub fn valid(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.valid()),
        }
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to the default layer.
    pub fn default_layer(&self) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.default_layer()),
        }
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.layers(names)),
        }
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_layers(names)),
        }
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.valid_layers(names)),
        }
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_valid_layer(name)),
        }
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_valid_layers(names)),
        }
    }

    /// Restrict to a subgraph induced by the given node ids. Lazy — no RPC.
    ///
    /// Arguments:
    ///     nodes (list[str]): the ids of the nodes to keep.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to the induced subgraph.
    pub fn subgraph(&self, nodes: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.subgraph(nodes)),
        }
    }

    /// Restrict to nodes matching one of the given node types. Lazy — no RPC.
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep.
    ///
    /// Returns:
    ///     RemoteGraph: a new view restricted to those node types.
    pub fn subgraph_node_types(&self, node_types: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.subgraph_node_types(node_types)),
        }
    }

    /// Exclude the given nodes from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     nodes (list[str]): the ids of the nodes to exclude.
    ///
    /// Returns:
    ///     RemoteGraph: a new view with those nodes excluded.
    pub fn exclude_nodes(&self, nodes: Vec<String>) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.graph.exclude_nodes(nodes)),
        }
    }

    /// Terminal: total node count under the current view. Fires one RPC.
    ///
    /// Returns:
    ///     int: the number of nodes.
    pub fn count_nodes(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_nodes().await })
    }

    /// Terminal: total edge count under the current view. Fires one RPC.
    ///
    /// Returns:
    ///     int: the number of edges.
    pub fn count_edges(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_edges().await })
    }

    /// Earliest event time under the current view. `None` if the view has no
    /// events. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the earliest event time, or `None` if the view has no
    ///         events.
    #[getter]
    pub fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.earliest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// Latest event time under the current view. Property — fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the latest event time, or `None` if the view has no events.
    #[getter]
    pub fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.latest_time().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View start bound. `None` for an unbounded view. Property — fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the view start bound, or `None` if unbounded.
    #[getter]
    pub fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.start().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// View end bound. `None` for an unbounded view. Property — fires one RPC.
    ///
    /// Returns:
    ///     Optional[EventTime]: the view end bound, or `None` if unbounded.
    #[getter]
    pub fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let graph = Arc::clone(&self.graph);
        Ok(
            execute_async_task(move || async move { graph.end().await })?
                .and_then(|t| t.to_event_time()),
        )
    }

    /// Terminal: graph creation timestamp. Fires one RPC.
    ///
    /// Returns:
    ///     int: the graph's creation timestamp.
    pub fn created(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.created().await })
    }

    /// Terminal: last time this graph was opened. Fires one RPC.
    ///
    /// Returns:
    ///     int: the timestamp the graph was last opened at.
    pub fn last_opened(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.last_opened().await })
    }

    /// Terminal: last time this graph was updated. Fires one RPC.
    ///
    /// Returns:
    ///     int: the timestamp the graph was last updated at.
    pub fn last_updated(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.last_updated().await })
    }

    /// List of unique layer names present in this graph. Property — fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the unique layer names.
    #[getter]
    pub fn unique_layers(&self) -> Result<Vec<String>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.unique_layers().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///   name (str): the name of the layer to check.
    ///
    /// Returns:
    ///   bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let graph = Arc::clone(&self.graph);
        let name = name.to_string();
        execute_async_task(move || async move { graph.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.window_size().await })
    }

    /// Terminal: earliest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the earliest edge event time, or `None` if the view has no edge
    ///         events.
    pub fn earliest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.earliest_edge_time().await })
    }

    /// Terminal: latest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the latest edge event time, or `None` if the view has no edge
    ///         events.
    pub fn latest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.latest_edge_time().await })
    }

    /// Terminal: does the graph have a node with this id? Fires one RPC.
    ///
    /// Arguments:
    ///     id (str | int): the id of the node to check.
    ///
    /// Returns:
    ///     bool: True if the node is present.
    pub fn has_node(&self, id: GID) -> Result<bool, ClientError> {
        let graph = Arc::clone(&self.graph);
        let id_str = id.to_string();
        execute_async_task(move || async move { graph.has_node(id_str).await })
    }

    /// Terminal: does the graph have an edge `(src, dst)`? Fires one RPC.
    ///
    /// Arguments:
    ///     src (str | int): the id of the source node.
    ///     dst (str | int): the id of the destination node.
    ///
    /// Returns:
    ///     bool: True if the edge is present.
    #[pyo3(signature = (src, dst))]
    pub fn has_edge(&self, src: GID, dst: GID) -> Result<bool, ClientError> {
        let graph = Arc::clone(&self.graph);
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        execute_async_task(move || async move { graph.has_edge(src_str, dst_str).await })
    }

    /// Terminal: total temporal-edge count (edge updates) under the current
    /// view. Fires one RPC.
    ///
    /// Returns:
    ///     int: the number of edge updates.
    pub fn count_temporal_edges(&self) -> Result<i64, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.count_temporal_edges().await })
    }

    /// Terminal: the graph's name. Fires one RPC.
    ///
    /// Returns:
    ///     str: the graph's name.
    pub fn name(&self) -> Result<String, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.name().await })
    }

    /// Terminal: the graph's full path. Fires one RPC.
    ///
    /// Returns:
    ///     str: the graph's full path.
    pub fn path(&self) -> Result<String, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.path().await })
    }

    /// Terminal: the parent namespace of the graph path. Fires one RPC.
    ///
    /// Returns:
    ///     str: the parent namespace of the graph path.
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
    ///
    /// Arguments:
    ///     id (str | int): the node id
    ///
    /// Returns:
    ///     Optional[RemoteNode]: the remote node, or `None` if it isn't visible
    ///         under the current view.
    pub fn node(&self, id: GID) -> Result<Option<PyRemoteNode>, ClientError> {
        let graph = Arc::clone(&self.graph);
        let id_str = id.to_string();
        let node = execute_async_task(move || async move { graph.node(id_str).await })?;
        Ok(node.map(PyRemoteNode::new))
    }

    /// Gets a remote edge with the specified source and destination nodes.
    ///
    /// Fires one RPC — a `hasEdge` check against the current view chain.
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     Optional[RemoteEdge]: the remote edge, or `None` if it isn't visible
    ///         under the current view.
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: GID, dst: GID) -> Result<Option<PyRemoteEdge>, ClientError> {
        let graph = Arc::clone(&self.graph);
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let edge = execute_async_task(move || async move { graph.edge(src_str, dst_str).await })?;
        Ok(edge.map(PyRemoteEdge::new))
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

    /// Get the nodes whose latest value matches every property in
    /// `properties_dict`. Mirrors the local `Graph.find_nodes`. Fires one RPC.
    ///
    /// Arguments:
    ///     properties_dict (dict[str, PropValue]): the property names and values
    ///         a node must match.
    ///
    /// Returns:
    ///     list[RemoteNode]: the nodes that match all the given properties.
    pub fn find_nodes(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<PyRemoteNode>, ClientError> {
        let graph = Arc::clone(&self.graph);
        let nodes =
            execute_async_task(move || async move { graph.find_nodes(properties_dict).await })?;
        Ok(nodes.into_iter().map(PyRemoteNode::new).collect())
    }

    /// Get the edges whose latest value matches every property in
    /// `properties_dict`. Mirrors the local `Graph.find_edges`. Fires one RPC.
    ///
    /// Arguments:
    ///     properties_dict (dict[str, PropValue]): the property names and values
    ///         an edge must match.
    ///
    /// Returns:
    ///     list[RemoteEdge]: the edges that match all the given properties.
    pub fn find_edges(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<PyRemoteEdge>, ClientError> {
        let graph = Arc::clone(&self.graph);
        let edges =
            execute_async_task(move || async move { graph.find_edges(properties_dict).await })?;
        Ok(edges.into_iter().map(PyRemoteEdge::new).collect())
    }

    /// Returns all the node types present in the graph. Mirrors the local
    /// `Graph.get_all_node_types`. Fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the node types.
    pub fn get_all_node_types(&self) -> Result<Vec<String>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.get_all_node_types().await })
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
    ///     event_id (int, optional): Secondary index to disambiguate multiple
    ///         updates at the same timestamp. If omitted, the server auto-increments it.
    ///     layer (str, optional): The optional layer where the node update should be written
    ///
    /// Returns:
    ///     RemoteNode: the new remote node
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, event_id = None, layer = None))]
    pub fn add_node(
        &self,
        timestamp: EventTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        event_id: Option<usize>,
        layer: Option<&str>,
    ) -> Result<PyRemoteNode, ClientError> {
        let graph = Arc::clone(&self.graph);
        let node_type = node_type.map(|s| s.to_string());
        let layer = layer.map(|s| s.to_string());

        let node = execute_async_task(move || async move {
            graph
                .add_node(timestamp, id, properties, node_type, layer, event_id)
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
    ///     event_id (int, optional): Secondary index to disambiguate multiple
    ///         updates at the same timestamp. If omitted, the server auto-increments it.
    ///     layer (str, optional): The optional layer where the node update should be written
    ///
    /// Returns:
    ///     RemoteNode: the new remote node
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None, event_id = None, layer = None))]
    pub fn create_node(
        &self,
        timestamp: EventTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
        event_id: Option<usize>,
        layer: Option<&str>,
    ) -> Result<PyRemoteNode, ClientError> {
        let graph = Arc::clone(&self.graph);
        let node_type = node_type.map(|s| s.to_string());
        let layer = layer.map(|s| s.to_string());

        let node = execute_async_task(move || async move {
            graph
                .create_node(timestamp, id, properties, node_type, layer, event_id)
                .await
        })?;

        Ok(PyRemoteNode::new(node))
    }

    /// Adds temporal properties to the remote graph.
    ///
    /// Arguments:
    ///     timestamp (int | str | datetime): The timestamp of the temporal property.
    ///     properties (dict): The temporal properties of the graph.
    ///     event_id (int, optional): Secondary index to disambiguate multiple
    ///         updates at the same timestamp. If omitted, the server
    ///         auto-increments it.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (timestamp, properties, event_id = None))]
    pub fn add_properties(
        &self,
        timestamp: EventTime,
        properties: HashMap<String, Prop>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move {
            graph.add_properties(timestamp, properties, event_id).await
        })
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
    ///     event_id (int, optional): Secondary index to disambiguate multiple
    ///         updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None, event_id = None))]
    pub fn add_edge(
        &self,
        timestamp: EventTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<PyRemoteEdge, ClientError> {
        let graph = Arc::clone(&self.graph);
        let layer = layer.map(|s| s.to_string());

        let edge = execute_async_task(move || async move {
            graph
                .add_edge(timestamp, src, dst, properties, layer, event_id)
                .await
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
    ///     event_id (int, optional): Secondary index to disambiguate multiple
    ///         updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge
    #[pyo3(signature = (timestamp, src, dst, layer=None, event_id=None))]
    pub fn delete_edge(
        &self,
        timestamp: EventTime,
        src: GID,
        dst: GID,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<PyRemoteEdge, ClientError> {
        let graph = Arc::clone(&self.graph);
        let layer = layer.map(|s| s.to_string());

        let edge = execute_async_task(move || async move {
            graph
                .delete_edge(timestamp, src, dst, layer, event_id)
                .await
        })?;

        Ok(PyRemoteEdge::new(edge))
    }
}
