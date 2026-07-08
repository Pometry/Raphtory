use crate::{
    client::{
        op::{
            AddEdges as AddEdgesOp, AddNodes as AddNodesOp, EdgeAddition, NodeAddition, Op,
            TemporalUpdate, WriteOp,
        },
        remote_graph::RemoteGraph,
        transport::Transport,
        ClientError,
    },
    python::client::{
        remote_edge::PyRemoteEdge, remote_node::PyRemoteNode, PyEdgeAddition, PyNodeAddition,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory::python::utils::execute_async_task;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
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

    /// Terminal: earliest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.earliest_time().await })
    }

    /// Terminal: latest event timestamp under the current view. Fires one RPC.
    pub fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.latest_time().await })
    }

    /// Terminal: view start bound. `None` for an unbounded view. Fires one RPC.
    pub fn start(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.start().await })
    }

    /// Terminal: view end bound. `None` for an unbounded view. Fires one RPC.
    pub fn end(&self) -> Result<Option<i64>, ClientError> {
        let graph = Arc::clone(&self.graph);
        execute_async_task(move || async move { graph.end().await })
    }

    /// Gets a remote node with the specified id.
    ///
    /// Inherits any view chain built up on the parent `RemoteGraph` (e.g. after
    /// `rg.window(...)`) so subsequent terminals like `degree()` evaluate under
    /// the same view context.
    ///
    /// Arguments:
    ///     id (str | int): the node id
    ///
    /// Returns:
    ///     RemoteNode: the remote node reference
    pub fn node(&self, id: GID) -> PyRemoteNode {
        PyRemoteNode::new(self.graph.node(id.to_string()))
    }

    /// Gets a remote edge with the specified source and destination nodes.
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge reference
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: GID, dst: GID) -> PyRemoteEdge {
        PyRemoteEdge::new(self.graph.edge(src.to_string(), dst.to_string()))
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
