use crate::{
    client::{
        remote_edge::GraphQLRemoteEdge, remote_graph::GraphQLRemoteGraph,
        remote_node::GraphQLRemoteNode,
    },
    python::client::{
        build_query, remote_edge::PyRemoteEdge, remote_node::PyRemoteNode, PyEdgeAddition,
        PyNodeAddition,
    },
};
use minijinja::context;
use pyo3::{pyclass, pymethods, Python};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::EventTime,
};
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::runtime::Runtime;

#[derive(Clone)]
#[pyclass(name = "RemoteGraph", module = "raphtory.graphql")]
pub struct PyRemoteGraph {
    pub(crate) graph: GraphQLRemoteGraph,
    pub(crate) runtime: Arc<Runtime>,
}

impl PyRemoteGraph {
    fn execute_async_task<T, F, O>(&self, task: T) -> O
    where
        T: FnOnce() -> F + Send + 'static,
        F: Future<Output = O> + 'static,
        O: Send + 'static,
    {
        Python::attach(|py| py.detach(|| self.runtime.block_on(task())))
    }
}

#[pymethods]
impl PyRemoteGraph {
    /// Gets a remote node with the specified id
    ///
    /// Arguments:
    ///     id (str | int): the node id
    ///
    /// Returns:
    ///     RemoteNode: the remote node reference
    pub fn node(&self, id: GID) -> PyRemoteNode {
        let node = GraphQLRemoteNode::new(
            self.graph.path.clone(),
            self.graph.client.clone(),
            id.to_string(),
        );
        PyRemoteNode::new(node, self.runtime.clone())
    }

    /// Gets a remote edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str | int): the source node id
    ///     dst (str | int): the destination node id
    ///
    /// Returns:
    ///     RemoteEdge: the remote edge reference
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: GID, dst: GID) -> PyRemoteEdge {
        let edge = GraphQLRemoteEdge::new(
            self.graph.path.clone(),
            self.graph.client.clone(),
            src.to_string(),
            dst.to_string(),
        );
        PyRemoteEdge::new(edge, self.runtime.clone())
    }

    /// Batch add node updates to the remote graph
    ///
    /// Arguments:
    ///     updates (List[RemoteNodeAddition]): The list of updates you want to apply to the remote graph
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (updates))]
    pub fn add_nodes(&self, _py: Python, updates: Vec<PyNodeAddition>) -> Result<(), GraphError> {
        let template = r#"
        {
        updateGraph(path: "{{ path }}") {
            addNodes(
                nodes: [
                    {% for node in nodes %}
                    {
                        name: "{{ node.name }}"
                        {% if node.node_type%}, nodeType: "{{ node.node_type }}"{% endif %}
                        {% if node.updates%},
                        updates: [
                            {% for tprop in node.updates %}
                            {
                                time: {{ tprop.time[0] }},
                                {% if tprop.properties%}
                                properties: [
                                    {% for prop in tprop.properties%}
                                    {
                                        key: "{{ prop.key }}",
                                        value:{{prop.value | safe}}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                        {% endif %}
                        {% if node.metadata%},
                        metadata: [
                            {% for cprop in node.metadata %}
                            {
                                key: "{{ cprop.key }}",
                                value:{{ cprop.value }}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                        {% endif %}
                    }
                    {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            )
        }
    }
        "#;

        let query_context = context! {
            path => self.graph.path.clone(),
            nodes => updates
        };

        let query = build_query(template, query_context)?;
        let task = {
            let graph = self.graph.clone();
            move || async move { graph.client.query_async(&query, HashMap::new()).await }
        };
        self.execute_async_task(task).map_err(GraphError::from)?;

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
    pub fn add_edges(&self, _py: Python, updates: Vec<PyEdgeAddition>) -> Result<(), GraphError> {
        let template = r#"
                {
                updateGraph(path: "{{ path }}") {
                    addEdges(
                        edges: [
                            {% for edge in edges %}
                            {
                                src: "{{ edge.src }}"
                                dst: "{{ edge.dst }}"
                                {% if edge.layer%}, layer: "{{ edge.layer }}"{% endif %}
                                {% if edge.updates%},
                                updates: [
                                    {% for tprop in edge.updates %}
                                    {
                                        time: {{ tprop.time[0] }},
                                        {% if tprop.properties%}
                                        properties: [
                                            {% for prop in tprop.properties%}
                                            {
                                                key: "{{ prop.key }}",
                                                value:{{prop.value | safe}}
                                            }
                                            {% if not loop.last %},{% endif %}
                                            {% endfor %}
                                        ]
                                        {% endif %}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                                {% if edge.metadata%},
                                metadata: [
                                    {% for cprop in edge.metadata %}
                                    {
                                        key: "{{ cprop.key }}",
                                        value:{{ cprop.value }}
                                    }
                                    {% if not loop.last %},{% endif %}
                                    {% endfor %}
                                ]
                                {% endif %}
                            }
                            {% if not loop.last %},{% endif %}
                            {% endfor %}
                        ]
                    )
                }
            }
        "#;

        let query_context = context! {
            path => self.graph.path.clone(),
            edges => updates,
        };

        let query = build_query(template, query_context)?;
        let task = {
            let graph = self.graph.clone();
            move || async move { graph.client.query_async(&query, HashMap::new()).await }
        };
        self.execute_async_task(task).map_err(GraphError::from)?;

        Ok(())
    }

    /// Adds a new node with the given id and properties to the remote graph.
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
    pub fn add_node(
        &self,
        timestamp: EventTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<PyRemoteNode, GraphError> {
        let graph = self.graph.clone();
        let id_str = id.to_string();
        let node_type = node_type.map(|s| s.to_string());

        let task =
            move || async move { graph.add_node(timestamp, id, properties, node_type).await };
        self.execute_async_task(task).map_err(GraphError::from)?;

        let node =
            GraphQLRemoteNode::new(self.graph.path.clone(), self.graph.client.clone(), id_str);
        Ok(PyRemoteNode::new(node, self.runtime.clone()))
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
    ) -> Result<PyRemoteNode, GraphError> {
        let graph = self.graph.clone();
        let id_str = id.to_string();
        let node_type = node_type.map(|s| s.to_string());

        let task = move || async move {
            graph
                .create_node(timestamp, id, properties, node_type)
                .await
        };
        self.execute_async_task(task).map_err(GraphError::from)?;

        let node =
            GraphQLRemoteNode::new(self.graph.path.clone(), self.graph.client.clone(), id_str);
        Ok(PyRemoteNode::new(node, self.runtime.clone()))
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
    ) -> Result<(), GraphError> {
        let graph = self.graph.clone();
        let task = move || async move { graph.add_property(timestamp, properties).await };
        self.execute_async_task(task).map_err(GraphError::from)?;

        Ok(())
    }

    /// Adds metadata to the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The metadata of the graph.
    ///
    /// Returns:
    ///     None:
    pub fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        let graph = self.graph.clone();
        let task = move || async move { graph.add_metadata(properties).await };
        self.execute_async_task(task).map_err(GraphError::from)?;

        Ok(())
    }

    /// Updates metadata on the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The metadata of the graph.
    ///
    /// Returns:
    ///     None:
    pub fn update_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        let graph = self.graph.clone();
        let task = move || async move { graph.update_metadata(properties).await };
        self.execute_async_task(task).map_err(GraphError::from)?;

        Ok(())
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
    ) -> Result<PyRemoteEdge, GraphError> {
        let graph = self.graph.clone();
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let layer = layer.map(|s| s.to_string());

        let task =
            move || async move { graph.add_edge(timestamp, src, dst, properties, layer).await };
        self.execute_async_task(task).map_err(GraphError::from)?;
        let edge = GraphQLRemoteEdge::new(
            self.graph.path.clone(),
            self.graph.client.clone(),
            src_str,
            dst_str,
        );
        Ok(PyRemoteEdge::new(edge, self.runtime.clone()))
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
    ) -> Result<PyRemoteEdge, GraphError> {
        let graph = self.graph.clone();
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let layer = layer.map(|s| s.to_string());

        let task = move || async move { graph.delete_edge(timestamp, src, dst, layer).await };
        self.execute_async_task(task).map_err(GraphError::from)?;

        let edge = GraphQLRemoteEdge::new(
            self.graph.path.clone(),
            self.graph.client.clone(),
            src_str,
            dst_str,
        );
        Ok(PyRemoteEdge::new(edge, self.runtime.clone()))
    }
}
