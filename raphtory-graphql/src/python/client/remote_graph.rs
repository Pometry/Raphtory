use crate::python::client::{
    build_property_string, build_query, raphtory_client::PyRaphtoryClient,
    remote_edge::PyRemoteEdge, remote_node::PyRemoteNode, PyEdgeAddition, PyNodeAddition,
};
use minijinja::context;
use pyo3::{pyclass, pymethods, Python};
use raphtory::{
    core::{
        utils::{errors::GraphError, time::IntoTime},
        Prop,
    },
    python::utils::PyTime,
};
use raphtory_api::core::entities::GID;
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteGraph")]
pub struct PyRemoteGraph {
    pub(crate) path: String,
    pub(crate) client: PyRaphtoryClient,
}

#[pymethods]
impl PyRemoteGraph {
    #[new]
    pub(crate) fn new(path: String, client: PyRaphtoryClient) -> Self {
        Self { path, client }
    }

    /// Gets a remote node with the specified id
    ///
    /// Arguments:
    ///   id (str or int): the node id
    ///
    /// Returns:
    ///   RemoteNode
    pub fn node(&self, id: GID) -> PyRemoteNode {
        PyRemoteNode::new(self.path.clone(), self.client.clone(), id.to_string())
    }

    /// Gets a remote edge with the specified source and destination nodes
    ///
    /// Arguments:
    ///     src (str or int): the source node id
    ///     dst (str or int): the destination node id
    ///
    /// Returns:
    ///     RemoteEdge
    #[pyo3(signature = (src, dst))]
    pub fn edge(&self, src: GID, dst: GID) -> PyRemoteEdge {
        PyRemoteEdge::new(
            self.path.clone(),
            self.client.clone(),
            src.to_string(),
            dst.to_string(),
        )
    }

    /// Batch add node updates to the remote graph
    ///
    /// Arguments:
    ///   updates (List[RemoteNodeAddition]): The list of updates you want to apply to the remote graph
    /// Returns:
    ///   None
    #[pyo3(signature = (updates))]
    pub fn add_nodes(&self, py: Python, updates: Vec<PyNodeAddition>) -> Result<(), GraphError> {
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
                                time: {{ tprop.time }},
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
                        {% if node.constant_properties%},
                        constantProperties: [
                            {% for cprop in node.constant_properties %}
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
            path => self.path,
            nodes => updates
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Batch add edge updates to the remote graph
    ///
    /// Arguments:
    ///   updates (List[RemoteEdgeAddition]): The list of updates you want to apply to the remote graph
    /// Returns:
    ///   None
    #[pyo3(signature = (updates))]
    pub fn add_edges(&self, py: Python, updates: Vec<PyEdgeAddition>) -> Result<(), GraphError> {
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
                                        time: {{ tprop.time }},
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
                                {% if edge.constant_properties%},
                                constantProperties: [
                                    {% for cprop in edge.constant_properties %}
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
            path => self.path,
            edges => updates,
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Adds a new node with the given id and properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the node.
    ///    id (str or int): The id of the node.
    ///    properties (dict): The properties of the node (optional).
    ///    node_type (str): The optional string which will be used as a node type
    /// Returns:
    ///   the added node (RemoteNode)
    ///
    #[pyo3(signature = (timestamp, id, properties = None, node_type = None))]
    pub fn add_node(
        &self,
        py: Python,
        timestamp: PyTime,
        id: GID,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<&str>,
    ) -> Result<PyRemoteNode, GraphError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addNode(time: {{ time }}, name: "{{ name }}" {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %}{% if node_type is not none %}, nodeType: "{{ node_type }}"{% endif %}) {
                    success
                }
            }
        }
        "#;

        let query_context = context! {
            path => self.path,
            time => timestamp.into_time(),
            name => id.to_string(),
            properties => properties.map(|p| build_property_string(p)),
            node_type => node_type
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(PyRemoteNode::new(
            self.path.clone(),
            self.client.clone(),
            id.to_string(),
        ))
    }

    /// Adds properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the temporal property.
    ///    properties (dict): The temporal properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_property(
        &self,
        py: Python,
        timestamp: PyTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addProperties(t: {{t}} properties: {{ properties | safe }})
          }
        }
        "#;
        let query_context = context! {
            path => self.path,
            t => timestamp.into_time(),
            properties => build_property_string(properties),
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Adds static properties to the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The static properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn add_constant_properties(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            addConstantProperties(properties: {{ properties | safe }})
          }
        }
        "#;

        let query_context = context! {
            path => self.path,
            properties => build_property_string(properties),
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Updates static properties on the remote graph.
    ///
    /// Arguments:
    ///     properties (dict): The static properties of the graph.
    ///
    /// Returns:
    ///    None
    pub fn update_constant_properties(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        let template = r#"
        {
          updateGraph(path: "{{ path }}") {
            updateConstantProperties(properties: {{ properties | safe }})
          }
        }
        "#;

        let query_context = context! {
            path => self.path,
            properties => build_property_string(properties),
        };

        let query = build_query(template, query_context)?;

        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Adds a new edge with the given source and destination nodes and properties to the remote graph.
    ///
    /// Arguments:
    ///    timestamp (int, str, or datetime(utc)): The timestamp of the edge.
    ///    src (str or int): The id of the source node.
    ///    dst (str or int): The id of the destination node.
    ///    properties (dict): The properties of the edge, as a dict of string and properties (optional).
    ///    layer (str): The layer of the edge (optional).
    ///
    /// Returns:
    ///   The added edge (RemoteEdge)
    #[pyo3(signature = (timestamp, src, dst, properties = None, layer = None))]
    pub fn add_edge(
        &self,
        py: Python,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, GraphError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                addEdge(time: {{ time }}, src: "{{ src }}", dst: "{{ dst }}" {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %}{% if layer is not none %}, layer: "{{ layer }}"{% endif %}) {
                    success
                }
            }
        }
        "#;

        let query_context = context! {
            path => self.path,
            time => timestamp.into_time(),
            src => src.to_string(),
            dst => dst.to_string(),
            properties => properties.map(|p| build_property_string(p)),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;
        Ok(PyRemoteEdge::new(
            self.path.clone(),
            self.client.clone(),
            src.to_string(),
            dst.to_string(),
        ))
    }

    /// Deletes an edge in the remote graph, given the timestamp, src and dst nodes and layer (optional)
    ///
    /// Arguments:
    ///   timestamp (int): The timestamp of the edge.
    ///   src (str or int): The id of the source node.
    ///   dst (str or int): The id of the destination node.
    ///   layer (str): The layer of the edge. (optional)
    ///
    /// Returns:
    ///  The deleted edge (RemoteEdge)
    pub fn delete_edge(
        &self,
        py: Python,
        timestamp: PyTime,
        src: GID,
        dst: GID,
        layer: Option<&str>,
    ) -> Result<PyRemoteEdge, GraphError> {
        let template = r#"
        {
            updateGraph(path: "{{ path }}") {
                deleteEdge(time: {{ time }}, src: "{{ src }}", dst: "{{ dst }}" {% if layer is not none %}, layer: "{{ layer }}"{% endif %}) {
                    success
                }
            }
        }
        "#;

        let query_context = context! {
            path => self.path,
            time => timestamp.into_time(),
            src => src.to_string(),
            dst => dst.to_string(),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;
        Ok(PyRemoteEdge::new(
            self.path.clone(),
            self.client.clone(),
            src.to_string(),
            dst.to_string(),
        ))
    }
}
