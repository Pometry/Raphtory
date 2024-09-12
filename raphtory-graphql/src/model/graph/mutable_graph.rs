use crate::model::graph::{edge::Edge, graph::GqlGraph, node::Node, property::GqlPropValue};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::view::MaterializedGraph,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    search::IndexedGraph,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use std::path::PathBuf;

#[derive(InputObject)]
pub struct GqlPropInput {
    key: String,
    value: GqlPropValue,
}

#[derive(InputObject)]
pub struct TPropInput {
    time: i64,
    properties: Option<Vec<GqlPropInput>>,
}

#[derive(InputObject)]
pub struct NodeAddition {
    name: String,
    node_type: Option<String>,
    constant_properties: Option<Vec<GqlPropInput>>,
    updates: Option<Vec<TPropInput>>,
}

#[derive(InputObject)]
pub struct EdgeAddition {
    src: String,
    dst: String,
    layer: Option<String>,
    constant_properties: Option<Vec<GqlPropInput>>,
    updates: Option<Vec<TPropInput>>,
}

#[derive(ResolvedObject)]
pub struct GqlMutableGraph {
    path: PathBuf,
    graph: IndexedGraph<MaterializedGraph>,
}

impl GqlMutableGraph {
    pub(crate) fn new(path: impl Into<PathBuf>, graph: IndexedGraph<MaterializedGraph>) -> Self {
        Self {
            path: path.into(),
            graph,
        }
    }
}

fn as_properties(properties: Vec<GqlPropInput>) -> impl Iterator<Item = (String, Prop)> {
    properties.into_iter().map(|p| (p.key, p.value.0))
}

#[ResolvedObjectFields]
impl GqlMutableGraph {
    /// Get the non-mutable graph

    async fn graph(&self) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.clone())
    }

    /// Get mutable existing node

    async fn node(&self, name: String) -> Option<GqlMutableNode> {
        self.graph.node(name).map(|n| n.into())
    }

    /// Add a new node or add updates to an existing node
    async fn add_node(
        &self,
        time: i64,
        name: String,
        properties: Option<Vec<GqlPropInput>>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let node = self.graph.add_node(
            time,
            name,
            as_properties(properties.unwrap_or(vec![])),
            node_type.as_str(),
        )?;
        self.graph.write_updates()?;
        Ok(node.into())
    }

    /// Add a batch of nodes
    async fn add_nodes(&self, nodes: Vec<NodeAddition>) -> Result<bool, GraphError> {
        for node in nodes {
            let name = node.name.as_str();

            for prop in node.updates.unwrap_or(vec![]) {
                self.graph.add_node(
                    prop.time,
                    name,
                    as_properties(prop.properties.unwrap_or(vec![])),
                    None,
                )?;
            }
            if let Some(node_type) = node.node_type.as_str() {
                let node_view = self
                    .graph
                    .node(name)
                    .ok_or(GraphError::NodeNameError(node.name.clone()))?;
                node_view.set_node_type(node_type)?;
            }
            let constant_props = node.constant_properties.unwrap_or(vec![]);
            if !constant_props.is_empty() {
                let node_view = self
                    .graph
                    .node(name)
                    .ok_or(GraphError::NodeNameError(node.name))?;
                node_view.add_constant_properties(as_properties(constant_props))?;
            }
        }
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Get a mutable existing edge

    async fn edge(&self, src: String, dst: String) -> Option<GqlMutableEdge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    /// Add a new edge or add updates to an existing edge
    async fn add_edge(
        &self,
        time: i64,
        src: String,
        dst: String,
        properties: Option<Vec<GqlPropInput>>,
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let edge = self.graph.add_edge(
            time,
            src,
            dst,
            as_properties(properties.unwrap_or(vec![])),
            layer.as_str(),
        )?;
        self.graph.write_updates()?;
        Ok(edge.into())
    }

    /// Add a batch of edges
    async fn add_edges(&self, edges: Vec<EdgeAddition>) -> Result<bool, GraphError> {
        for edge in edges {
            let src = edge.src.as_str();
            let dst = edge.dst.as_str();
            let layer = edge.layer.as_str();
            for prop in edge.updates.unwrap_or(vec![]) {
                self.graph.add_edge(
                    prop.time,
                    src,
                    dst,
                    as_properties(prop.properties.unwrap_or(vec![])),
                    layer,
                )?;
            }
            let constant_props = edge.constant_properties.unwrap_or(vec![]);
            if !constant_props.is_empty() {
                let edge_view = self.graph.edge(src, dst).ok_or(GraphError::EdgeNameError {
                    src: edge.src,
                    dst: edge.dst,
                })?;
                edge_view.add_constant_properties(as_properties(constant_props), layer)?;
            }
        }
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Mark an edge as deleted (creates the edge if it did not exist)

    async fn delete_edge(
        &self,
        time: i64,
        src: String,
        dst: String,
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let edge = self.graph.delete_edge(time, src, dst, layer.as_str())?;
        self.graph.write_updates()?;
        Ok(edge.into())
    }

    /// Add temporal properties to graph
    async fn add_properties(
        &self,
        t: i64,
        properties: Vec<GqlPropInput>,
    ) -> Result<bool, GraphError> {
        self.graph.add_properties(t, as_properties(properties))?;
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Add constant properties to graph (errors if the property already exists)
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
    ) -> Result<bool, GraphError> {
        self.graph
            .add_constant_properties(as_properties(properties))?;
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the graph (overwrites existing values)
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
    ) -> Result<bool, GraphError> {
        self.graph
            .update_constant_properties(as_properties(properties))?;
        self.graph.write_updates()?;
        Ok(true)
    }
}

#[derive(ResolvedObject)]
pub struct GqlMutableNode {
    node: NodeView<IndexedGraph<MaterializedGraph>>,
}

impl From<NodeView<IndexedGraph<MaterializedGraph>>> for GqlMutableNode {
    fn from(node: NodeView<IndexedGraph<MaterializedGraph>>) -> Self {
        Self { node }
    }
}

#[ResolvedObjectFields]
impl GqlMutableNode {
    /// Use to check if adding the node was successful
    async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable `Node`
    async fn node(&self) -> Node {
        self.node.clone().into()
    }

    /// Add constant properties to the node (errors if the property already exists)
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
    ) -> Result<bool, GraphError> {
        self.node
            .add_constant_properties(as_properties(properties))?;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Set the node type (errors if the node already has a non-default type)
    async fn set_node_type(&self, new_type: String) -> Result<bool, GraphError> {
        self.node.set_node_type(&new_type)?;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the node (overwrites existing property values)
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
    ) -> Result<bool, GraphError> {
        self.node
            .update_constant_properties(as_properties(properties))?;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Add temporal property updates to the node
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropInput>>,
    ) -> Result<bool, GraphError> {
        self.node
            .add_updates(time, as_properties(properties.unwrap_or(vec![])))?;
        self.node.graph.write_updates()?;
        Ok(true)
    }
}

#[derive(ResolvedObject)]
pub struct GqlMutableEdge {
    edge: EdgeView<IndexedGraph<MaterializedGraph>>,
}

impl From<EdgeView<IndexedGraph<MaterializedGraph>>> for GqlMutableEdge {
    fn from(edge: EdgeView<IndexedGraph<MaterializedGraph>>) -> Self {
        Self { edge }
    }
}

#[ResolvedObjectFields]
impl GqlMutableEdge {
    /// Use to check if adding the edge was successful
    async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable edge for querying
    async fn edge(&self) -> Edge {
        self.edge.clone().into()
    }

    /// Get the mutable source node of the edge
    async fn src(&self) -> GqlMutableNode {
        self.edge.src().into()
    }

    /// Get the mutable destination node of the edge
    async fn dst(&self) -> GqlMutableNode {
        self.edge.dst().into()
    }

    /// Mark the edge as deleted at time `time`
    async fn delete(&self, time: i64, layer: Option<String>) -> Result<bool, GraphError> {
        self.edge.delete(time, layer.as_str())?;
        self.edge.graph.write_updates()?;
        Ok(true)
    }

    /// Add constant properties to the edge (errors if the value already exists)
    ///
    /// If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
    /// need to be specified again.
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge
            .add_constant_properties(as_properties(properties), layer.as_str())?;
        self.edge.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the edge (existing values are overwritten)
    ///
    /// If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
    /// need to be specified again.
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge
            .update_constant_properties(as_properties(properties), layer.as_str())?;
        self.edge.graph.write_updates()?;
        Ok(true)
    }

    /// Add temporal property updates to the edge
    ///
    /// If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
    /// need to be specified again.
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropInput>>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge.add_updates(
            time,
            as_properties(properties.unwrap_or(vec![])),
            layer.as_str(),
        )?;
        self.edge.graph.write_updates()?;
        Ok(true)
    }
}
