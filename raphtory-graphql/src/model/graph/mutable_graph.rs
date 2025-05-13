use crate::{
    graph::{GraphWithVectors, UpdateEmbeddings},
    model::graph::{edge::GqlEdge, graph::GqlGraph, node::GqlNode, property::Value},
    paths::ExistingGraphFolder,
};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::GraphError,
    db::graph::{edge::EdgeView, node::NodeView},
    prelude::*,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use tokio::{spawn, task::spawn_blocking};

#[derive(InputObject)]
#[graphql(name = "PropertyInput")]
pub struct GqlPropertyInput {
    key: String,
    value: Value,
}

#[derive(InputObject)]
pub struct TemporalPropertyInput {
    time: i64,
    properties: Option<Vec<GqlPropertyInput>>,
}

#[derive(InputObject)]
pub struct NodeAddition {
    name: String,
    node_type: Option<String>,
    constant_properties: Option<Vec<GqlPropertyInput>>,
    updates: Option<Vec<TemporalPropertyInput>>,
}

#[derive(InputObject)]
pub struct EdgeAddition {
    src: String,
    dst: String,
    layer: Option<String>,
    constant_properties: Option<Vec<GqlPropertyInput>>,
    updates: Option<Vec<TemporalPropertyInput>>,
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "MutableGraph")]
pub struct GqlMutableGraph {
    path: ExistingGraphFolder,
    graph: GraphWithVectors,
}

impl GqlMutableGraph {
    pub(crate) fn new(path: ExistingGraphFolder, graph: GraphWithVectors) -> Self {
        Self {
            path: path.into(),
            graph,
        }
    }
}

fn as_properties(
    properties: Vec<GqlPropertyInput>,
) -> Result<impl Iterator<Item = (String, Prop)>, GraphError> {
    let props: Result<Vec<(String, Prop)>, GraphError> = properties
        .into_iter()
        .map(|p| {
            let v = Prop::try_from(p.value)?;
            Ok((p.key, v))
        })
        .collect();

    props.map(|vec| vec.into_iter())
}

#[ResolvedObjectFields]
impl GqlMutableGraph {
    /// Get the non-mutable graph

    async fn graph(&self) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.graph.clone())
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
        properties: Option<Vec<GqlPropertyInput>>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let self_clone_2 = self_clone.clone();
            let node = spawn_blocking(move || {
                self_clone
                    .graph
                    .add_node(time, &name, prop_iter, node_type.as_str())
            })
            .await
            .unwrap()?;
            node.update_embeddings().await?;
            self_clone_2.graph.write_updates()?;
            Ok(node.into())
        })
        .await
        .unwrap()
    }

    /// Create a new node or fail if it already exists
    async fn create_node(
        &self,
        time: i64,
        name: String,
        properties: Option<Vec<GqlPropertyInput>>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            let self_clone_2 = self_clone.clone();
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let node = spawn_blocking(move || {
                self_clone
                    .graph
                    .create_node(time, &name, prop_iter, node_type.as_str())
            })
            .await
            .unwrap()?;
            node.update_embeddings().await?;
            self_clone_2.graph.write_updates()?;
            Ok(node.into())
        })
        .await
        .unwrap()
    }

    /// Add a batch of nodes
    async fn add_nodes(&self, nodes: Vec<NodeAddition>) -> Result<bool, GraphError> {
        //TODO: How do we want to handle this par?
        let self_clone = self.clone();
        spawn(async move {
            for node in nodes {
                let name = node.name.as_str();

                for prop in node.updates.unwrap_or(vec![]) {
                    let prop_iter = as_properties(prop.properties.unwrap_or(vec![]))?;
                    self_clone
                        .graph
                        .add_node(prop.time, name, prop_iter, None)?;
                }
                if let Some(node_type) = node.node_type.as_str() {
                    self_clone.get_node_view(name)?.set_node_type(node_type)?;
                }
                let constant_props = node.constant_properties.unwrap_or(vec![]);
                if !constant_props.is_empty() {
                    let prop_iter = as_properties(constant_props)?;
                    self_clone
                        .get_node_view(name)?
                        .add_constant_properties(prop_iter)?;
                }
                if let Ok(node) = self_clone.get_node_view(name) {
                    let _ = node.update_embeddings().await; // FIXME: ideally this should call the embedding function just once!!
                }
            }
            self_clone.graph.write_updates()?;
            Ok(true)
        })
        .await
        .unwrap()
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
        properties: Option<Vec<GqlPropertyInput>>,
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
        let edge = self
            .graph
            .add_edge(time, src, dst, prop_iter, layer.as_str())?;
        let _ = edge.update_embeddings().await;
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
                let prop_iter = as_properties(prop.properties.unwrap_or(vec![]))?;
                self.graph.add_edge(prop.time, src, dst, prop_iter, layer)?;
            }
            let constant_props = edge.constant_properties.unwrap_or(vec![]);
            if !constant_props.is_empty() {
                let prop_iter = as_properties(constant_props)?;
                self.get_edge_view(src, dst)?
                    .add_constant_properties(prop_iter, layer)?;
            }
            if let Ok(edge) = self.get_edge_view(src, dst) {
                let _ = edge.update_embeddings().await; // FIXME: ideally this should call the embedding function just once!!
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
        let _ = edge.update_embeddings().await;
        self.graph.write_updates()?;
        Ok(edge.into())
    }

    /// Add temporal properties to graph
    async fn add_properties(
        &self,
        t: i64,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        self.graph.add_properties(t, as_properties(properties)?)?;
        self.update_graph_embeddings().await;
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Add constant properties to graph (errors if the property already exists)
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        self.graph
            .add_constant_properties(as_properties(properties)?)?;
        self.update_graph_embeddings().await;
        self.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the graph (overwrites existing values)
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        self.graph
            .update_constant_properties(as_properties(properties)?)?;
        self.update_graph_embeddings().await;
        self.graph.write_updates()?;
        Ok(true)
    }
}

impl GqlMutableGraph {
    async fn update_graph_embeddings(&self) {
        let _ = self
            .graph
            .update_graph_embeddings(Some(self.path.get_original_path_str().to_owned()))
            .await;
    }

    fn get_node_view(&self, name: &str) -> Result<NodeView<GraphWithVectors>, GraphError> {
        self.graph
            .node(name)
            .ok_or_else(|| GraphError::NodeMissingError(GID::Str(name.to_owned())))
    }

    fn get_edge_view(
        &self,
        src: &str,
        dst: &str,
    ) -> Result<EdgeView<GraphWithVectors>, GraphError> {
        self.graph
            .edge(&src, &dst)
            .ok_or(GraphError::EdgeMissingError {
                src: GID::Str(src.to_owned()),
                dst: GID::Str(dst.to_owned()),
            })
    }
}

#[derive(ResolvedObject)]
#[graphql(name = "MutableNode")]
pub struct GqlMutableNode {
    node: NodeView<GraphWithVectors>,
}

impl From<NodeView<GraphWithVectors>> for GqlMutableNode {
    fn from(node: NodeView<GraphWithVectors>) -> Self {
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
    async fn node(&self) -> GqlNode {
        self.node.clone().into()
    }

    /// Add constant properties to the node (errors if the property already exists)
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        self.node
            .add_constant_properties(as_properties(properties)?)?;
        let _ = self.node.update_embeddings().await;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Set the node type (errors if the node already has a non-default type)
    async fn set_node_type(&self, new_type: String) -> Result<bool, GraphError> {
        self.node.set_node_type(&new_type)?;
        let _ = self.node.update_embeddings().await;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the node (overwrites existing property values)
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        self.node
            .update_constant_properties(as_properties(properties)?)?;
        let _ = self.node.update_embeddings().await;
        self.node.graph.write_updates()?;
        Ok(true)
    }

    /// Add temporal property updates to the node
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropertyInput>>,
    ) -> Result<bool, GraphError> {
        self.node
            .add_updates(time, as_properties(properties.unwrap_or(vec![]))?)?;
        let _ = self.node.update_embeddings().await;
        self.node.graph.write_updates()?;
        Ok(true)
    }
}

#[derive(ResolvedObject)]
#[graphql(name = "MutableEdge")]
pub struct GqlMutableEdge {
    edge: EdgeView<GraphWithVectors>,
}

impl From<EdgeView<GraphWithVectors>> for GqlMutableEdge {
    fn from(edge: EdgeView<GraphWithVectors>) -> Self {
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
    async fn edge(&self) -> GqlEdge {
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
        let _ = self.edge.update_embeddings().await;
        self.edge.graph.write_updates()?;
        Ok(true)
    }

    /// Add constant properties to the edge (errors if the value already exists)
    ///
    /// If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
    /// need to be specified again.
    async fn add_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge
            .add_constant_properties(as_properties(properties)?, layer.as_str())?;
        let _ = self.edge.update_embeddings().await;
        self.edge.graph.write_updates()?;
        Ok(true)
    }

    /// Update constant properties of the edge (existing values are overwritten)
    ///
    /// If this is called after `add_edge`, the layer is inherited from the `add_edge` and does not
    /// need to be specified again.
    async fn update_constant_properties(
        &self,
        properties: Vec<GqlPropertyInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge
            .update_constant_properties(as_properties(properties)?, layer.as_str())?;
        let _ = self.edge.update_embeddings().await;
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
        properties: Option<Vec<GqlPropertyInput>>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        self.edge.add_updates(
            time,
            as_properties(properties.unwrap_or(vec![]))?,
            layer.as_str(),
        )?;
        let _ = self.edge.update_embeddings().await;
        self.edge.graph.write_updates()?;
        Ok(true)
    }
}
