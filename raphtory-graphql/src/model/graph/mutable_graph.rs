use crate::{
    graph::{GraphWithVectors, UpdateEmbeddings},
    model::{
        blocking_io,
        graph::{edge::GqlEdge, graph::GqlGraph, node::GqlNode, property::Value},
    },
    paths::ExistingGraphFolder,
    rayon::blocking_compute,
};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::graph::{edge::EdgeView, node::NodeView},
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use tokio::spawn;

#[derive(InputObject, Clone)]
#[graphql(name = "PropertyInput")]
pub struct GqlPropertyInput {
    /// Key.
    key: String,
    /// Value.
    value: Value,
}

#[derive(InputObject, Clone)]
pub struct TemporalPropertyInput {
    /// Time.
    time: i64,
    /// Properties.
    properties: Option<Vec<GqlPropertyInput>>,
}

#[derive(InputObject, Clone)]
pub struct NodeAddition {
    /// Name.
    name: String,
    /// Node type.
    node_type: Option<String>,
    /// Metadata.
    metadata: Option<Vec<GqlPropertyInput>>,
    /// Updates.
    updates: Option<Vec<TemporalPropertyInput>>,
}

#[derive(InputObject, Clone)]
pub struct EdgeAddition {
    /// Source node.
    src: String,
    /// Destination node.
    dst: String,
    /// Layer.
    layer: Option<String>,
    /// Metadata.
    metadata: Option<Vec<GqlPropertyInput>>,
    // Update events.
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
) -> Result<impl ExactSizeIterator<Item = (String, Prop)>, GraphError> {
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
    /// Get the non-mutable graph.
    async fn graph(&self) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.graph.clone())
    }

    /// Get mutable existing node.
    async fn node(&self, name: String) -> Option<GqlMutableNode> {
        self.graph.node(name).map(|n| n.into())
    }

    /// Add a new node or add updates to an existing node.
    async fn add_node(
        &self,
        time: i64,
        name: String,
        properties: Option<Vec<GqlPropertyInput>>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        let node = blocking_compute(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            self_clone
                .graph
                .add_node(time, &name, prop_iter, node_type.as_str())
        })
        .await?;
        node.update_embeddings().await?;
        Ok(node.into())
    }

    /// Create a new node or fail if it already exists.
    async fn create_node(
        &self,
        time: i64,
        name: String,
        properties: Option<Vec<GqlPropertyInput>>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        let node = blocking_compute(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            self_clone
                .graph
                .create_node(time, &name, prop_iter, node_type.as_str())
        })
        .await?;
        node.update_embeddings().await?;
        Ok(node.into())
    }

    /// Add a batch of nodes.
    async fn add_nodes(&self, nodes: Vec<NodeAddition>) -> Result<bool, GraphError> {
        let self_clone = self.clone();

        let nodes: Vec<Result<NodeView<GraphWithVectors>, GraphError>> =
            blocking_compute(move || {
                nodes
                    .iter()
                    .map(|node| {
                        let node = node.clone();
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
                        let metadata = node.metadata.unwrap_or(vec![]);
                        if !metadata.is_empty() {
                            let prop_iter = as_properties(metadata)?;
                            self_clone.get_node_view(name)?.add_metadata(prop_iter)?;
                        }
                        self_clone.get_node_view(name)
                    })
                    .collect()
            })
            .await;

        // Generate embeddings.
        let nodes: Vec<_> = nodes.into_iter().collect::<Result<Vec<_>, _>>()?;
        self.graph.update_node_embeddings(nodes).await?;

        Ok(true)
    }

    /// Get a mutable existing edge.
    async fn edge(&self, src: String, dst: String) -> Option<GqlMutableEdge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    /// Add a new edge or add updates to an existing edge.
    async fn add_edge(
        &self,
        time: i64,
        src: String,
        dst: String,
        properties: Option<Vec<GqlPropertyInput>>,
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let self_clone = self.clone();
        let edge = blocking_compute(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            self_clone
                .graph
                .add_edge(time, src, dst, prop_iter, layer.as_str())
        })
        .await;
        let edge = edge?;
        let _ = edge.update_embeddings().await;

        Ok(edge.into())
    }

    /// Add a batch of edges.
    async fn add_edges(&self, edges: Vec<EdgeAddition>) -> Result<bool, GraphError> {
        let self_clone = self.clone();

        let edges: Vec<Result<EdgeView<GraphWithVectors>, GraphError>> =
            blocking_compute(move || {
                edges
                    .iter()
                    .map(|edge| {
                        let edge = edge.clone();
                        let src = edge.src.as_str();
                        let dst = edge.dst.as_str();
                        let layer = edge.layer.as_str();
                        for prop in edge.updates.unwrap_or(vec![]) {
                            let prop_iter = as_properties(prop.properties.unwrap_or(vec![]))?;
                            self_clone
                                .graph
                                .add_edge(prop.time, src, dst, prop_iter, layer)?;
                        }
                        let metadata = edge.metadata.unwrap_or(vec![]);
                        if !metadata.is_empty() {
                            let prop_iter = as_properties(metadata)?;
                            self_clone
                                .get_edge_view(src.to_string(), dst.to_string())?
                                .add_metadata(prop_iter, layer)?;
                        }
                        self_clone.get_edge_view(src.to_string(), dst.to_string())
                    })
                    .collect()
            })
            .await;

        // Generate embeddings.
        let edge_pairs: Vec<_> = edges
            .into_iter()
            .collect::<Result<Vec<_>, _>>()? // Return 1st encountered error
            .into_iter()
            .map(|edge| (edge.src().name(), edge.dst().name()))
            .collect();

        self.graph.update_edge_embeddings(edge_pairs).await?;

        Ok(true)
    }

    /// Mark an edge as deleted (creates the edge if it did not exist).
    async fn delete_edge(
        &self,
        time: i64,
        src: String,
        dst: String,
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let self_clone = self.clone();
        let self_clone_2 = self.clone();
        let edge =
            blocking_compute(move || self_clone.graph.delete_edge(time, src, dst, layer.as_str()))
                .await;
        let edge = edge?;
        let _ = edge.update_embeddings().await;

        Ok(edge.into())
    }

    /// Add temporal properties to graph.
    async fn add_properties(
        &self,
        t: i64,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .graph
                .add_properties(t, as_properties(properties)?)?;
            Ok(true)
        })
        .await
    }

    /// Add metadata to graph (errors if the property already exists).
    async fn add_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.graph.add_metadata(as_properties(properties)?)?;
            Ok(true)
        })
        .await
    }

    /// Update metadata of the graph (overwrites existing values).
    async fn update_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .graph
                .update_metadata(as_properties(properties)?)?;
            Ok(true)
        })
        .await
    }
}

impl GqlMutableGraph {
    fn get_node_view(&self, name: &str) -> Result<NodeView<'static, GraphWithVectors>, GraphError> {
        self.graph
            .node(name)
            .ok_or_else(|| GraphError::NodeMissingError(GID::Str(name.to_owned())))
    }

    fn get_edge_view(
        &self,
        src: String,
        dst: String,
    ) -> Result<EdgeView<GraphWithVectors>, GraphError> {
        self.graph
            .edge(src.clone(), dst.clone())
            .ok_or(GraphError::EdgeMissingError {
                src: GID::Str(src),
                dst: GID::Str(dst),
            })
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "MutableNode")]
pub struct GqlMutableNode {
    node: NodeView<'static, GraphWithVectors>,
}

impl From<NodeView<'static, GraphWithVectors>> for GqlMutableNode {
    fn from(node: NodeView<'static, GraphWithVectors>) -> Self {
        Self { node }
    }
}

#[ResolvedObjectFields]
impl GqlMutableNode {
    /// Use to check if adding the node was successful.
    async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable Node.
    async fn node(&self) -> GqlNode {
        self.node.clone().into()
    }

    /// Add metadata to the node (errors if the property already exists).
    async fn add_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone.node.add_metadata(as_properties(properties)?)?;
            let _ = self_clone.node.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Set the node type (errors if the node already has a non-default type).
    async fn set_node_type(&self, new_type: String) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone.node.set_node_type(&new_type)?;
            let _ = self_clone.node.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Update metadata of the node (overwrites existing property values).
    async fn update_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone
                .node
                .update_metadata(as_properties(properties)?)?;
            let _ = self_clone.node.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Add temporal property updates to the node.
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropertyInput>>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone
                .node
                .add_updates(time, as_properties(properties.unwrap_or(vec![]))?)?;
            let _ = self_clone.node.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }
}

#[derive(ResolvedObject, Clone)]
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
    /// Use to check if adding the edge was successful.
    async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable edge for querying.
    async fn edge(&self) -> GqlEdge {
        self.edge.clone().into()
    }

    /// Get the mutable source node of the edge.
    async fn src(&self) -> GqlMutableNode {
        self.edge.src().into()
    }

    /// Get the mutable destination node of the edge.
    async fn dst(&self) -> GqlMutableNode {
        self.edge.dst().into()
    }

    /// Mark the edge as deleted at time time.
    async fn delete(&self, time: i64, layer: Option<String>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone.edge.delete(time, layer.as_str())?;
            let _ = self_clone.edge.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Add metadata to the edge (errors if the value already exists).
    ///
    /// If this is called after add_edge, the layer is inherited from the add_edge and does not
    /// need to be specified again.
    async fn add_metadata(
        &self,
        properties: Vec<GqlPropertyInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone
                .edge
                .add_metadata(as_properties(properties)?, layer.as_str())?;
            let _ = self_clone.edge.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Update metadata of the edge (existing values are overwritten).
    ///
    /// If this is called after add_edge, the layer is inherited from the add_edge and does not
    /// need to be specified again.
    async fn update_metadata(
        &self,
        properties: Vec<GqlPropertyInput>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone
                .edge
                .update_metadata(as_properties(properties)?, layer.as_str())?;
            let _ = self_clone.edge.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }

    /// Add temporal property updates to the edge.
    ///
    /// If this is called after add_edge, the layer is inherited from the add_edge and does not
    /// need to be specified again.
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropertyInput>>,
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            self_clone.edge.add_updates(
                time,
                as_properties(properties.unwrap_or(vec![]))?,
                layer.as_str(),
            )?;
            let _ = self_clone.edge.update_embeddings().await;
            Ok(true)
        })
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::app_config::AppConfig,
        data::{Data, EmbeddingConf},
    };
    use itertools::Itertools;
    use raphtory::{
        db::api::view::MaterializedGraph,
        vectors::{
            cache::VectorCache, embeddings::EmbeddingResult, template::DocumentTemplate, Embedding,
        },
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    async fn fake_embedding(texts: Vec<String>) -> EmbeddingResult<Vec<Embedding>> {
        Ok(texts
            .into_iter()
            .map(|_| vec![1.0, 0.0, 0.0].into())
            .collect_vec())
    }

    fn custom_template() -> DocumentTemplate {
        DocumentTemplate {
            node_template: Some("{{ name }} is a {{ node_type }}".to_string()),
            edge_template: Some("{{ src.name }} appeared with {{ dst.name}}".to_string()),
        }
    }

    fn create_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        graph.into()
    }

    async fn create_mutable_graph() -> (GqlMutableGraph, tempfile::TempDir) {
        let graph = create_test_graph();
        let tmp_dir = tempdir().unwrap();

        let config = AppConfig::default();
        let mut data = Data::new(tmp_dir.path(), &config);

        // Override the embedding function with a mock for testing.
        data.embedding_conf = Some(EmbeddingConf {
            cache: VectorCache::in_memory(fake_embedding),
            global_template: Some(custom_template()),
            individual_templates: HashMap::new(),
        });

        let folder = data.validate_path_for_insert("test_graph").unwrap();
        data.insert_graph(folder, graph).await.unwrap();

        let (graph_with_vectors, path) = data.get_graph("test_graph").await.unwrap();
        let mutable_graph = GqlMutableGraph::new(path, graph_with_vectors);

        (mutable_graph, tmp_dir)
    }

    #[tokio::test]
    async fn test_add_nodes_empty_list() {
        let (mutable_graph, _tmp_dir) = create_mutable_graph().await;

        let nodes = vec![];
        let result = mutable_graph.add_nodes(nodes).await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_add_nodes_simple() {
        let (mutable_graph, _tmp_dir) = create_mutable_graph().await;

        let nodes = vec![
            NodeAddition {
                name: "node1".to_string(),
                node_type: Some("test_node_type".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
            NodeAddition {
                name: "node2".to_string(),
                node_type: Some("test_node_type".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
        ];

        let result = mutable_graph.add_nodes(nodes).await;

        assert!(result.is_ok());
        assert!(result.unwrap());

        let query = "node1".to_string();
        let embedding = &fake_embedding(vec![query]).await.unwrap().remove(0);
        let limit = 5;
        let result = mutable_graph
            .graph
            .vectors
            .unwrap()
            .nodes_by_similarity(embedding, limit, None);

        assert!(result.is_ok());
        assert!(result.unwrap().get_documents().unwrap().len() == 2);
    }

    #[tokio::test]
    async fn test_add_nodes_with_properties() {
        let (mutable_graph, _tmp_dir) = create_mutable_graph().await;

        let nodes = vec![
            NodeAddition {
                name: "complex_node_1".to_string(),
                node_type: Some("employee".to_string()),
                metadata: Some(vec![GqlPropertyInput {
                    key: "department".to_string(),
                    value: Value::Str("Sales".to_string()),
                }]),
                updates: Some(vec![
                    TemporalPropertyInput {
                        time: 0,
                        properties: Some(vec![GqlPropertyInput {
                            key: "salary".to_string(),
                            value: Value::F64(50000.0),
                        }]),
                    },
                    TemporalPropertyInput {
                        time: 0,
                        properties: Some(vec![GqlPropertyInput {
                            key: "salary".to_string(),
                            value: Value::F64(55000.0),
                        }]),
                    },
                ]),
            },
            NodeAddition {
                name: "complex_node_2".to_string(),
                node_type: Some("employee".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
            NodeAddition {
                name: "complex_node_3".to_string(),
                node_type: Some("employee".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: Some(vec![GqlPropertyInput {
                        key: "salary".to_string(),
                        value: Value::F64(55000.0),
                    }]),
                }]),
            },
        ];

        let result = mutable_graph.add_nodes(nodes).await;

        assert!(result.is_ok());
        assert!(result.unwrap());

        let query = "complex_node_1".to_string();
        let embedding = &fake_embedding(vec![query]).await.unwrap().remove(0);
        let limit = 5;
        let result = mutable_graph
            .graph
            .vectors
            .unwrap()
            .nodes_by_similarity(embedding, limit, None);

        assert!(result.is_ok());
        assert!(result.unwrap().get_documents().unwrap().len() == 3);
    }

    #[tokio::test]
    async fn test_add_edges_simple() {
        let (mutable_graph, _tmp_dir) = create_mutable_graph().await;

        // First add some nodes.
        let nodes = vec![
            NodeAddition {
                name: "node1".to_string(),
                node_type: Some("person".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
            NodeAddition {
                name: "node2".to_string(),
                node_type: Some("person".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
        ];

        let result = mutable_graph.add_nodes(nodes).await;
        assert!(result.is_ok());

        // Now add edges between them.
        let edges = vec![
            EdgeAddition {
                src: "node1".to_string(),
                dst: "node2".to_string(),
                layer: Some("friendship".to_string()),
                metadata: Some(vec![GqlPropertyInput {
                    key: "strength".to_string(),
                    value: Value::F64(0.8),
                }]),
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
            EdgeAddition {
                src: "node2".to_string(),
                dst: "node1".to_string(),
                layer: Some("friendship".to_string()),
                metadata: None,
                updates: Some(vec![TemporalPropertyInput {
                    time: 0,
                    properties: None,
                }]),
            },
        ];

        let result = mutable_graph.add_edges(edges).await;

        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test that edge embeddings were generated.
        let query = "node1 appeared with node2".to_string();
        let embedding = &fake_embedding(vec![query]).await.unwrap().remove(0);
        let limit = 5;
        let result = mutable_graph
            .graph
            .vectors
            .unwrap()
            .edges_by_similarity(embedding, limit, None);

        assert!(result.is_ok());
        assert!(result.unwrap().get_documents().unwrap().len() == 2);
    }
}
