use crate::{
    graph::{GraphWithVectors, UpdateEmbeddings},
    model::graph::{edge::GqlEdge, graph::GqlGraph, node::GqlNode, property::Value},
    rayon::blocking_write,
};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::graph::{edge::EdgeView, node::NodeView},
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct BatchFailures {
    batch_failures: Vec<(usize, GraphError)>,
    write_failure: Option<GraphError>,
}

fn split_failures<S>(
    results: impl IntoIterator<Item = Result<S, GraphError>>,
    write_result: Result<(), GraphError>,
) -> (Vec<S>, Option<BatchFailures>) {
    let mut succeeded = Vec::new();
    let mut batch_failures = Vec::new();
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(s) => succeeded.push(s),
            Err(err) => batch_failures.push((i, err)),
        }
    }
    let write_failure = write_result.err();
    let err = (!batch_failures.is_empty()).then(|| BatchFailures {
        batch_failures,
        write_failure,
    });
    (succeeded, err)
}

impl Display for BatchFailures {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(embedding_failure) = self.write_failure.as_ref() {
            write!(f, "Writing failed: {embedding_failure}")?;
        }
        if !self.batch_failures.is_empty() {
            if self.write_failure.is_some() {
                f.write_str(", ")?;
            }
            f.write_str("Batch updates failed: ")?;
            f.write_str(
                &self
                    .batch_failures
                    .iter()
                    .map(|(idx, err)| format!("{idx} -> {err}"))
                    .join(", "),
            )?;
        }
        Ok(())
    }
}

impl Error for BatchFailures {}

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
    graph: GraphWithVectors,
}

impl From<GraphWithVectors> for GqlMutableGraph {
    fn from(graph: GraphWithVectors) -> Self {
        Self { graph }
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
        GqlGraph::new(self.graph.folder.clone(), self.graph.graph.clone())
    }

    /// Get mutable existing node.
    async fn node(&self, name: String) -> Option<GqlMutableNode> {
        self.graph.node(name).map(|n| GqlMutableNode::new(n))
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
        let node = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let node = self_clone
                .graph
                .add_node(time, &name, prop_iter, node_type.as_str())?;

            Ok::<_, GraphError>(node)
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = node.update_embeddings().await;

        Ok(GqlMutableNode::new(node))
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
        let node = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let node = self_clone
                .graph
                .create_node(time, &name, prop_iter, node_type.as_str())?;

            Ok::<_, GraphError>(node)
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = node.update_embeddings().await;

        Ok(GqlMutableNode::new(node))
    }

    /// Add a batch of nodes.
    async fn add_nodes(&self, nodes: Vec<NodeAddition>) -> Result<bool, BatchFailures> {
        let self_clone = self.clone();

        let (succeeded, batch_failures) = blocking_write(move || {
            let nodes: Vec<_> = nodes
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
                .collect();

            split_failures(nodes, Ok(()))
        })
        .await;

        self.post_mutation_ops().await;

        // Generate embeddings
        let _ = self.graph.update_node_embeddings(succeeded).await;
        if let Some(failures) = batch_failures {
            Err(failures)
        } else {
            Ok(true)
        }
    }

    /// Get a mutable existing edge.
    async fn edge(&self, src: String, dst: String) -> Option<GqlMutableEdge> {
        self.graph.edge(src, dst).map(|e| GqlMutableEdge::new(e))
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
        let edge = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let edge = self_clone
                .graph
                .add_edge(time, src, dst, prop_iter, layer.as_str())?;

            Ok::<_, GraphError>(edge)
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = edge.update_embeddings().await;

        Ok(GqlMutableEdge::new(edge))
    }

    /// Add a batch of edges.
    async fn add_edges(&self, edges: Vec<EdgeAddition>) -> Result<bool, BatchFailures> {
        let self_clone = self.clone();

        let (edge_pairs, failures) = blocking_write(move || {
            let edge_res: Vec<_> = edges
                .into_iter()
                .map(|edge| {
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
                    Ok((edge.src, edge.dst))
                })
                .collect();

            split_failures(edge_res, Ok(()))
        })
        .await;

        self.post_mutation_ops().await;
        let _ = self.graph.update_edge_embeddings(edge_pairs).await;

        match failures {
            None => Ok(true),
            Some(failures) => Err(failures),
        }
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
        let edge = blocking_write(move || {
            let edge = self_clone
                .graph
                .delete_edge(time, src, dst, layer.as_str())?;

            Ok::<_, GraphError>(edge)
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = edge.update_embeddings().await;

        Ok(GqlMutableEdge::new(edge))
    }

    /// Add temporal properties to graph.
    async fn add_properties(
        &self,
        t: i64,
        properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        let result = blocking_write(move || {
            self_clone
                .graph
                .add_properties(t, as_properties(properties)?)?;
            Ok(true)
        })
        .await;

        self.post_mutation_ops().await;

        result
    }

    /// Add metadata to graph (errors if the property already exists).
    async fn add_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        let result = blocking_write(move || {
            self_clone.graph.add_metadata(as_properties(properties)?)?;
            Ok(true)
        })
        .await;
        self.post_mutation_ops().await;

        result
    }

    /// Update metadata of the graph (overwrites existing values).
    async fn update_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        let result = blocking_write(move || {
            self_clone
                .graph
                .update_metadata(as_properties(properties)?)?;
            Ok(true)
        })
        .await;

        self.post_mutation_ops().await;

        result
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

    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.graph.set_dirty(true);
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "MutableNode")]
pub struct GqlMutableNode {
    node: NodeView<'static, GraphWithVectors>,
}

impl GqlMutableNode {
    pub fn new(node: NodeView<'static, GraphWithVectors>) -> Self {
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
        blocking_write(move || {
            self_clone.node.add_metadata(as_properties(properties)?)?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;

        Ok(true)
    }

    /// Set the node type (errors if the node already has a non-default type).
    async fn set_node_type(&self, new_type: String) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.node.set_node_type(&new_type)?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;

        Ok(true)
    }

    /// Update metadata of the node (overwrites existing property values).
    async fn update_metadata(&self, properties: Vec<GqlPropertyInput>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone
                .node
                .update_metadata(as_properties(properties)?)?;

            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;

        Ok(true)
    }

    /// Add temporal property updates to the node.
    async fn add_updates(
        &self,
        time: i64,
        properties: Option<Vec<GqlPropertyInput>>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone
                .node
                .add_updates(time, as_properties(properties.unwrap_or(vec![]))?)?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = self.node.update_embeddings().await;

        Ok(true)
    }
}

impl GqlMutableNode {
    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.node.graph.set_dirty(true);
    }
}

#[derive(ResolvedObject, Clone)]
#[graphql(name = "MutableEdge")]
pub struct GqlMutableEdge {
    edge: EdgeView<GraphWithVectors>,
}

impl GqlMutableEdge {
    pub fn new(edge: EdgeView<GraphWithVectors>) -> Self {
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
        GqlMutableNode::new(self.edge.src())
    }

    /// Get the mutable destination node of the edge.
    async fn dst(&self) -> GqlMutableNode {
        GqlMutableNode::new(self.edge.dst())
    }

    /// Mark the edge as deleted at time time.
    async fn delete(&self, time: i64, layer: Option<String>) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.edge.delete(time, layer.as_str())?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = self.edge.update_embeddings().await;

        Ok(true)
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
        blocking_write(move || {
            self_clone
                .edge
                .add_metadata(as_properties(properties)?, layer.as_str())?;

            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = self.edge.update_embeddings().await;

        Ok(true)
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
        blocking_write(move || {
            self_clone
                .edge
                .update_metadata(as_properties(properties)?, layer.as_str())?;

            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = self.edge.update_embeddings().await;

        Ok(true)
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
        blocking_write(move || {
            self_clone.edge.add_updates(
                time,
                as_properties(properties.unwrap_or(vec![]))?,
                layer.as_str(),
            )?;

            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        let _ = self.edge.update_embeddings().await;

        Ok(true)
    }
}

impl GqlMutableEdge {
    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.edge.graph.set_dirty(true);
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
        db::api::{storage::storage::Config, view::MaterializedGraph},
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

    async fn create_mutable_graph() -> (GqlMutableGraph, Data, tempfile::TempDir) {
        let graph = create_test_graph();
        let tmp_dir = tempdir().unwrap();

        let config = AppConfig::default();
        let mut data = Data::new(tmp_dir.path(), &config, Config::default());

        // Override the embedding function with a mock for testing.
        data.embedding_conf = Some(EmbeddingConf {
            cache: VectorCache::in_memory(fake_embedding),
            global_template: Some(custom_template()),
            individual_templates: HashMap::new(),
        });

        let overwrite = false;
        let folder = data
            .validate_path_for_insert("test_graph", overwrite)
            .unwrap();
        data.insert_graph(folder.clone(), graph).await.unwrap();

        let graph_with_vectors = data.get_graph("test_graph").await.unwrap();
        let mutable_graph = GqlMutableGraph::from(graph_with_vectors);

        (mutable_graph, data, tmp_dir)
    }

    #[tokio::test]
    async fn test_add_nodes_empty_list() {
        let (mutable_graph, _data, _tmp_dir) = create_mutable_graph().await;

        let nodes = vec![];
        let result = mutable_graph.add_nodes(nodes).await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    #[ignore = "TODO: #2384"]
    async fn test_add_nodes_simple() {
        let (mutable_graph, _data, _tmp_dir) = create_mutable_graph().await;

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

        // TODO: #2380 (embeddings aren't working right now)
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
    #[ignore = "TODO: #2384"]
    async fn test_add_nodes_with_properties() {
        let (mutable_graph, _data, _tmp_dir) = create_mutable_graph().await;

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

        // TODO: #2380 (embeddings aren't working right now)
        // let query = "complex_node_1".to_string();
        // let embedding = &fake_embedding(vec![query]).await.unwrap().remove(0);
        // let limit = 5;
        // let result = mutable_graph
        //     .graph
        //     .vectors
        //     .unwrap()
        //     .nodes_by_similarity(embedding, limit, None);
        //
        // assert!(result.is_ok());
        // assert!(result.unwrap().get_documents().unwrap().len() == 3);
    }

    #[tokio::test]
    #[ignore = "TODO: #2384"]
    async fn test_add_edges_simple() {
        let (mutable_graph, _data, _tmp_dir) = create_mutable_graph().await;

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

        // TODO: #2380 (embeddings aren't working right now)
        // Test that edge embeddings were generated.
        // let query = "node1 appeared with node2".to_string();
        // let embedding = &fake_embedding(vec![query]).await.unwrap().remove(0);
        // let limit = 5;
        // let result = mutable_graph
        //     .graph
        //     .vectors
        //     .unwrap()
        //     .edges_by_similarity(embedding, limit, None);
        //
        // assert!(result.is_ok());
        // assert!(result.unwrap().get_documents().unwrap().len() == 2);
    }
}
