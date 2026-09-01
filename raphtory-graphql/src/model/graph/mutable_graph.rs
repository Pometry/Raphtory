use crate::{
    graph::{GraphWithVectors, UpdateEmbeddings},
    model::{
        graph::{
            edge::GqlEdge, graph::GqlGraph, node::GqlNode, node_id::GqlNodeId, property::Value,
            timeindex::GqlTimeInput,
        },
        GqlGraphType,
    },
    paths::ValidGraphPaths,
    rayon::blocking_write,
};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::MaterializedGraph,
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};
use tracing::error;

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
    /// Time of the update — accepts the same forms as `TimeInput` (epoch
    /// millis Int, RFC3339 string, or `{timestamp, eventId}` object).
    time: GqlTimeInput,
    /// Properties.
    properties: Option<Vec<GqlPropertyInput>>,
}

#[derive(InputObject, Clone)]
pub struct NodeAddition {
    /// Node id (string or non-negative integer).
    name: GqlNodeId,
    /// Node type.
    node_type: Option<String>,
    /// Metadata.
    metadata: Option<Vec<GqlPropertyInput>>,
    /// Updates.
    updates: Option<Vec<TemporalPropertyInput>>,
    /// Layer.
    layer: Option<String>,
}

#[derive(InputObject, Clone)]
pub struct EdgeAddition {
    /// Source node id (string or non-negative integer).
    src: GqlNodeId,
    /// Destination node id (string or non-negative integer).
    dst: GqlNodeId,
    /// Layer.
    layer: Option<String>,
    /// Metadata.
    metadata: Option<Vec<GqlPropertyInput>>,
    // Update events.
    updates: Option<Vec<TemporalPropertyInput>>,
}

/// Write-enabled handle for a graph. Obtained by calling `updateGraph(path)`
/// on the root query with a path you have write permission for. Supports
/// adding nodes and edges (individually or in batches), attaching
/// properties/metadata, and looking up mutable `node`/`edge` handles. Use the
/// read-only `graph(path)` resolver for queries.
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

pub(crate) fn as_properties(
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
    /// Read-only view of this graph — identical to what you'd get from
    /// `graph(path:)` on the query root. Use this when you want to compose
    /// queries on the graph you've just mutated. `graphType` lets you
    /// re-interpret the graph at query time (see `graph(path:)` for
    /// semantics); defaults to the stored graph's native type.
    pub async fn graph(
        &self,
        #[graphql(
            desc = "Optional override for graph semantics — `EVENT` treats every update as a point-in-time event, `PERSISTENT` carries values forward until overwritten or deleted. Defaults to the stored graph's native type."
        )]
        graph_type: Option<GqlGraphType>,
    ) -> GqlGraph {
        let folder = self.graph.folder().clone();
        match graph_type {
            Some(GqlGraphType::Event) => match self.graph.graph().clone() {
                MaterializedGraph::EventGraph(g) => GqlGraph::new(folder, g),
                MaterializedGraph::PersistentGraph(g) => GqlGraph::new(folder, g.event_graph()),
            },
            Some(GqlGraphType::Persistent) => match self.graph.graph().clone() {
                MaterializedGraph::EventGraph(g) => GqlGraph::new(folder, g.persistent_graph()),
                MaterializedGraph::PersistentGraph(g) => GqlGraph::new(folder, g),
            },
            None => GqlGraph::new(folder, self.graph.graph().clone()),
        }
    }

    /// Look up an existing node for mutation. Returns null if the node doesn't
    /// exist; use `addNode` or `createNode` to create one.

    pub async fn node(
        &self,
        #[graphql(desc = "Node id.")] name: GqlNodeId,
    ) -> Option<GqlMutableNode> {
        self.graph.node(name).map(|n| GqlMutableNode::new(n))
    }

    /// Add a new node or append an update to an existing one. Upsert semantics:
    /// no error if the node already exists — properties and type are merged.

    pub async fn add_node(
        &self,
        #[graphql(desc = "Time of the event.")] time: GqlTimeInput,
        #[graphql(desc = "Node id.")] name: GqlNodeId,
        #[graphql(desc = "Optional property updates attached to this event.")] properties: Option<
            Vec<GqlPropertyInput>,
        >,
        #[graphql(
            desc = "Optional node type to assign. If provided, sets the node's type at this event."
        )]
        node_type: Option<String>,
        #[graphql(desc = "Optional layer name. If omitted, the default layer is used.")]
        layer: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        let node = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let node = self_clone.graph.add_node(
                time.into_input_time(),
                &name,
                prop_iter,
                node_type.as_str(),
                layer.as_str(),
            )?;

            Ok::<_, GraphError>(node)
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = node.update_embeddings().await {
            error!(
                "Failed to embed the node added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }

        Ok(GqlMutableNode::new(node))
    }

    /// Create a new node or fail if it already exists. Strict alternative to
    /// `addNode` — use this when you want to detect collisions.

    pub async fn create_node(
        &self,
        #[graphql(desc = "Time of the create event.")] time: GqlTimeInput,
        #[graphql(desc = "Node id.")] name: GqlNodeId,
        #[graphql(desc = "Optional property updates attached to this event.")] properties: Option<
            Vec<GqlPropertyInput>,
        >,
        #[graphql(
            desc = "Optional node type to assign. If provided, sets the node's type at this event."
        )]
        node_type: Option<String>,
        #[graphql(desc = "Optional layer name. If omitted, the default layer is used.")]
        layer: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let self_clone = self.clone();
        let node = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let node = self_clone.graph.create_node(
                time.into_input_time(),
                &name,
                prop_iter,
                node_type.as_str(),
                layer.as_str(),
            )?;

            Ok::<_, GraphError>(node)
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = node.update_embeddings().await {
            error!(
                "Failed to embed the node added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }

        Ok(GqlMutableNode::new(node))
    }

    /// Batch-add multiple nodes in one call. For each `NodeAddition`, applies every
    /// update it carries (time/properties pairs), then optionally sets its node type
    /// and adds any metadata. On partial failure, returns a `BatchFailures` error
    /// describing which entries failed and why; otherwise returns true.

    pub async fn add_nodes(
        &self,
        #[graphql(
            desc = "List of `NodeAddition` inputs, each specifying a node's name, optional type, layer, per-timestamp updates, and metadata."
        )]
        nodes: Vec<NodeAddition>,
    ) -> Result<bool, BatchFailures> {
        let self_clone = self.clone();

        let (succeeded, batch_failures) = blocking_write(move || {
            let nodes: Vec<_> = nodes
                .iter()
                .map(|node| {
                    let node = node.clone();
                    let name = &node.name;
                    let node_type = node.node_type.as_str();
                    let layer = node.layer.as_str();

                    for prop in node.updates.unwrap_or(vec![]) {
                        let prop_iter = as_properties(prop.properties.unwrap_or(vec![]))?;
                        self_clone.graph.add_node(
                            prop.time.into_input_time(),
                            name,
                            prop_iter,
                            node_type,
                            layer,
                        )?;
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

        let count = succeeded.len();
        if let Err(error) = self.graph.update_node_embeddings(succeeded).await {
            error!(
                "Failed to embed {count} nodes added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }
        if let Some(failures) = batch_failures {
            Err(failures)
        } else {
            Ok(true)
        }
    }

    /// Look up an existing edge for mutation. Returns null if no such edge exists.

    pub async fn edge(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> Option<GqlMutableEdge> {
        self.graph.edge(src, dst).map(|e| GqlMutableEdge::new(e))
    }

    /// Add a new edge or append an update to an existing one. Upsert semantics:
    /// safe to call on an edge that already exists — creates missing endpoints if
    /// needed.

    pub async fn add_edge(
        &self,
        #[graphql(desc = "Time of the event.")] time: GqlTimeInput,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
        #[graphql(desc = "Optional property updates attached to this event.")] properties: Option<
            Vec<GqlPropertyInput>,
        >,
        #[graphql(desc = "Optional layer name. If omitted, the default layer is used.")]
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let self_clone = self.clone();
        let edge = blocking_write(move || {
            let prop_iter = as_properties(properties.unwrap_or(vec![]))?;
            let edge = self_clone.graph.add_edge(
                time.into_input_time(),
                src,
                dst,
                prop_iter,
                layer.as_str(),
            )?;

            Ok::<_, GraphError>(edge)
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = edge.update_embeddings().await {
            error!(
                "Failed to embed the edge added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }

        Ok(GqlMutableEdge::new(edge))
    }

    /// Batch-add multiple edges in one call. For each `EdgeAddition`, applies every
    /// update it carries, then adds any metadata. On partial failure, returns a
    /// `BatchFailures` error describing which entries failed; otherwise returns
    /// true.

    pub async fn add_edges(
        &self,
        #[graphql(
            desc = "List of `EdgeAddition` inputs, each specifying an edge's `src`, `dst`, optional layer, per-timestamp updates, and metadata."
        )]
        edges: Vec<EdgeAddition>,
    ) -> Result<bool, BatchFailures> {
        let self_clone = self.clone();

        let (edge_pairs, failures) = blocking_write(move || {
            let edge_res: Vec<_> = edges
                .into_iter()
                .map(|edge| {
                    let src = &edge.src;
                    let dst = &edge.dst;
                    let layer = edge.layer.as_str();
                    for prop in edge.updates.unwrap_or(vec![]) {
                        let prop_iter = as_properties(prop.properties.unwrap_or(vec![]))?;
                        self_clone.graph.add_edge(
                            prop.time.into_input_time(),
                            src,
                            dst,
                            prop_iter,
                            layer,
                        )?;
                    }
                    let metadata = edge.metadata.unwrap_or(vec![]);
                    if !metadata.is_empty() {
                        let prop_iter = as_properties(metadata)?;
                        self_clone
                            .get_edge_view(src, dst)?
                            .add_metadata(prop_iter, layer)?;
                    }
                    Ok((edge.src, edge.dst))
                })
                .collect();

            split_failures(edge_res, Ok(()))
        })
        .await;

        self.post_mutation_ops().await;
        let count = edge_pairs.len();
        if let Err(error) = self.graph.update_edge_embeddings(edge_pairs).await {
            error!(
                "Failed to embed {count} edges added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }

        match failures {
            None => Ok(true),
            Some(failures) => Err(failures),
        }
    }

    /// Mark an edge as deleted at the given time. Persistent graphs treat this
    /// as a tombstone (the edge becomes invalid from `time` onwards); event
    /// graphs simply log the deletion event. Creates the edge first if it did
    /// not exist.

    pub async fn delete_edge(
        &self,
        #[graphql(desc = "Time of the deletion.")] time: GqlTimeInput,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
        #[graphql(desc = "Optional layer name. If omitted, the default layer is used.")]
        layer: Option<String>,
    ) -> Result<GqlMutableEdge, GraphError> {
        let self_clone = self.clone();
        let edge = blocking_write(move || {
            let edge =
                self_clone
                    .graph
                    .delete_edge(time.into_input_time(), src, dst, layer.as_str())?;

            Ok::<_, GraphError>(edge)
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = edge.update_embeddings().await {
            error!(
                "Failed to embed the edge added to graph {}: {error}",
                self.graph.folder().local_path()
            );
        }

        Ok(GqlMutableEdge::new(edge))
    }

    /// Add temporal properties to the graph itself (not a node or edge). Each
    /// call records a property update at `t`.

    pub async fn add_properties(
        &self,
        #[graphql(desc = "Time of the update.")] t: GqlTimeInput,
        #[graphql(desc = "List of `{key, value}` pairs to set.")] properties: Vec<GqlPropertyInput>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        let result = blocking_write(move || {
            self_clone
                .graph
                .add_properties(t.into_input_time(), as_properties(properties)?)?;
            Ok(true)
        })
        .await;

        self.post_mutation_ops().await;

        result
    }

    /// Add metadata to the graph itself. Errors if any of the keys already
    /// exists — use `updateMetadata` to overwrite.

    pub async fn add_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to set as metadata.")] properties: Vec<
            GqlPropertyInput,
        >,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        let result = blocking_write(move || {
            self_clone.graph.add_metadata(as_properties(properties)?)?;
            Ok(true)
        })
        .await;
        self.post_mutation_ops().await;

        result
    }

    /// Update metadata of the graph itself, overwriting any existing values for
    /// the given keys.

    pub async fn update_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to upsert.")] properties: Vec<
            GqlPropertyInput,
        >,
    ) -> Result<bool, GraphError> {
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

    /// Persist any in-memory state for this graph to disk so other
    /// processes attaching a read-only handle observe up-to-date data.
    pub async fn flush(&self) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            let res = self_clone.graph.persist();
            if res.is_err() {
                self_clone.graph.set_dirty(true);
            }
            res.map(|_| true)
        })
        .await
    }
}

impl GqlMutableGraph {
    fn get_node_view(
        &self,
        name: &GqlNodeId,
    ) -> Result<NodeView<'static, GraphWithVectors>, GraphError> {
        self.graph
            .node(name)
            .ok_or_else(|| GraphError::NodeMissingError(name.0.clone()))
    }

    fn get_edge_view(
        &self,
        src: &GqlNodeId,
        dst: &GqlNodeId,
    ) -> Result<EdgeView<GraphWithVectors>, GraphError> {
        self.graph
            .edge(src, dst)
            .ok_or_else(|| GraphError::EdgeMissingError {
                src: src.0.clone(),
                dst: dst.0.clone(),
            })
    }

    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.graph.set_dirty(true);
    }
}

/// Write-side handle for a single node — returned from `addNode`, `createNode`,
/// or `MutableGraph.node`. Supports adding updates, setting node type, and
/// attaching or updating metadata.
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
    pub async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable Node.
    pub async fn node(&self) -> GqlNode {
        self.node.clone().into()
    }

    /// Add metadata to this node. Errors if any of the keys already exists —
    /// use `updateMetadata` to overwrite.

    pub async fn add_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to set as metadata.")] properties: Vec<
            GqlPropertyInput,
        >,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.node.add_metadata(as_properties(properties)?)?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;

        Ok(true)
    }

    /// Set this node's type. Errors if the node already has a non-default
    /// type and you're trying to change it.

    pub async fn set_node_type(
        &self,
        #[graphql(desc = "Node-type name to assign.")] new_type: String,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.node.set_node_type(&new_type)?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;

        Ok(true)
    }

    /// Update metadata of this node, overwriting any existing values for the
    /// given keys.

    pub async fn update_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to upsert.")] properties: Vec<
            GqlPropertyInput,
        >,
    ) -> Result<bool, GraphError> {
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

    /// Append a property update to this node at a specific time.

    pub async fn add_updates(
        &self,
        #[graphql(desc = "Time of the update.")] time: GqlTimeInput,
        #[graphql(desc = "Optional `{key, value}` pairs attached to the event.")]
        properties: Option<Vec<GqlPropertyInput>>,
        #[graphql(desc = "Optional layer name. If omitted, the default layer is used.")]
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.node.add_updates(
                time.into_input_time(),
                as_properties(properties.unwrap_or(vec![]))?,
                layer.as_str(),
            )?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = self.node.update_embeddings().await {
            error!(
                "Failed to re-embed an updated node in graph {}: {error}",
                self.node.graph.folder().local_path()
            );
        }

        Ok(true)
    }
}

impl GqlMutableNode {
    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.node.graph.set_dirty(true);
    }
}

/// Write-side handle for a single edge — returned from `addEdge` or
/// `MutableGraph.edge`. Supports adding updates, deletions, and attaching
/// or updating metadata.
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
    pub async fn success(&self) -> bool {
        true
    }

    /// Get the non-mutable edge for querying.
    pub async fn edge(&self) -> GqlEdge {
        self.edge.clone().into()
    }

    /// Get the mutable source node of the edge.
    pub async fn src(&self) -> GqlMutableNode {
        GqlMutableNode::new(self.edge.src())
    }

    /// Get the mutable destination node of the edge.
    pub async fn dst(&self) -> GqlMutableNode {
        GqlMutableNode::new(self.edge.dst())
    }

    /// Mark this edge as deleted at the given time. Persistent graphs treat this
    /// as a tombstone (the edge becomes invalid from `time` onwards); event
    /// graphs simply log the deletion event.

    pub async fn delete(
        &self,
        #[graphql(desc = "Time of the deletion.")] time: GqlTimeInput,
        #[graphql(
            desc = "Optional layer name. If omitted, uses the layer the edge was originally added on (when called after `addEdge`)."
        )]
        layer: Option<String>,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone
                .edge
                .delete(time.into_input_time(), layer.as_str())?;
            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = self.edge.update_embeddings().await {
            error!(
                "Failed to re-embed an updated edge in graph {}: {error}",
                self.edge.graph.folder().local_path()
            );
        }

        Ok(true)
    }

    /// Add metadata to this edge. Errors if any of the keys already exists —
    /// use `updateMetadata` to overwrite. If this is called after `addEdge`,
    /// the layer is inherited and does not need to be specified again.

    pub async fn add_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to set as metadata.")] properties: Vec<
            GqlPropertyInput,
        >,
        #[graphql(desc = "Optional layer name; defaults to the inherited layer.")] layer: Option<
            String,
        >,
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
        if let Err(error) = self.edge.update_embeddings().await {
            error!(
                "Failed to re-embed an updated edge in graph {}: {error}",
                self.edge.graph.folder().local_path()
            );
        }

        Ok(true)
    }

    /// Update metadata of this edge, overwriting any existing values for the
    /// given keys. If this is called after `addEdge`, the layer is inherited
    /// and does not need to be specified again.

    pub async fn update_metadata(
        &self,
        #[graphql(desc = "List of `{key, value}` pairs to upsert.")] properties: Vec<
            GqlPropertyInput,
        >,
        #[graphql(desc = "Optional layer name; defaults to the inherited layer.")] layer: Option<
            String,
        >,
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
        if let Err(error) = self.edge.update_embeddings().await {
            error!(
                "Failed to re-embed an updated edge in graph {}: {error}",
                self.edge.graph.folder().local_path()
            );
        }

        Ok(true)
    }

    /// Append a property update to this edge at a specific time. If called
    /// after `addEdge`, the layer is inherited and does not need to be
    /// specified again.

    pub async fn add_updates(
        &self,
        #[graphql(desc = "Time of the update.")] time: GqlTimeInput,
        #[graphql(desc = "Optional `{key, value}` pairs attached to the event.")]
        properties: Option<Vec<GqlPropertyInput>>,
        #[graphql(desc = "Optional layer name; defaults to the inherited layer.")] layer: Option<
            String,
        >,
    ) -> Result<bool, GraphError> {
        let self_clone = self.clone();
        blocking_write(move || {
            self_clone.edge.add_updates(
                time.into_input_time(),
                as_properties(properties.unwrap_or(vec![]))?,
                layer.as_str(),
            )?;

            Ok::<_, GraphError>(())
        })
        .await?;

        self.post_mutation_ops().await;
        if let Err(error) = self.edge.update_embeddings().await {
            error!(
                "Failed to re-embed an updated edge in graph {}: {error}",
                self.edge.graph.folder().local_path()
            );
        }

        Ok(true)
    }
}

impl GqlMutableEdge {
    /// Post mutation operations.
    async fn post_mutation_ops(&self) {
        self.edge.graph.set_dirty(true);
    }
}

#[cfg(all(test, feature = "vectors"))]
mod tests {
    use super::*;
    use crate::{config::app_config::AppConfig, data::Data, paths::ExistingGraphFolder};
    use raphtory::{
        db::api::{storage::storage::Config, view::MaterializedGraph},
        vectors::{
            custom::{serve_custom_embedding, EmbeddingServer},
            storage::OpenAIEmbeddings,
            template::DocumentTemplate,
        },
    };
    use std::path::Path;
    use tempfile::TempDir;

    fn fake_embedding(_: &str) -> Vec<f32> {
        vec![1.0]
    }

    fn create_test_graph() -> MaterializedGraph {
        let graph = Graph::new();
        graph.into()
    }

    /// Struct returned by `create_mutable_graph` to make sure the directory is dropped last.
    /// Otherwise, the drop of the graph might fail as the directory no longer exists.
    pub struct GraphTestContext {
        mutable_graph: GqlMutableGraph,
        embedding_server: EmbeddingServer,
    }

    async fn create_mutable_graph(port: u16, work_dir: &Path) -> GraphTestContext {
        let graph = create_test_graph();
        let config = AppConfig::default();
        let data = Data::new(work_dir, &config, Config::default());

        let graph_name = "test_graph";

        let overwrite = false;
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(graph_name, overwrite)
            .unwrap();
        data.insert_graph(folder, graph).await.unwrap();
        let template = DocumentTemplate {
            node_template: Some("{{ name }} is a {{ node_type }}".to_string()),
            edge_template: Some("{{ src.name }} appeared with {{ dst.name}}".to_string()),
        };

        let embedding_server = serve_custom_embedding(None, port, fake_embedding).await;

        let config = OpenAIEmbeddings::new("whatever", format!("http://localhost:{port}"));
        let vector_cache = data.vector_cache.resolve().await.unwrap();
        let model = vector_cache.openai(config.into()).await.unwrap();
        data.vectorise_folder(
            &ExistingGraphFolder::try_from(data.work_dir_read().await, graph_name).unwrap(),
            &template,
            model,
        )
        .await
        .unwrap();

        let graph_with_vectors = data.get_graph_for_test(graph_name).await.unwrap();
        let mutable_graph = GqlMutableGraph::from(graph_with_vectors);

        GraphTestContext {
            mutable_graph,
            embedding_server,
        }
    }

    #[tokio::test]
    async fn test_add_nodes_empty_list() {
        let work_dir = TempDir::new().unwrap();
        {
            let context = create_mutable_graph(1745, work_dir.path()).await;

            let nodes = vec![];
            let result = context.mutable_graph.add_nodes(nodes).await;

            assert!(result.is_ok());
            assert!(result.unwrap());
            context.embedding_server.stop().await;
        }
    }

    #[tokio::test]
    #[ignore = "TODO: #2384"]
    async fn test_add_nodes_simple() {
        let work_dir = TempDir::new().unwrap();
        {
            let context = create_mutable_graph(1746, work_dir.path()).await;
            let mutable_graph = &context.mutable_graph;

            let nodes = vec![
                NodeAddition {
                    name: "node1".into(),
                    node_type: Some("test_node_type".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                    layer: None,
                },
                NodeAddition {
                    name: "node2".into(),
                    node_type: Some("test_node_type".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                    layer: None,
                },
            ];

            let result = mutable_graph.add_nodes(nodes).await;

            assert!(result.is_ok());
            assert!(result.unwrap());

            let embedding = fake_embedding("node1");
            let limit = 5;
            let result = mutable_graph
                .graph
                .vectors()
                .unwrap()
                .nodes_by_similarity(&embedding.into(), limit, None)
                .execute()
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().get_documents().await.unwrap().len(), 2);
            context.embedding_server.stop().await;
        }
    }

    #[tokio::test]
    #[ignore = "TODO: #2384"]
    async fn test_add_nodes_with_properties() {
        let work_dir = TempDir::new().unwrap();
        {
            let context = create_mutable_graph(1747, work_dir.path()).await;
            let mutable_graph = &context.mutable_graph;

            let nodes = vec![
                NodeAddition {
                    name: "complex_node_1".into(),
                    node_type: Some("employee".to_string()),
                    metadata: Some(vec![GqlPropertyInput {
                        key: "department".to_string(),
                        value: Value::Str("Sales".to_string()),
                    }]),
                    updates: Some(vec![
                        TemporalPropertyInput {
                            time: 0.into(),
                            properties: Some(vec![GqlPropertyInput {
                                key: "salary".to_string(),
                                value: Value::F64(50000.0),
                            }]),
                        },
                        TemporalPropertyInput {
                            time: 0.into(),
                            properties: Some(vec![GqlPropertyInput {
                                key: "salary".to_string(),
                                value: Value::F64(55000.0),
                            }]),
                        },
                    ]),
                    layer: None,
                },
                NodeAddition {
                    name: "complex_node_2".into(),
                    node_type: Some("employee".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                    layer: None,
                },
                NodeAddition {
                    name: "complex_node_3".into(),
                    node_type: Some("employee".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: Some(vec![GqlPropertyInput {
                            key: "salary".to_string(),
                            value: Value::F64(55000.0),
                        }]),
                    }]),
                    layer: None,
                },
            ];

            let result = mutable_graph.add_nodes(nodes).await;

            assert!(result.is_ok());
            assert!(result.unwrap());

            let embedding = fake_embedding("complex_node_1");
            let limit = 5;
            let result = mutable_graph
                .graph
                .vectors()
                .unwrap()
                .nodes_by_similarity(&embedding.into(), limit, None)
                .execute()
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().get_documents().await.unwrap().len(), 3);
            context.embedding_server.stop().await;
        }
    }

    #[tokio::test]
    #[ignore = "TODO: #2384"]
    async fn test_add_edges_simple() {
        let work_dir = TempDir::new().unwrap();
        {
            let context = create_mutable_graph(1748, work_dir.path()).await;
            let mutable_graph = &context.mutable_graph;

            // First add some nodes.
            let nodes = vec![
                NodeAddition {
                    name: "node1".into(),
                    node_type: Some("person".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                    layer: None,
                },
                NodeAddition {
                    name: "node2".into(),
                    node_type: Some("person".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                    layer: None,
                },
            ];

            let result = mutable_graph.add_nodes(nodes).await;
            assert!(result.is_ok());

            // Now add edges between them.
            let edges = vec![
                EdgeAddition {
                    src: "node1".into(),
                    dst: "node2".into(),
                    layer: Some("friendship".to_string()),
                    metadata: Some(vec![GqlPropertyInput {
                        key: "strength".to_string(),
                        value: Value::F64(0.8),
                    }]),
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                },
                EdgeAddition {
                    src: "node2".into(),
                    dst: "node1".into(),
                    layer: Some("friendship".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 0.into(),
                        properties: None,
                    }]),
                },
            ];

            let result = mutable_graph.add_edges(edges).await;

            assert!(result.is_ok());
            assert!(result.unwrap());

            // TODO: #2380 (embeddings aren't working right now)
            // Test that edge embeddings were generated.
            let embedding = fake_embedding("node1 appeared with node2");
            let limit = 5;
            let result = mutable_graph
                .graph
                .vectors()
                .unwrap()
                .edges_by_similarity(&embedding.into(), limit, None)
                .execute()
                .await;

            assert!(result.is_ok());
            assert_eq!(result.unwrap().get_documents().await.unwrap().len(), 2);
            context.embedding_server.stop().await;
        }
    }

    /// Every write path embeds inline, and none of them may fail the write when embedding is
    /// unavailable — but the entity is then missing from the index, so the failure has to be
    /// reported (a warning naming the graph) instead of discarded. This drives each of those
    /// paths with the embedding server stopped.
    #[tokio::test]
    async fn test_every_write_path_survives_a_failing_embedding() {
        let work_dir = TempDir::new().unwrap();
        {
            let context = create_mutable_graph(1752, work_dir.path()).await;
            let graph = &context.mutable_graph;

            // seed while embedding still works, so there is an index to compare against
            graph
                .add_node(0.into(), "seed".into(), None, Some("person".into()), None)
                .await
                .unwrap();
            let indexed_before = documents(graph).await;

            // from here on every embedding attempt fails
            context.embedding_server.stop().await;

            let node = graph
                .add_node(1.into(), "added".into(), None, Some("person".into()), None)
                .await
                .expect("add_node must not fail because embedding failed");
            graph
                .create_node(
                    1.into(),
                    "created".into(),
                    None,
                    Some("person".into()),
                    None,
                )
                .await
                .expect("create_node must not fail because embedding failed");
            graph
                .add_nodes(vec![NodeAddition {
                    name: "batched".into(),
                    node_type: Some("person".to_string()),
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 1.into(),
                        properties: None,
                    }]),
                    layer: None,
                }])
                .await
                .expect("add_nodes must not fail because embedding failed");
            let edge = graph
                .add_edge(1.into(), "added".into(), "created".into(), None, None)
                .await
                .expect("add_edge must not fail because embedding failed");
            graph
                .add_edges(vec![EdgeAddition {
                    src: "batched".into(),
                    dst: "seed".into(),
                    layer: None,
                    metadata: None,
                    updates: Some(vec![TemporalPropertyInput {
                        time: 1.into(),
                        properties: None,
                    }]),
                }])
                .await
                .expect("add_edges must not fail because embedding failed");

            // node handle paths
            node.add_updates(2.into(), None, None)
                .await
                .expect("node add_updates must not fail because embedding failed");
            // a node with no type yet, so setting one is a legal update
            graph
                .add_node(1.into(), "typeless".into(), None, None, None)
                .await
                .unwrap()
                .set_node_type("person".to_string())
                .await
                .expect("set_node_type must not fail because embedding failed");
            node.add_metadata(vec![GqlPropertyInput {
                key: "team".to_string(),
                value: Value::Str("red".to_string()),
            }])
            .await
            .expect("node add_metadata must not fail because embedding failed");
            node.update_metadata(vec![GqlPropertyInput {
                key: "team".to_string(),
                value: Value::Str("blue".to_string()),
            }])
            .await
            .expect("node update_metadata must not fail because embedding failed");

            // edge handle paths
            edge.add_updates(3.into(), None, None)
                .await
                .expect("edge add_updates must not fail because embedding failed");
            edge.add_metadata(
                vec![GqlPropertyInput {
                    key: "weight".to_string(),
                    value: Value::Str("heavy".to_string()),
                }],
                None,
            )
            .await
            .expect("edge add_metadata must not fail because embedding failed");
            edge.update_metadata(
                vec![GqlPropertyInput {
                    key: "weight".to_string(),
                    value: Value::Str("light".to_string()),
                }],
                None,
            )
            .await
            .expect("edge update_metadata must not fail because embedding failed");
            edge.delete(4.into(), None)
                .await
                .expect("edge delete must not fail because embedding failed");

            // the writes landed in the graph
            assert!(
                graph.graph.node("added").is_some(),
                "the write itself must be applied even when embedding fails"
            );
            // ...and none of them reached the index, which is why the warning matters
            assert_eq!(
                documents(graph).await,
                indexed_before,
                "nothing should have been indexed while embedding was unavailable"
            );
        }
    }

    /// Documents currently in the node index.
    async fn documents(graph: &GqlMutableGraph) -> usize {
        graph
            .graph
            .vectors()
            .unwrap()
            .nodes_by_similarity(&fake_embedding("anything").into(), 100, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap()
            .len()
    }
}
