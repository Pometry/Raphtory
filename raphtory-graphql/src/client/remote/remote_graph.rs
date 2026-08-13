use crate::{
    client::{
        graphql_transport::GraphqlTransport,
        op::{
            input_time_from_parts, AddEdge as AddEdgeOp, AddGraphMetadata as AddGraphMetadataOp,
            AddGraphProperty as AddGraphPropertyOp, AddNode as AddNodeOp,
            CreateNode as CreateNodeOp, DeleteEdge as DeleteEdgeOp, HandleCtx, InputTime, Op,
            ReadExpr, UpdateGraphMetadata as UpdateGraphMetadataOp, ViewOp, WriteOp,
        },
        remote_client::RemoteClient,
        remote_edge::RemoteEdge,
        remote_edges::RemoteEdges,
        remote_metadata::{RemoteMetadata, RemoteProperties},
        remote_node::RemoteNode,
        remote_nodes::RemoteNodes,
        remote_schema::RemoteGraphSchema,
        transport::{
            expect_bool, expect_edge_list, expect_i64, expect_optional_event_time,
            expect_optional_i64, expect_string, expect_string_list, Transport,
        },
        ClientError,
    },
    model::graph::filtering::GqlFilter,
};
use raphtory::errors::GraphError;
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID},
    storage::timeindex::{AsTime, EventTime},
    utils::time::TryIntoInputTime,
};
use std::{collections::HashMap, sync::Arc};

/// A handle to a remote graph on the server.
///
/// Holds an accumulating `ReadExpr` for view construction. View-op methods
/// (`.window()`, `.layer()`, `.at()`, ...) append lazily — no RPC. Selection
/// methods `.node()` and `.edge()` fire one RPC each (`hasNode` / `hasEdge`
/// against the current view chain) to validate that the id resolves, raising
/// `ClientError::NotFound` if not. Terminals on the child types (e.g.
/// `RemoteNode::degree`) fire their own RPC evaluating the accumulated
/// expression via the transport.
#[derive(Clone)]
pub struct RemoteGraph {
    pub path: String,
    pub transport: Arc<dyn Transport>,
    /// The read expression built so far. Starts as `Root { path }`.
    pub expr: Arc<ReadExpr>,
}

impl RemoteGraph {
    /// Construct a `RemoteGraph` handle for the graph at `path`, using the
    /// given `RemoteClient` for transport. Wraps the client in a
    /// `GraphqlTransport`; starts the accumulated read expression at
    /// `Root { path }`.
    pub fn new(path: String, client: RemoteClient) -> Self {
        let transport: Arc<dyn Transport> = Arc::new(GraphqlTransport::new(client));
        let expr = ReadExpr::Root { path: path.clone() };
        Self {
            path,
            transport,
            expr: expr.into(),
        }
    }

    /// Time-window the graph. Lazy — builds up the read expression, no RPC.
    pub fn window(&self, start: InputTime, end: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::Window { start, end }.apply(self.expr.clone()))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(
            ViewOp::Layer {
                name: name.to_string(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::At { time }.apply(self.expr.clone()))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::Before { time }.apply(self.expr.clone()))
    }

    /// Restrict to events strictly after the given time (exclusive). Lazy — no RPC.
    pub fn after(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::After { time }.apply(self.expr.clone()))
    }

    /// Restrict to the latest state — no args. Lazy — no RPC.
    pub fn latest(&self) -> RemoteGraph {
        self.with_expr(ViewOp::Latest.apply(self.expr.clone()))
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> RemoteGraph {
        self.with_expr(ViewOp::SnapshotLatest.apply(self.expr.clone()))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::SnapshotAt { time }.apply(self.expr.clone()))
    }

    /// Exclude a specific layer from the view. Lazy — no RPC.
    pub fn exclude_layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(
            ViewOp::ExcludeLayer {
                name: name.to_string(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Shrink both start and end of the current window (intersection, never widens).
    /// Lazy — no RPC.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::ShrinkWindow { start, end }.apply(self.expr.clone()))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::ShrinkStart { start }.apply(self.expr.clone()))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: InputTime) -> RemoteGraph {
        self.with_expr(ViewOp::ShrinkEnd { end }.apply(self.expr.clone()))
    }

    /// Restrict to the "valid" subgraph (event-graph filter). Lazy — no RPC.
    pub fn valid(&self) -> RemoteGraph {
        self.with_expr(ReadExpr::Valid {
            input: self.expr.clone(),
        })
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> RemoteGraph {
        self.with_expr(ViewOp::DefaultLayer.apply(self.expr.clone()))
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(
            ViewOp::Layers {
                names: names.into(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Exclude the given set of layers from the view. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(
            ViewOp::ExcludeLayers {
                names: names.into(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    pub fn valid_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(
            ViewOp::ValidLayers {
                names: names.into(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    pub fn exclude_valid_layer(&self, name: impl ToString) -> RemoteGraph {
        self.with_expr(
            ViewOp::ExcludeValidLayer {
                name: name.to_string(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> RemoteGraph {
        self.with_expr(
            ViewOp::ExcludeValidLayers {
                names: names.into(),
            }
            .apply(self.expr.clone()),
        )
    }

    /// Restrict to a subgraph induced by the given node ids. Lazy — no RPC.
    pub fn subgraph(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::Subgraph {
            input: self.expr.clone(),
            nodes: nodes.into(),
        })
    }

    /// Restrict to nodes matching one of the given node types. Lazy — no RPC.
    pub fn subgraph_node_types(&self, node_types: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::SubgraphNodeTypes {
            input: self.expr.clone(),
            node_types: node_types.into(),
        })
    }

    /// Return a filtered graph view. Takes a general filter expression —
    /// node/edge predicates, graph views, or and/or/not combinations of them
    /// (`and` is an intersection). Mirrors the local `Graph.filter`. Lazy —
    /// no RPC.
    pub fn filter(
        &self,
        filter: impl TryInto<GqlFilter, Error = GraphError>,
    ) -> Result<RemoteGraph, ClientError> {
        Ok(self.with_expr(ReadExpr::Filtered {
            input: self.expr.clone(),
            filter: Arc::new(filter.try_into()?),
        }))
    }

    /// Exclude the given nodes from the view. Lazy — no RPC.
    pub fn exclude_nodes(&self, nodes: Vec<String>) -> RemoteGraph {
        self.with_expr(ReadExpr::ExcludeNodes {
            input: self.expr.clone(),
            nodes: nodes.into(),
        })
    }

    /// Terminal: count of nodes under the current view. Fires one RPC.
    pub async fn count_nodes(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountNodes {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "countNodes")
    }

    /// Terminal: count of edges under the current view. Fires one RPC.
    pub async fn count_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountEdges {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "countEdges")
    }

    /// Terminal: earliest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn earliest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "earliestTime")
    }

    /// Terminal: latest event timestamp under the current view. Returns
    /// `None` if the view has no events. Fires one RPC.
    pub async fn latest_time(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::LatestTime {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "latestTime")
    }

    /// Terminal: view start bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn start(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::Start {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "start")
    }

    /// Terminal: view end bound. Returns `None` for an unbounded view.
    /// Fires one RPC.
    pub async fn end(&self) -> Result<Option<EventTime>, ClientError> {
        let op = Op::Read(ReadExpr::End {
            input: self.expr.clone(),
        });
        expect_optional_event_time(self.transport.execute(&op).await?, "end")
    }

    /// Terminal: does the graph have a node with this id? Fires one RPC.
    pub async fn has_node(&self, id: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasNode {
            input: self.expr.clone(),
            id: id.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasNode")
    }

    /// Terminal: does the graph have an edge `(src, dst)`? Fires one RPC.
    pub async fn has_edge(
        &self,
        src: impl ToString,
        dst: impl ToString,
    ) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasEdge {
            input: self.expr.clone(),
            src: src.to_string(),
            dst: dst.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasEdge")
    }

    /// Terminal: total temporal-edge count (edge updates) under the current view.
    /// Fires one RPC.
    pub async fn count_temporal_edges(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::CountTemporalEdges {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "countTemporalEdges")
    }

    /// Terminal: graph name. Fires one RPC.
    pub async fn name(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Name {
            input: self.expr.clone(),
        });
        expect_string(self.transport.execute(&op).await?, "name")
    }

    /// Terminal: graph path. Fires one RPC.
    pub async fn path(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Path {
            input: self.expr.clone(),
        });
        expect_string(self.transport.execute(&op).await?, "path")
    }

    /// Terminal: parent namespace of the graph path. Fires one RPC.
    pub async fn namespace(&self) -> Result<String, ClientError> {
        let op = Op::Read(ReadExpr::Namespace {
            input: self.expr.clone(),
        });
        expect_string(self.transport.execute(&op).await?, "namespace")
    }

    /// Terminal: graph creation timestamp — never null (server metadata).
    /// Fires one RPC.
    pub async fn created(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::Created {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "created")
    }

    /// Terminal: graph last-opened timestamp — never null. Fires one RPC.
    pub async fn last_opened(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::LastOpened {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "lastOpened")
    }

    /// Terminal: graph last-updated timestamp — never null. Fires one RPC.
    pub async fn last_updated(&self) -> Result<i64, ClientError> {
        let op = Op::Read(ReadExpr::LastUpdated {
            input: self.expr.clone(),
        });
        expect_i64(self.transport.execute(&op).await?, "lastUpdated")
    }

    /// Terminal: list of unique layer names present in this graph. Fires one RPC.
    pub async fn unique_layers(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::UniqueLayers {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "uniqueLayers")
    }

    /// Terminal: whether this view contains a layer named `name`. Fires one RPC.
    pub async fn has_layer(&self, name: impl ToString) -> Result<bool, ClientError> {
        let op = Op::Read(ReadExpr::HasLayer {
            input: self.expr.clone(),
            name: name.to_string(),
        });
        expect_bool(self.transport.execute(&op).await?, "hasLayer")
    }

    /// Terminal: the size of the window covered by this view (`end - start`),
    /// or `None` for an unbounded view. Fires one RPC.
    pub async fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::WindowSize {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "windowSize")
    }

    /// Terminal: earliest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub async fn earliest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::EarliestEdgeTime {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "earliestEdgeTime")
    }

    /// Terminal: latest edge event time under the current view. Returns
    /// `None` if the view has no edge events. Fires one RPC.
    pub async fn latest_edge_time(&self) -> Result<Option<i64>, ClientError> {
        let op = Op::Read(ReadExpr::LatestEdgeTime {
            input: self.expr.clone(),
        });
        expect_optional_i64(self.transport.execute(&op).await?, "latestEdgeTime")
    }

    /// Internal helper: clone `self` with a new `expr`. Keeps the view-op
    /// builder methods (`.window`, `.layer`, `.at`, ...) as one-liners.
    fn with_expr(&self, expr: ReadExpr) -> RemoteGraph {
        RemoteGraph {
            path: self.path.clone(),
            transport: self.transport.clone(),
            expr: Arc::new(expr),
        }
    }

    /// Returns a remote node reference for the given node id.
    ///
    /// **Fires one RPC** — a `hasNode` check against the current view chain,
    /// raising `ClientError::NotFound` if the node isn't visible under the
    /// current view. This guarantees that any handle you hold refers to a
    /// node the server actually resolved at handle-construction time; race
    /// conditions where the node is deleted between selection and terminal
    /// are still caught downstream via the null-intermediate NotFound path
    /// in `parse_read`.
    ///
    /// Server-returned handles (from `.nodes.collect()`, `.neighbours`, etc.)
    /// bypass this check — those ids came from the server, so we trust them.
    pub async fn node(&self, id: impl ToString) -> Result<Option<RemoteNode>, ClientError> {
        let id_str = id.to_string();
        let check = Op::Read(ReadExpr::HasNode {
            input: self.expr.clone(),
            id: id_str.clone(),
        });
        let exists = expect_bool(self.transport.execute(&check).await?, "hasNode")?;
        if !exists {
            return Ok(None);
        }
        Ok(Some(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: self.expr.clone(),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        )))
    }

    /// Returns the collection of all nodes in the graph, evaluated under the
    /// current view chain. Lazy — no RPC.
    pub fn nodes(&self) -> RemoteNodes {
        RemoteNodes::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Nodes {
                input: self.expr.clone(),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns the metadata container of this graph — non-temporal
    /// properties whose values don't depend on time. Lazy — no RPC.
    pub fn metadata(&self) -> RemoteMetadata {
        RemoteMetadata::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Metadata {
                input: self.expr.clone(),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Terminal: fetch the graph's schema — node types, edge layers, and
    /// their observed property/metadata schemas. Fires one RPC and
    /// materializes the entire tree eagerly (unlike other containers,
    /// which lazy-fetch on demand).
    pub async fn schema(&self) -> Result<RemoteGraphSchema, ClientError> {
        let op = Op::Read(ReadExpr::Schema {
            input: self.expr.clone(),
        });
        let prop = self
            .transport
            .execute(&op)
            .await?
            .ok_or_else(|| ClientError::InvalidResponse("schema returned null".into()))?;
        RemoteGraphSchema::from_prop(prop)
    }

    /// Terminal: the set-intersection of neighbours across the given node
    /// ids. Fires one RPC. Server behaviour:
    /// - empty input list → empty result
    /// - **ids that don't exist in the current view are silently dropped**;
    ///   the intersection is taken over the remaining ids
    /// - if no ids remain after dropping missing ones → empty result
    ///
    /// Each returned handle rebases at `self.expr`, so downstream terminals
    /// inherit the same view chain.
    pub async fn shared_neighbours(
        &self,
        ids: Vec<String>,
    ) -> Result<Vec<RemoteNode>, ClientError> {
        let op = Op::Read(ReadExpr::SharedNeighbours {
            input: self.expr.clone(),
            ids,
        });
        let names = expect_string_list(self.transport.execute(&op).await?, "sharedNeighbours")?;
        Ok(names
            .into_iter()
            .map(|name| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    name.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: self.expr.clone(),
                        id: name,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: the nodes whose latest property value equals the given value
    /// for **every** `(name, value)` entry in `properties_dict`. Mirrors the
    /// local `Graph.find_nodes`. Fires one RPC. Each returned handle rebases at
    /// `self.expr`, inheriting the current view chain.
    pub async fn find_nodes(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<RemoteNode>, ClientError> {
        let op = Op::Read(ReadExpr::FindNodes {
            input: self.expr.clone(),
            properties: properties_dict,
        });
        let names = expect_string_list(self.transport.execute(&op).await?, "findNodes")?;
        Ok(names
            .into_iter()
            .map(|name| {
                RemoteNode::with_expr(
                    self.path.clone(),
                    name.clone(),
                    self.transport.clone(),
                    ReadExpr::Node {
                        input: self.expr.clone(),
                        id: name,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: the edges whose latest property value equals the given value
    /// for **every** `(name, value)` entry in `properties_dict`. Mirrors the
    /// local `Graph.find_edges`. Fires one RPC. Each returned handle rebases at
    /// `self.expr`, inheriting the current view chain.
    pub async fn find_edges(
        &self,
        properties_dict: HashMap<String, Prop>,
    ) -> Result<Vec<RemoteEdge>, ClientError> {
        let op = Op::Read(ReadExpr::FindEdges {
            input: self.expr.clone(),
            properties: properties_dict,
        });
        let pairs = expect_edge_list(self.transport.execute(&op).await?, "findEdges")?;
        Ok(pairs
            .into_iter()
            .map(|(src, dst)| {
                RemoteEdge::with_expr(
                    self.path.clone(),
                    src.clone(),
                    dst.clone(),
                    self.transport.clone(),
                    ReadExpr::Edge {
                        input: self.expr.clone(),
                        src,
                        dst,
                    },
                    HandleCtx::new(self.expr.clone()),
                )
            })
            .collect())
    }

    /// Terminal: all node types present in the graph. Mirrors the local
    /// `Graph.get_all_node_types`. Fires one RPC.
    pub async fn get_all_node_types(&self) -> Result<Vec<String>, ClientError> {
        let op = Op::Read(ReadExpr::GetAllNodeTypes {
            input: self.expr.clone(),
        });
        expect_string_list(self.transport.execute(&op).await?, "getAllNodeTypes")
    }

    /// Returns the full properties container of this graph — includes both
    /// temporal properties and metadata. Lazy — no RPC.
    pub fn properties(&self) -> RemoteProperties {
        RemoteProperties::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Properties {
                input: self.expr.clone(),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns the collection of all edges in the graph, evaluated under the
    /// current view chain. Lazy — no RPC.
    pub fn edges(&self) -> RemoteEdges {
        RemoteEdges::with_expr(
            self.path.clone(),
            self.transport.clone(),
            ReadExpr::Edges {
                input: self.expr.clone(),
            },
            HandleCtx::new(self.expr.clone()),
        )
    }

    /// Returns a remote edge reference for the given source and destination node ids.
    ///
    /// **Fires one RPC** — a `hasEdge` check against the current view chain,
    /// raising `ClientError::NotFound` if the edge isn't visible under the
    /// current view. Same guarantee and rationale as `.node()`.
    pub async fn edge(
        &self,
        src: impl ToString,
        dst: impl ToString,
    ) -> Result<Option<RemoteEdge>, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let check = Op::Read(ReadExpr::HasEdge {
            input: self.expr.clone(),
            src: src_str.clone(),
            dst: dst_str.clone(),
        });
        let exists = expect_bool(self.transport.execute(&check).await?, "hasEdge")?;
        if !exists {
            return Ok(None);
        }
        Ok(Some(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: self.expr.clone(),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        )))
    }

    /// Add a node to the graph at the given timestamp.
    ///
    /// Upsert-like: if a node with this id already exists, additional updates
    /// are appended at the given time. Use `create_node` for strict-create.
    ///
    /// Fires one RPC. Returns a trusted `RemoteNode` handle for the added
    /// node — no follow-up `hasNode` validation is fired, since the server
    /// just confirmed the write.
    pub async fn add_node<G: Into<GID> + ToString, T: TryIntoInputTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::AddNode(AddNodeOp {
            path: self.path.clone(),
            time: timestamp.try_into_input_time()?,
            id: id_str.clone(),
            properties,
            node_type,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: self.expr.clone(),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Create a new node at the given timestamp. Fails if a node with this
    /// id already exists — use `add_node` for upsert semantics.
    ///
    /// Fires one RPC. Returns a trusted `RemoteNode` handle for the created
    /// node — no follow-up `hasNode` validation is fired.
    pub async fn create_node<G: Into<GID> + ToString, T: TryIntoInputTime>(
        &self,
        timestamp: T,
        id: G,
        properties: Option<HashMap<String, Prop>>,
        node_type: Option<String>,
        layer: Option<String>,
    ) -> Result<RemoteNode, ClientError> {
        let id_str = id.to_string();
        let op = Op::Write(WriteOp::CreateNode(CreateNodeOp {
            path: self.path.clone(),
            time: timestamp.try_into_input_time()?,
            id: id_str.clone(),
            properties,
            node_type,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteNode::with_expr(
            self.path.clone(),
            id_str.clone(),
            self.transport.clone(),
            ReadExpr::Node {
                input: self.expr.clone(),
                id: id_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Add an edge to the graph at the given timestamp.
    ///
    /// Upsert-like: if an edge with these endpoints already exists, additional
    /// updates are appended at the given time (optionally on a specific layer).
    ///
    /// Fires one RPC. Returns a trusted `RemoteEdge` handle — no follow-up
    /// `hasEdge` validation is fired, since the server just confirmed the write.
    pub async fn add_edge<G: Into<GID> + ToString, T: TryIntoInputTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::AddEdge(AddEdgeOp {
            path: self.path.clone(),
            time: timestamp.try_into_input_time()?,
            src: src_str.clone(),
            dst: dst_str.clone(),
            properties,
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: self.expr.clone(),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }

    /// Add temporal properties on the graph itself (not on any node/edge) at
    /// the given timestamp. Distinct from `add_metadata`, which is non-temporal.
    ///
    /// Fires one RPC.
    pub async fn add_properties(
        &self,
        timestamp: impl TryIntoInputTime,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphProperty(AddGraphPropertyOp {
            path: self.path.clone(),
            time: timestamp.try_into_input_time()?,
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Add non-temporal metadata on the graph itself. Values persist for the
    /// lifetime of the graph and don't depend on any timestamp.
    ///
    /// Fires one RPC.
    pub async fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::AddGraphMetadata(AddGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Overwrite existing metadata on the graph. Unlike `add_metadata`, this
    /// replaces the value for each supplied key rather than adding.
    ///
    /// Fires one RPC.
    pub async fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), ClientError> {
        let op = Op::Write(WriteOp::UpdateGraphMetadata(UpdateGraphMetadataOp {
            path: self.path.clone(),
            properties,
        }));
        self.transport.execute(&op).await?;
        Ok(())
    }

    /// Mark an edge as deleted at the given timestamp (optionally on a
    /// specific layer). The edge remains queryable in earlier views; only
    /// events at or after `timestamp` see it as deleted.
    ///
    /// Fires one RPC. Returns a trusted `RemoteEdge` handle for the deleted
    /// edge — subsequent reads on it observe the deletion.
    pub async fn delete_edge<G: Into<GID> + ToString, T: TryIntoInputTime>(
        &self,
        timestamp: T,
        src: G,
        dst: G,
        layer: Option<String>,
    ) -> Result<RemoteEdge, ClientError> {
        let src_str = src.to_string();
        let dst_str = dst.to_string();
        let op = Op::Write(WriteOp::DeleteEdge(DeleteEdgeOp {
            path: self.path.clone(),
            time: timestamp.try_into_input_time()?,
            src: src_str.clone(),
            dst: dst_str.clone(),
            layer,
        }));
        self.transport.execute(&op).await?;
        Ok(RemoteEdge::with_expr(
            self.path.clone(),
            src_str.clone(),
            dst_str.clone(),
            self.transport.clone(),
            ReadExpr::Edge {
                input: self.expr.clone(),
                src: src_str,
                dst: dst_str,
            },
            HandleCtx::new(self.expr.clone()),
        ))
    }
}
