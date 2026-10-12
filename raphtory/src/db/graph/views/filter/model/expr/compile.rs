//! From expression data to a filter the engine can apply.
//!
//! A tree compiles straight to the ops that evaluate it: a term's views fold
//! into a view chain over the graph and the term becomes the op that reads
//! through it, and every other node of a value expression becomes one op
//! over the ops of its children, built by the entity's [`OpFamily`]. A
//! `Bool`-typed value is a filter on its entity through its [`Leaf`]; this
//! module adds what only a whole filter knows: which entities each leg
//! answers for, how `and`, `or` and `not` combine those answers, and where a
//! view applies.

use super::{
    builder::{Chain, GraphLeaf},
    split::{both_ends, closed_edges, needs_operand, view_below_top_level},
    stream::{
        EdgeHistory, EndpointHistory, NodeHistory, StreamedAggEdgeOp, StreamedAggNodeOp,
        StreamedAllEdgeOp, StreamedAllNodeOp, StreamedAnyEdgeOp, StreamedAnyNodeOp,
    },
    Agg, EdgeExpr, EdgeLeaf, EdgePredicate, ExplodedEdgeExpr, ExplodedEdgeLeaf, Expr, Field,
    FilterExpr, NodeExpr, NodeLeaf, Quantifier, Question, SplitFilter, ValueTest, ViewOp,
};
use crate::{
    db::{
        api::{
            state::{
                ops::{
                    filter::NodeExistsOp,
                    node::{Name, Type},
                    Const, Degree, HistoryOp, NodeFilterOp,
                },
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView, IntoDynGraphArc},
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                after_bounds, at_bounds, before_bounds,
                edge_expr::{
                    ops::{
                        AndEdgeOp, AndValueEdgeOp, BinaryValueEdgeOp, EdgeEndpointNodeOp,
                        EdgeExistsOp, EdgeMetaOp, EdgePropOp, IsActiveEdgePropOp,
                        IsDeletedEdgePropOp, IsSelfLoopEdgePropOp, IsValidEdgePropOp, NotEdgeOp,
                        OrEdgeOp, OrValueEdgeOp, TemporalEdgePropOp, TruthyEdgeOp,
                        UnaryValueEdgeOp,
                    },
                    EdgeOp,
                },
                edge_filter::{EdgeFilter, Endpoint},
                exploded_edge_filter::ExplodedEdgeFilter,
                filter_operator::UnaryOp,
                graph_filter::GraphFilter,
                latest_filter::Latest,
                layered_filter::{
                    DefaultLayer, ExcludeLayers, ExcludeValidLayers, Layered, ValidLayers,
                },
                node_expr::{
                    ops::{
                        AllEdgeOp, AllNodeOp, AndValueNodeOp, AnyEdgeOp, AnyNodeOp, AvgEdgeOp,
                        AvgNodeOp, BinaryValueNodeOp, DomainNodeOp, EarliestEdgeOp, EarliestNodeOp,
                        FirstEdgeOp, FirstNodeOp, LastEdgeOp, LastNodeOp, LatestEdgeOp,
                        LatestNodeOp, LenEdgeOp, LenNodeOp, MaxEdgeOp, MaxNodeOp, MinEdgeOp,
                        MinNodeOp, NodeIdOp, NodeMetaOp, NodePropOp, OrValueNodeOp, ShownNodeOp,
                        SumEdgeOp, SumNodeOp, TemporalNodePropOp, UnaryValueNodeOp,
                    },
                    pushdown::IndexTerm,
                    typing::{
                        cmp_kernel, comparison_shape, not_kernel, presence_kernel, qualified_type,
                        require_bool, set_kernel, set_shape, str_kernel, string_shape, truthy,
                        BinaryKernel, SetMembers, Shape, ShapeError, UnaryKernel,
                    },
                },
                node_filter::NodeFilter,
                require_aggregable, resolved_prop_type,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                subgraph_filter::{ExcludeNodes, Subgraph, SubgraphNodeTypes, Valid},
                windowed_filter::{ShrinkEnd, ShrinkStart, Windowed},
                CreateView, DynCreateView,
            },
            node_filtered_graph::NodeFilteredGraph,
            or_filtered_graph::OrFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoProp, Prop, PropType},
    storage::timeindex::EventTime,
};
use std::{
    fmt::Debug,
    sync::{Arc, LazyLock},
};

// ── leaves ───────────────────────────────────────────────────────────────────

/// The terms an entity offers. Implemented by the leaf enum of each entity.
pub trait Leaf: Clone + Debug + PartialEq + Send + Sync + 'static {
    /// The ops the values of this entity compile to.
    type Ops: OpFamily;

    /// The op this term reads against `graph`: its name resolves to an id
    /// once, and its views build over `graph`.
    fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<Self::Ops as OpFamily>::Value<'g>>, GraphError>;

    /// The history this term reads against `graph`, for a consumer that walks
    /// the values instead of taking them as one list: `None` unless the term
    /// is a temporal property.
    fn history_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<<Self::Ops as OpFamily>::History<'g>>>, GraphError>;

    // What a yes/no tree over this leaf type is as a filter; see
    // `CreateFilter for Expr`.

    /// The per-node test the tree is. Only a node tree says which nodes
    /// belong in a node collection.
    fn node_filter<'g>(
        _expr: &Expr<Self>,
        _graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    /// The per-edge test the tree is: which edges stay.
    fn edge_filter<'g>(
        expr: &Expr<Self>,
        graph: DynGraphArc<'g>,
    ) -> Result<DynEdgeFilter<'g>, GraphError>;

    /// The graph that keeps what the tree keeps.
    fn graph_filter<'g>(
        expr: &Expr<Self>,
        graph: DynGraphArc<'g>,
    ) -> Result<DynGraphArc<'g>, GraphError>;

    /// The latest value of a property, or its history when `temporal`, seen
    /// through `views`.
    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self;

    /// A metadata entry seen through `views`.
    fn metadata(views: Vec<ViewOp>, name: String) -> Self;

    /// Whether the entity is active inside `views`.
    fn is_active(views: Vec<ViewOp>) -> Self;

    /// The filter a yes/no expression over this leaf type is, as data.
    fn filter(expr: Expr<Self>) -> FilterExpr;

    // What a term knows about itself before any graph is at hand; see
    // `Expr::prop_type` and the rest. Each defaults to "nothing known".

    /// The type the term reads, when it is fixed whatever the graph.
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }

    /// Whether the term can be missing.
    fn nullable(&self) -> bool {
        true
    }

    /// The term a node index covers, when this is one read without a view.
    fn index_term(&self) -> Option<IndexTerm> {
        None
    }
}

/// A view chain over the graph, as the tree's view ops describe it, over the
/// identity view `root`, applied in order. A window inside a window narrows and
/// never widens, because the graph's own `window` intersects with the view it
/// is applied to, as `graph.window(..).window(..)` does.
fn view_chain(root: Arc<dyn DynCreateView>, views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    views.iter().fold(root, |chain, op| match op {
        ViewOp::Window { start, end } => window((*start, *end), chain),
        ViewOp::At(t) => window(at_bounds(*t), chain),
        ViewOp::After(t) => window(after_bounds(*t), chain),
        ViewOp::Before(t) => window(before_bounds(*t), chain),
        ViewOp::Latest => Arc::new(Latest::new(chain)),
        ViewOp::SnapshotAt(t) => Arc::new(SnapshotAt::new(*t, chain)),
        ViewOp::SnapshotLatest => Arc::new(SnapshotLatest::new(chain)),
        ViewOp::Layers(layer) => Arc::new(Layered::new(layer.clone(), chain)),
        ViewOp::DefaultLayer => Arc::new(DefaultLayer::new(chain)),
        ViewOp::ExcludeLayers(layer) => Arc::new(ExcludeLayers::new(layer.clone(), chain)),
        ViewOp::ValidLayers(layer) => Arc::new(ValidLayers::new(layer.clone(), chain)),
        ViewOp::ExcludeValidLayers(layer) => {
            Arc::new(ExcludeValidLayers::new(layer.clone(), chain))
        }
        ViewOp::ShrinkStart(t) => Arc::new(ShrinkStart::new(*t, chain)),
        ViewOp::ShrinkEnd(t) => Arc::new(ShrinkEnd::new(*t, chain)),
        ViewOp::ExcludeNodes(ids) => Arc::new(ExcludeNodes::new(ids.clone(), chain)),
        ViewOp::Subgraph(ids) => Arc::new(Subgraph::new(ids.clone(), chain)),
        ViewOp::SubgraphNodeTypes(types) => Arc::new(SubgraphNodeTypes::new(types.clone(), chain)),
        ViewOp::Valid => Arc::new(Valid::new(chain)),
    })
}

fn window(
    (start, end): (EventTime, EventTime),
    chain: Arc<dyn DynCreateView>,
) -> Arc<dyn DynCreateView> {
    Arc::new(Windowed::new(start, end, chain))
}

fn node_chain(views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    view_chain(Arc::new(NodeFilter), views)
}

fn edge_chain(exploded: bool, views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    if exploded {
        view_chain(Arc::new(ExplodedEdgeFilter), views)
    } else {
        view_chain(Arc::new(EdgeFilter), views)
    }
}

/// Whether a term read through `views` can miss an entity the incoming graph
/// shows, as `CreateView::narrows` of the chain over `views` answers: every
/// view can hide one, and the bare entity root hides none.
fn narrows(views: &[ViewOp]) -> bool {
    !views.is_empty()
}

/// A property or metadata entry as the term an index covers. A view can hide
/// what the index holds, so a term read through one is not covered.
fn indexed(views: &[ViewOp], name: &str, metadata: bool, ever: bool) -> Option<IndexTerm> {
    (!narrows(views)).then(|| IndexTerm::Property {
        name: name.to_owned(),
        metadata,
        ever,
    })
}

/// A node property read through `chain`: the graph through the chain, the
/// property's id, and whether the chain can hide a node.
fn node_property<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<(DynGraphArc<'g>, usize, bool), GraphError> {
    let prop_id = graph
        .node_meta()
        .get_prop_id(name, false)
        .ok_or_else(|| GraphError::PropertyMissingError(name.to_owned()))?;
    let narrows = chain.narrows();
    Ok((chain.create_view(graph)?, prop_id, narrows))
}

/// A node property read through `chain`, as its history.
fn node_history_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<TemporalNodePropOp<DynGraphArc<'g>>, GraphError> {
    let (graph, prop_id, narrows) = node_property(chain, name, graph)?;
    Ok(TemporalNodePropOp {
        graph,
        prop_id,
        narrows,
    })
}

/// A node property read through `chain`: its latest value, or its history.
fn node_property_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    temporal: bool,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let (graph, prop_id, narrows) = node_property(chain, name, graph)?;
    Ok(if temporal {
        Arc::new(TemporalNodePropOp {
            graph,
            prop_id,
            narrows,
        })
    } else {
        Arc::new(NodePropOp {
            graph,
            prop_id,
            narrows,
        })
    })
}

/// A node metadata entry read through `chain`.
fn node_metadata_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let prop_id = graph
        .node_meta()
        .get_prop_id(name, true)
        .ok_or_else(|| GraphError::MetadataMissingError(name.to_owned()))?;
    let narrows = chain.narrows();
    let graph = chain.create_view(graph)?;
    Ok(Arc::new(NodeMetaOp {
        graph,
        prop_id,
        narrows,
    }))
}

/// An edge property read through `chain`: the graph through the chain and
/// the property's id.
fn edge_property<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<(DynGraphArc<'g>, usize), GraphError> {
    let prop_id = graph
        .edge_meta()
        .get_prop_id(name, false)
        .ok_or_else(|| GraphError::PropertyMissingError(name.to_owned()))?;
    Ok((chain.create_view(graph)?, prop_id))
}

/// An edge property read through `chain`, as its history.
fn edge_history_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<TemporalEdgePropOp<DynGraphArc<'g>>, GraphError> {
    let (graph, prop_id) = edge_property(chain, name, graph)?;
    Ok(TemporalEdgePropOp { graph, prop_id })
}

/// An edge property read through `chain`: its latest value, or its history.
fn edge_property_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    temporal: bool,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let (graph, prop_id) = edge_property(chain, name, graph)?;
    Ok(if temporal {
        Arc::new(TemporalEdgePropOp { graph, prop_id })
    } else {
        Arc::new(EdgePropOp { graph, prop_id })
    })
}

/// An edge metadata entry read through `chain`.
fn edge_metadata_op<'g>(
    chain: Arc<dyn DynCreateView>,
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let prop_id = graph
        .edge_meta()
        .get_prop_id(name, true)
        .ok_or_else(|| GraphError::MetadataMissingError(name.to_owned()))?;
    let graph = chain.create_view(graph)?;
    Ok(Arc::new(EdgeMetaOp { graph, prop_id }))
}

/// The type key an untyped node carries, built once rather than per node.
static UNTYPED: LazyLock<Prop> = LazyLock::new(|| Prop::str("_default"));

impl Leaf for NodeLeaf {
    type Ops = NodeOps;

    fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(match self {
            // A field has no time axis and does not depend on layers or on the
            // node set, so the views written on it are kept for display and
            // ignored here.
            NodeLeaf::Field { field, .. } => match field {
                Field::Id => Arc::new(NodeIdOp {
                    id_type: graph.id_type(),
                }),
                Field::Name => Arc::new(Name.map(|a| Some(a.into_prop()))),
                // Untyped nodes carry the storage's default type key, so a type
                // test sees the same key the node-type mask is built over.
                Field::NodeType => Arc::new(
                    Type.map(|a| Some(a.map_or_else(|| UNTYPED.clone(), |b| b.into_prop()))),
                ),
            },
            // A degree read through a view that can hide the node is 0 there.
            NodeLeaf::Degree { views, direction } => {
                let chain = node_chain(views);
                let view = chain.create_view(graph)?;
                let degree = Degree {
                    dir: *direction,
                    view: view.clone(),
                }
                .map(|a| Some(Prop::U64(a as u64)));
                if chain.narrows() {
                    Arc::new(ShownNodeOp {
                        graph: view,
                        term: degree,
                        hidden: Some(Prop::U64(0)),
                    })
                } else {
                    Arc::new(degree)
                }
            }
            NodeLeaf::Property {
                views,
                name,
                temporal,
            } => node_property_op(node_chain(views), name, *temporal, graph)?,
            NodeLeaf::Metadata { views, name } => node_metadata_op(node_chain(views), name, graph)?,
            // A node the view hides is not active in it.
            NodeLeaf::IsActive { views } => {
                let graph = node_chain(views).create_view(graph)?;
                Arc::new(ShownNodeOp {
                    graph: graph.clone(),
                    term: HistoryOp::new(graph).map(|h| Some(Prop::Bool(!h.is_empty()))),
                    hidden: Some(Prop::Bool(false)),
                })
            }
            NodeLeaf::Opaque(value) => value.0.node_value_op(graph)?,
        })
    }

    fn history_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<dyn NodeHistory + 'g>>, GraphError> {
        Ok(match self {
            NodeLeaf::Property {
                views,
                name,
                temporal: true,
            } => Some(Arc::new(node_history_op(node_chain(views), name, graph)?)),
            _ => None,
        })
    }

    fn node_filter<'g>(
        expr: &NodeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        expr.node_test(graph)
    }

    /// An edge stays when both its ends pass.
    fn edge_filter<'g>(
        expr: &NodeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynEdgeFilter<'g>, GraphError> {
        expr.refuse_constant()?;
        both_ends(expr.clone()).edge_test(graph)
    }

    fn graph_filter<'g>(
        expr: &NodeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynGraphArc<'g>, GraphError> {
        let test = expr.node_test(graph.clone())?;
        Ok(Arc::new(NodeFilteredGraph::new(graph, test)))
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        NodeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        NodeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        NodeLeaf::IsActive { views }
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Node(expr)
    }

    fn prop_type(&self) -> PropType {
        match self {
            NodeLeaf::Field {
                field: Field::Name | Field::NodeType,
                ..
            } => PropType::Str,
            NodeLeaf::Degree { .. } => PropType::U64,
            NodeLeaf::IsActive { .. } => PropType::Bool,
            NodeLeaf::Opaque(value) => value.0.prop_type(),
            NodeLeaf::Field { .. } | NodeLeaf::Property { .. } | NodeLeaf::Metadata { .. } => {
                PropType::Empty
            }
        }
    }

    /// Every node has a type (an unset one reads as `"_default"`), a degree
    /// and an answer to whether it is active.
    fn nullable(&self) -> bool {
        match self {
            NodeLeaf::Field {
                field: Field::NodeType,
                ..
            }
            | NodeLeaf::Degree { .. }
            | NodeLeaf::IsActive { .. } => false,
            NodeLeaf::Opaque(value) => value.0.nullable(),
            NodeLeaf::Field { .. } | NodeLeaf::Property { .. } | NodeLeaf::Metadata { .. } => true,
        }
    }

    /// A field has no views that matter (see `value_op`), so the id and the
    /// name are always covered.
    fn index_term(&self) -> Option<IndexTerm> {
        match self {
            NodeLeaf::Field {
                field: Field::Id, ..
            } => Some(IndexTerm::Id),
            NodeLeaf::Field {
                field: Field::Name, ..
            } => Some(IndexTerm::Name),
            NodeLeaf::Property {
                views,
                name,
                temporal,
            } => indexed(views, name, false, *temporal),
            NodeLeaf::Metadata { views, name } => indexed(views, name, true, false),
            _ => None,
        }
    }
}

/// A term an edge and an exploded edge both offer, or an endpoint term, which
/// only an edge offers, borrowed from either leaf so that one body serves
/// both. `exploded` says which entity's view chain the term's views build on.
enum EdgeTerm<'a> {
    Property {
        views: &'a [ViewOp],
        name: &'a str,
        temporal: bool,
    },
    Metadata {
        views: &'a [ViewOp],
        name: &'a str,
    },
    IsActive(&'a [ViewOp]),
    IsValid(&'a [ViewOp]),
    IsDeleted(&'a [ViewOp]),
    IsSelfLoop,
    Endpoint(&'a NodeExpr, Endpoint),
}

impl EdgeTerm<'_> {
    fn value_op<'g>(
        self,
        exploded: bool,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(match self {
            EdgeTerm::Property {
                views,
                name,
                temporal,
            } => edge_property_op(edge_chain(exploded, views), name, temporal, graph)?,
            EdgeTerm::Metadata { views, name } => {
                edge_metadata_op(edge_chain(exploded, views), name, graph)?
            }
            EdgeTerm::IsActive(views) => Arc::new(IsActiveEdgePropOp {
                graph: edge_chain(exploded, views).create_view(graph)?,
            }),
            EdgeTerm::IsValid(views) => Arc::new(IsValidEdgePropOp {
                graph: edge_chain(exploded, views).create_view(graph)?,
            }),
            EdgeTerm::IsDeleted(views) => Arc::new(IsDeletedEdgePropOp {
                graph: edge_chain(exploded, views).create_view(graph)?,
            }),
            // Whether an edge is a self loop does not depend on any view.
            EdgeTerm::IsSelfLoop => Arc::new(IsSelfLoopEdgePropOp),
            // An endpoint term is the node op the edge applies to the node at
            // that end; each node term inside it carries its own views.
            EdgeTerm::Endpoint(inner, endpoint) => Arc::new(EdgeEndpointNodeOp {
                node_op: inner.value_op(graph)?,
                endpoint,
            }),
        })
    }

    /// An endpoint term reads the history on the node at that end.
    fn history_op<'g>(
        self,
        exploded: bool,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<dyn EdgeHistory + 'g>>, GraphError> {
        Ok(match self {
            EdgeTerm::Property {
                views,
                name,
                temporal: true,
            } => Some(Arc::new(edge_history_op(
                edge_chain(exploded, views),
                name,
                graph,
            )?)),
            EdgeTerm::Endpoint(inner, endpoint) => inner.history_op(graph)?.map(|node| {
                Arc::new(EndpointHistory { node, endpoint }) as Arc<dyn EdgeHistory + 'g>
            }),
            _ => None,
        })
    }

    fn prop_type(&self) -> PropType {
        match self {
            EdgeTerm::IsActive(_)
            | EdgeTerm::IsValid(_)
            | EdgeTerm::IsDeleted(_)
            | EdgeTerm::IsSelfLoop => PropType::Bool,
            EdgeTerm::Property { .. } | EdgeTerm::Metadata { .. } | EdgeTerm::Endpoint(..) => {
                PropType::Empty
            }
        }
    }

    fn nullable(&self) -> bool {
        matches!(
            self,
            EdgeTerm::Property { .. } | EdgeTerm::Metadata { .. } | EdgeTerm::Endpoint(..)
        )
    }

    /// An endpoint does not forward its node's index term, since an edge
    /// predicate never narrows by a node index.
    fn index_term(&self) -> Option<IndexTerm> {
        match self {
            EdgeTerm::Property {
                views,
                name,
                temporal,
            } => indexed(views, name, false, *temporal),
            EdgeTerm::Metadata { views, name } => indexed(views, name, true, false),
            _ => None,
        }
    }
}

impl EdgeLeaf {
    fn edge_term(&self) -> EdgeTerm<'_> {
        match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => EdgeTerm::Property {
                views,
                name,
                temporal: *temporal,
            },
            EdgeLeaf::Metadata { views, name } => EdgeTerm::Metadata { views, name },
            EdgeLeaf::IsActive { views } => EdgeTerm::IsActive(views),
            EdgeLeaf::IsValid { views } => EdgeTerm::IsValid(views),
            EdgeLeaf::IsDeleted { views } => EdgeTerm::IsDeleted(views),
            EdgeLeaf::IsSelfLoop { .. } => EdgeTerm::IsSelfLoop,
            EdgeLeaf::Src(inner) => EdgeTerm::Endpoint(inner, Endpoint::Src),
            EdgeLeaf::Dst(inner) => EdgeTerm::Endpoint(inner, Endpoint::Dst),
        }
    }
}

impl ExplodedEdgeLeaf {
    fn edge_term(&self) -> EdgeTerm<'_> {
        match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => EdgeTerm::Property {
                views,
                name,
                temporal: *temporal,
            },
            ExplodedEdgeLeaf::Metadata { views, name } => EdgeTerm::Metadata { views, name },
            ExplodedEdgeLeaf::IsActive { views } => EdgeTerm::IsActive(views),
            ExplodedEdgeLeaf::IsValid { views } => EdgeTerm::IsValid(views),
            ExplodedEdgeLeaf::IsDeleted { views } => EdgeTerm::IsDeleted(views),
            ExplodedEdgeLeaf::IsSelfLoop { .. } => EdgeTerm::IsSelfLoop,
        }
    }
}

impl Leaf for EdgeLeaf {
    type Ops = EdgeOps;

    fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.edge_term().value_op(false, graph)
    }

    fn history_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<dyn EdgeHistory + 'g>>, GraphError> {
        self.edge_term().history_op(false, graph)
    }

    fn edge_filter<'g>(
        expr: &EdgeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynEdgeFilter<'g>, GraphError> {
        expr.edge_test(graph)
    }

    fn graph_filter<'g>(
        expr: &EdgeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynGraphArc<'g>, GraphError> {
        let test = expr.edge_test(graph.clone())?;
        Ok(Arc::new(EdgeExprFilteredGraph::new(graph, test)))
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        EdgeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        EdgeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        EdgeLeaf::IsActive { views }
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Edge(expr)
    }

    fn prop_type(&self) -> PropType {
        self.edge_term().prop_type()
    }

    fn nullable(&self) -> bool {
        self.edge_term().nullable()
    }

    fn index_term(&self) -> Option<IndexTerm> {
        self.edge_term().index_term()
    }
}

impl Leaf for ExplodedEdgeLeaf {
    type Ops = EdgeOps;

    fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.edge_term().value_op(true, graph)
    }

    fn history_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<dyn EdgeHistory + 'g>>, GraphError> {
        self.edge_term().history_op(true, graph)
    }

    /// An edge stays when the filtered graph keeps an update of it.
    fn edge_filter<'g>(
        expr: &ExplodedEdgeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynEdgeFilter<'g>, GraphError> {
        Ok(Arc::new(EdgeExistsOp::new(Self::graph_filter(
            expr, graph,
        )?)))
    }

    fn graph_filter<'g>(
        expr: &ExplodedEdgeExpr,
        graph: DynGraphArc<'g>,
    ) -> Result<DynGraphArc<'g>, GraphError> {
        let test = expr.edge_test(graph.clone())?;
        Ok(Arc::new(ExplodedEdgeExprFilteredGraph::new(graph, test)))
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        ExplodedEdgeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        ExplodedEdgeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        ExplodedEdgeLeaf::IsActive { views }
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::ExplodedEdge(expr)
    }

    fn prop_type(&self) -> PropType {
        self.edge_term().prop_type()
    }

    fn nullable(&self) -> bool {
        self.edge_term().nullable()
    }

    fn index_term(&self) -> Option<IndexTerm> {
        self.edge_term().index_term()
    }
}

// ── op families ──────────────────────────────────────────────────────────────

// `OpFamily`, `NodeOps` and `EdgeOps` are `pub` although this module is
// private: the public `Leaf` names them, and a public trait cannot name a
// private item.

/// The ops the values of one entity compile to: node ops for a node tree,
/// edge ops for an edge or exploded-edge tree. Each method builds the op one
/// node of a tree stands for over the ops of its children, so a tree compiles
/// in one match whatever its entity; the type checks stay in that match.
pub trait OpFamily {
    /// A value per entity.
    type Value<'g>: ?Sized + 'g;

    /// A property's history per entity, walked rather than taken as one list.
    type History<'g>: ?Sized + 'g;

    /// The type `op` reports.
    fn prop_type(op: &Self::Value<'_>) -> PropType;

    /// The value of `op`, when it answers the same for every entity.
    fn const_value(op: &Self::Value<'_>) -> Option<Prop>;

    /// The type `history` has when read as one value.
    fn history_type(history: &Self::History<'_>) -> PropType;

    /// The same value for every entity.
    fn constant<'g>(value: Prop) -> Arc<Self::Value<'g>>;

    /// Two values combined by `kernel` with the test's `param`, giving a
    /// value of type `out`.
    fn binary<'g, P: Send + Sync + 'g>(
        left: Arc<Self::Value<'g>>,
        right: Arc<Self::Value<'g>>,
        param: P,
        kernel: BinaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>>;

    /// One value mapped by `kernel` with the test's `param`, giving a value of
    /// type `out`.
    fn unary<'g, P: Send + Sync + 'g>(
        inner: Arc<Self::Value<'g>>,
        param: P,
        kernel: UnaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>>;

    /// `and` of yes/no values.
    fn all_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>>;

    /// `or` of yes/no values.
    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>>;

    /// `agg` over the list value of `inner`.
    fn aggregate<'g>(inner: Arc<Self::Value<'g>>, agg: Agg) -> Arc<Self::Value<'g>>;

    /// `any()` or `all()` over the element-wise yes/no value of `inner`.
    fn quantified<'g>(inner: Arc<Self::Value<'g>>, quantifier: Quantifier) -> Arc<Self::Value<'g>>;

    /// `agg` walking `history`.
    fn streamed_aggregate<'g>(history: Arc<Self::History<'g>>, agg: Agg) -> Arc<Self::Value<'g>>;

    /// `any()` or `all()` of `test` walking `history`.
    fn streamed_quantified<'g>(
        history: Arc<Self::History<'g>>,
        test: ValueTest,
        quantifier: Quantifier,
    ) -> Arc<Self::Value<'g>>;
}

/// The ops of a node tree.
pub struct NodeOps;

impl OpFamily for NodeOps {
    type Value<'g> = dyn NodeOp<Output = Option<Prop>> + 'g;
    type History<'g> = dyn NodeHistory + 'g;

    fn prop_type(op: &Self::Value<'_>) -> PropType {
        NodeOp::prop_type(op)
    }

    fn const_value(op: &Self::Value<'_>) -> Option<Prop> {
        NodeOp::const_value(op).flatten()
    }

    fn history_type(history: &Self::History<'_>) -> PropType {
        NodeHistory::history_type(history)
    }

    fn constant<'g>(value: Prop) -> Arc<Self::Value<'g>> {
        Arc::new(Const(Some(value)))
    }

    fn binary<'g, P: Send + Sync + 'g>(
        left: Arc<Self::Value<'g>>,
        right: Arc<Self::Value<'g>>,
        param: P,
        kernel: BinaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>> {
        Arc::new(BinaryValueNodeOp {
            left,
            right,
            param,
            kernel,
            out,
        })
    }

    fn unary<'g, P: Send + Sync + 'g>(
        inner: Arc<Self::Value<'g>>,
        param: P,
        kernel: UnaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>> {
        Arc::new(UnaryValueNodeOp {
            inner,
            param,
            kernel,
            out,
        })
    }

    fn all_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(AndValueNodeOp { items })
    }

    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(OrValueNodeOp { items })
    }

    fn aggregate<'g>(inner: Arc<Self::Value<'g>>, agg: Agg) -> Arc<Self::Value<'g>> {
        match agg {
            Agg::Sum => Arc::new(SumNodeOp { inner }),
            Agg::Avg => Arc::new(AvgNodeOp { inner }),
            Agg::Min => Arc::new(MinNodeOp { inner }),
            Agg::Max => Arc::new(MaxNodeOp { inner }),
            Agg::First => Arc::new(FirstNodeOp { inner }),
            Agg::Last => Arc::new(LastNodeOp { inner }),
            Agg::Len => Arc::new(LenNodeOp { inner }),
            Agg::Earliest => Arc::new(EarliestNodeOp { inner }),
            Agg::Latest => Arc::new(LatestNodeOp { inner }),
        }
    }

    fn quantified<'g>(inner: Arc<Self::Value<'g>>, quantifier: Quantifier) -> Arc<Self::Value<'g>> {
        match quantifier {
            Quantifier::Any => Arc::new(AnyNodeOp { inner }),
            Quantifier::All => Arc::new(AllNodeOp { inner }),
        }
    }

    fn streamed_aggregate<'g>(history: Arc<Self::History<'g>>, agg: Agg) -> Arc<Self::Value<'g>> {
        Arc::new(StreamedAggNodeOp::new(history, agg))
    }

    fn streamed_quantified<'g>(
        history: Arc<Self::History<'g>>,
        test: ValueTest,
        quantifier: Quantifier,
    ) -> Arc<Self::Value<'g>> {
        match quantifier {
            Quantifier::Any => Arc::new(StreamedAnyNodeOp { history, test }),
            Quantifier::All => Arc::new(StreamedAllNodeOp { history, test }),
        }
    }
}

/// The ops of an edge or exploded-edge tree.
pub struct EdgeOps;

impl OpFamily for EdgeOps {
    type Value<'g> = dyn EdgeOp<Output = Option<Prop>> + 'g;
    type History<'g> = dyn EdgeHistory + 'g;

    fn prop_type(op: &Self::Value<'_>) -> PropType {
        EdgeOp::prop_type(op)
    }

    fn const_value(op: &Self::Value<'_>) -> Option<Prop> {
        EdgeOp::const_value(op).flatten()
    }

    fn history_type(history: &Self::History<'_>) -> PropType {
        EdgeHistory::history_type(history)
    }

    fn constant<'g>(value: Prop) -> Arc<Self::Value<'g>> {
        Arc::new(Const(Some(value)))
    }

    fn binary<'g, P: Send + Sync + 'g>(
        left: Arc<Self::Value<'g>>,
        right: Arc<Self::Value<'g>>,
        param: P,
        kernel: BinaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>> {
        Arc::new(BinaryValueEdgeOp {
            left,
            right,
            param,
            kernel,
            out,
        })
    }

    fn unary<'g, P: Send + Sync + 'g>(
        inner: Arc<Self::Value<'g>>,
        param: P,
        kernel: UnaryKernel<P>,
        out: PropType,
    ) -> Arc<Self::Value<'g>> {
        Arc::new(UnaryValueEdgeOp {
            inner,
            param,
            kernel,
            out,
        })
    }

    fn all_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(AndValueEdgeOp { items })
    }

    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(OrValueEdgeOp { items })
    }

    fn aggregate<'g>(inner: Arc<Self::Value<'g>>, agg: Agg) -> Arc<Self::Value<'g>> {
        match agg {
            Agg::Sum => Arc::new(SumEdgeOp { inner }),
            Agg::Avg => Arc::new(AvgEdgeOp { inner }),
            Agg::Min => Arc::new(MinEdgeOp { inner }),
            Agg::Max => Arc::new(MaxEdgeOp { inner }),
            Agg::First => Arc::new(FirstEdgeOp { inner }),
            Agg::Last => Arc::new(LastEdgeOp { inner }),
            Agg::Len => Arc::new(LenEdgeOp { inner }),
            Agg::Earliest => Arc::new(EarliestEdgeOp { inner }),
            Agg::Latest => Arc::new(LatestEdgeOp { inner }),
        }
    }

    fn quantified<'g>(inner: Arc<Self::Value<'g>>, quantifier: Quantifier) -> Arc<Self::Value<'g>> {
        match quantifier {
            Quantifier::Any => Arc::new(AnyEdgeOp { inner }),
            Quantifier::All => Arc::new(AllEdgeOp { inner }),
        }
    }

    fn streamed_aggregate<'g>(history: Arc<Self::History<'g>>, agg: Agg) -> Arc<Self::Value<'g>> {
        Arc::new(StreamedAggEdgeOp::new(history, agg))
    }

    fn streamed_quantified<'g>(
        history: Arc<Self::History<'g>>,
        test: ValueTest,
        quantifier: Quantifier,
    ) -> Arc<Self::Value<'g>> {
        match quantifier {
            Quantifier::Any => Arc::new(StreamedAnyEdgeOp { history, test }),
            Quantifier::All => Arc::new(StreamedAllEdgeOp { history, test }),
        }
    }
}

/// `earliest()` and `latest()` pick an update of a history; a list value
/// that is not one has elements, not updates.
fn require_list_input(agg: Agg) -> Result<(), GraphError> {
    if matches!(agg, Agg::Earliest | Agg::Latest) {
        return Err(GraphError::InvalidFilter(
            "earliest() and latest() pick an update of a temporal history; use first() or \
             last() for the elements of a list"
                .to_owned(),
        ));
    }
    Ok(())
}

impl<L: Leaf> Expr<L> {
    /// The op this tree compiles to against `graph`: each node of the tree is
    /// one op over the ops of its children, built by the entity's
    /// [`OpFamily`]. The result shape (one answer, or one per list element)
    /// is decided here, once the graph has fixed the property types.
    pub(crate) fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        Ok(match self {
            Expr::Const(value) => L::Ops::constant(value.clone()),
            Expr::Term(leaf) => leaf.value_op(graph)?,
            Expr::Agg(agg, inner) => inner.aggregate_op(*agg, graph)?,
            Expr::Cmp(op, lhs, rhs) => Self::binary_test_op(
                lhs,
                rhs,
                graph,
                *op,
                |l, r| comparison_shape(op, l, r),
                cmp_kernel,
            )?,
            Expr::Str(op, lhs, rhs) => {
                Self::binary_test_op(lhs, rhs, graph, *op, string_shape, str_kernel)?
            }
            Expr::In {
                expr,
                values,
                negated,
            } => {
                let inner = expr.value_op(graph)?;
                let (out, shape, members) = set_shape(&expr.resolved_type(&inner), values);
                L::Ops::unary(
                    inner,
                    SetMembers::new(members, *negated),
                    set_kernel(shape),
                    out,
                )
            }
            Expr::IsSome(inner) => inner.presence_op(UnaryOp::IsSome, graph)?,
            Expr::IsNone(inner) => inner.presence_op(UnaryOp::IsNone, graph)?,
            Expr::Any(inner) => inner.quantified_op(Quantifier::Any, graph)?,
            Expr::All(inner) => inner.quantified_op(Quantifier::All, graph)?,
            Expr::And(items) => L::Ops::all_of(Self::yes_no_operands(items, graph, "and")?),
            Expr::Or(items) => L::Ops::any_of(Self::yes_no_operands(items, graph, "or")?),
            Expr::Not(inner) => {
                let op = inner.value_op(graph)?;
                require_bool(&inner.resolved_type(&op), "not")?;
                L::Ops::unary(op, (), not_kernel, PropType::Bool)
            }
        })
    }

    /// The history this expression reads as is, against `graph`, for a
    /// consumer that walks the values instead of taking them as one list:
    /// `None` unless it is a term that reads one.
    pub(crate) fn history_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Option<Arc<<L::Ops as OpFamily>::History<'g>>>, GraphError> {
        match self {
            Expr::Term(leaf) => leaf.history_op(graph),
            _ => Ok(None),
        }
    }

    /// The type of this expression compiled to `op`: the one known before the
    /// graph when there is one, else the one the op reports.
    fn resolved_type(&self, op: &<L::Ops as OpFamily>::Value<'_>) -> PropType {
        resolved_prop_type(self.prop_type(), L::Ops::prop_type(op))
    }

    /// A test of `lhs` against `rhs`: `shape` decides from the two types what
    /// the test gives and how it evaluates, `kernel` evaluates it. A mismatch
    /// names the constant when the right side is one.
    fn binary_test_op<'g, P: Send + Sync + 'g>(
        lhs: &Self,
        rhs: &Self,
        graph: DynGraphArc<'g>,
        param: P,
        shape: impl FnOnce(&PropType, &PropType) -> Result<(PropType, Shape), ShapeError>,
        kernel: fn(Shape) -> BinaryKernel<P>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        let left = lhs.value_op(graph.clone())?;
        let right = rhs.value_op(graph)?;
        let lhs_pt = lhs.resolved_type(&left);
        let rhs_pt = rhs.resolved_type(&right);
        let rhs_const = L::Ops::const_value(&right);
        let (out, shape) =
            shape(&lhs_pt, &rhs_pt).map_err(|e| e.into_error(&rhs_pt, rhs_const.as_ref()))?;
        Ok(L::Ops::binary(left, right, param, kernel(shape), out))
    }

    /// A presence test of this expression. It only means something on a
    /// value that can be missing.
    fn presence_op<'g>(
        &self,
        op: UnaryOp,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        if !self.nullable() {
            return Err(GraphError::invalid_filter(format!(
                "{op}() is not valid on an expression that always has a value"
            )));
        }
        Ok(L::Ops::unary(
            self.value_op(graph)?,
            op,
            presence_kernel,
            PropType::Bool,
        ))
    }

    /// `agg` over this expression. Over a history it walks the values instead
    /// of taking them as one list; over anything else it reduces the list
    /// value.
    fn aggregate_op<'g>(
        &self,
        agg: Agg,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        if let Some(history) = self.history_op(graph.clone())? {
            require_aggregable(&L::Ops::history_type(&history), agg)?;
            return Ok(L::Ops::streamed_aggregate(history, agg));
        }
        require_list_input(agg)?;
        let op = self.value_op(graph)?;
        require_aggregable(&self.resolved_type(&op), agg)?;
        Ok(L::Ops::aggregate(op, agg))
    }

    /// `any()` or `all()` over this element-wise yes/no expression. A test of
    /// a history against a constant walks the history and stops at the first
    /// value that decides the answer; anything else collapses the list.
    fn quantified_op<'g>(
        &self,
        quantifier: Quantifier,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        if let Some((side, test)) = self.value_test() {
            if let Some(history) = side.history_op(graph.clone())? {
                if let Some(test) = test.for_history(&L::Ops::history_type(&history)) {
                    return Ok(L::Ops::streamed_quantified(history, test, quantifier));
                }
            }
        }
        let op = self.value_op(graph)?;
        qualified_type(&self.resolved_type(&op))?;
        Ok(L::Ops::quantified(op, quantifier))
    }

    /// The ops of the operands of `and` or `or`, each checked to answer
    /// yes/no. An empty list is refused.
    fn yes_no_operands<'g>(
        items: &[Self],
        graph: DynGraphArc<'g>,
        name: &str,
    ) -> Result<Vec<Arc<<L::Ops as OpFamily>::Value<'g>>>, GraphError> {
        if items.is_empty() {
            return Err(needs_operand(name));
        }
        items
            .iter()
            .map(|item| {
                let op = item.value_op(graph.clone())?;
                require_bool(&item.resolved_type(&op), name)?;
                Ok(op)
            })
            .collect()
    }
}

impl NodeExpr {
    /// The per-node test this yes/no tree is. It starts from the nodes the
    /// tree names by id, or the index candidates for its test, when it can.
    /// A tree built only from constants answers the same for every node, so
    /// it is refused.
    fn node_test<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        self.refuse_constant()?;
        let op = self.value_op(graph.clone())?;
        let nodes = self.pushdown().and_then(|pushdown| pushdown.domain(&graph));
        require_bool(&self.resolved_type(&op), "a filter")?;
        let test: Arc<dyn NodeOp<Output = bool> + 'g> = Arc::new(op.map(|v| truthy(&v)));
        Ok(match nodes {
            Some(nodes) => Arc::new(DomainNodeOp { nodes, inner: test }),
            None => test,
        })
    }
}

impl<L: Leaf<Ops = EdgeOps>> Expr<L> {
    /// The per-edge test this yes/no tree is; a tree built only from
    /// constants is refused.
    fn edge_test<'g>(&self, graph: DynGraphArc<'g>) -> Result<DynEdgeFilter<'g>, GraphError> {
        self.refuse_constant()?;
        let op = self.value_op(graph)?;
        require_bool(&self.resolved_type(&op), "a filter")?;
        Ok(Arc::new(TruthyEdgeOp { inner: op }))
    }
}

// ── filters ──────────────────────────────────────────────────────────────────

impl SplitFilter {
    /// The graph seen through every view leg in turn, erased so every part
    /// of the filter builds over the one `Arc`.
    fn viewed<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        if self.views.is_empty() {
            Ok(graph.into_dyn_graph_arc())
        } else {
            through(&self.views.concat(), graph.into_dyn_graph_arc())
        }
    }

    /// The graph seen through each view leg on its own, for the existence
    /// tests a node or edge filter asks.
    fn each_view<'graph>(
        &self,
        graph: &DynGraphArc<'graph>,
    ) -> Result<Vec<DynGraphArc<'graph>>, GraphError> {
        self.views
            .iter()
            .map(|leg| through(leg, graph.clone()))
            .collect()
    }
}

/// The graph seen through one view leg.
fn through<'graph>(
    views: &[ViewOp],
    graph: DynGraphArc<'graph>,
) -> Result<DynGraphArc<'graph>, GraphError> {
    view_chain(Arc::new(GraphFilter), views).create_view(graph)
}

/// The legs built one by one over the same graph and joined pairwise, left
/// to right.
fn fold<'graph, Q, T>(
    legs: &[Q],
    graph: &DynGraphArc<'graph>,
    build: impl Fn(&Q, DynGraphArc<'graph>) -> Result<T, GraphError>,
    join: impl Fn(T, T) -> T,
) -> Result<T, GraphError> {
    let Some((first, rest)) = legs.split_first() else {
        return Err(GraphError::invalid_filter(
            "an answer needs at least one leg",
        ));
    };
    rest.iter()
        .try_fold(build(first, graph.clone())?, |acc, leg| {
            Ok(join(acc, build(leg, graph.clone())?))
        })
}

impl<P> Question<P> {
    /// The graph that keeps what this answer keeps, `predicate` giving the
    /// graph of each predicate. Existence in a view has no graph below the
    /// top level.
    fn create_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
        predicate: &impl Fn(&P, DynGraphArc<'graph>) -> Result<DynGraphArc<'graph>, GraphError>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        let build = |leg: &Self, graph| leg.create_graph_filter(graph, predicate);
        match self {
            Question::Predicate(p) => predicate(p, graph),
            Question::Exists { .. } => Err(view_below_top_level()),
            Question::And(legs) => fold(legs, &graph, build, |l, r| {
                AndFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc()
            }),
            Question::Or(legs) => fold(legs, &graph, build, |l, r| {
                OrFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc()
            }),
        }
    }
}

impl Question<NodeExpr> {
    /// The per-node test for the nodes this answer keeps.
    fn create_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        match self {
            Question::Predicate(nodes) => NodeLeaf::node_filter(nodes, graph),
            Question::Exists { views, negated } => {
                let exists = NodeExistsOp::new(through(views, graph)?);
                Ok(if *negated {
                    Arc::new(exists.not())
                } else {
                    Arc::new(exists)
                })
            }
            Question::And(legs) => fold(legs, &graph, Self::create_node_filter, |l, r| {
                Arc::new(l.and(r))
            }),
            Question::Or(legs) => fold(legs, &graph, Self::create_node_filter, |l, r| {
                Arc::new(l.or(r))
            }),
        }
    }
}

impl EdgePredicate {
    /// The graph with the edges this predicate keeps, as its tree's
    /// [`Leaf`] answers.
    fn graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        match self {
            EdgePredicate::Edge(e) => EdgeLeaf::graph_filter(e, graph),
            EdgePredicate::Exploded(e) => ExplodedEdgeLeaf::graph_filter(e, graph),
        }
    }
}

impl Question<EdgePredicate> {
    /// The per-edge test for the edges this answer keeps.
    fn create_edge_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynEdgeFilter<'graph>, GraphError> {
        match self {
            Question::Predicate(EdgePredicate::Edge(e)) => EdgeLeaf::edge_filter(e, graph),
            Question::Predicate(EdgePredicate::Exploded(e)) => {
                ExplodedEdgeLeaf::edge_filter(e, graph)
            }
            Question::Exists { views, negated } => {
                let exists = EdgeExistsOp::new(through(views, graph)?);
                Ok(if *negated {
                    Arc::new(NotEdgeOp(exists))
                } else {
                    Arc::new(exists)
                })
            }
            Question::And(legs) => fold(legs, &graph, Self::create_edge_filter, |l, r| {
                Arc::new(AndEdgeOp { left: l, right: r })
            }),
            Question::Or(legs) => fold(legs, &graph, Self::create_edge_filter, |l, r| {
                Arc::new(OrEdgeOp { left: l, right: r })
            }),
        }
    }
}

/// The filter a split filter is: the graph seen through its view legs, then
/// its node answer and its edge answer side by side, one of them alone, or
/// the viewed graph when nothing constrains either. As a per-node or per-edge
/// test it asks that the entity exist in each view leg and pass the answers
/// on the collection's own graph: there a view leg is a test like any other.
impl CreateFilter for SplitFilter {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let graph = self.viewed(graph)?;
        let nodes = self
            .nodes
            .map(|nodes| nodes.create_graph_filter(graph.clone(), &NodeLeaf::graph_filter))
            .transpose()?;
        let edges = self
            .edges
            .map(|edges| edges.create_graph_filter(graph.clone(), &EdgePredicate::graph_filter))
            .transpose()?;
        Ok(match (nodes, edges) {
            (Some(nodes), Some(edges)) => {
                AndFilteredGraph::new(graph, nodes, edges).into_dyn_graph_arc()
            }
            (Some(answer), None) | (None, Some(answer)) => answer,
            (None, None) => graph,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let graph = graph.into_dyn_graph_arc();
        let views = self.each_view(&graph)?;
        let test: Arc<dyn NodeOp<Output = bool> + 'graph> = match &self.nodes {
            Some(nodes) => nodes.create_node_filter(graph.clone())?,
            None if views.is_empty() => return Ok(Arc::new(NodeExistsOp::new(graph))),
            None => Arc::new(NodeExistsOp::new(graph)),
        };
        Ok(views.into_iter().rev().fold(test, |test, view| {
            Arc::new(NodeExistsOp::new(view).and(test))
        }))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let graph = graph.into_dyn_graph_arc();
        let views = self.each_view(&graph)?;
        let answer = match (self.nodes.map(closed_edges), self.edges) {
            (Some(nodes), Some(edges)) => Some(nodes.and(edges)),
            (Some(answer), None) | (None, Some(answer)) => Some(answer),
            (None, None) => None,
        };
        let test: DynEdgeFilter<'graph> = match answer {
            Some(answer) => answer.create_edge_filter(graph.clone())?,
            None if views.is_empty() => return Ok(Arc::new(EdgeExistsOp::new(graph))),
            None => Arc::new(EdgeExistsOp::new(graph)),
        };
        Ok(views.into_iter().rev().fold(test, |test, view| {
            Arc::new(AndEdgeOp {
                left: EdgeExistsOp::new(view),
                right: test,
            })
        }))
    }
}

/// A tree is a filter in its own right: applying it splits it by question
/// first.
impl CreateFilter for FilterExpr {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.split()?.create_graph_filter(graph)
    }

    /// An edge test says nothing about which nodes belong in a node
    /// collection, so a filter that tests edges anywhere is refused here,
    /// whatever it splits into.
    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if self.tests_edges() {
            return Err(GraphError::NotNodeFilter);
        }
        self.split()?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.split()?.create_edge_filter(graph)
    }
}

/// A yes/no expression is a filter on its entity. A node tree answers which
/// nodes stay, and which edges by "both ends stayed"; an edge tree answers
/// which edges stay; an exploded-edge tree which updates stay, and so which
/// edges keep one. Each answer is the one the tree's [`Leaf`] gives.
impl<L: Leaf> CreateFilter for Expr<L> {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        L::graph_filter(&self, graph.into_dyn_graph_arc())
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        L::node_filter(&self, graph.into_dyn_graph_arc())
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        L::edge_filter(&self, graph.into_dyn_graph_arc())
    }
}

/// A view on the graph is a filter: the graph seen through it.
impl CreateFilter for Chain<GraphLeaf> {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        FilterExpr::from(self).create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_edge_filter(graph)
    }
}
