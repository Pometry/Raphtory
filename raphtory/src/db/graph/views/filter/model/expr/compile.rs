//! From expression data to a filter the engine can apply.
//!
//! A tree compiles straight to the ops that evaluate it: a term's views apply
//! to the graph in order and the term becomes the op that reads the viewed
//! graph, and every other node of a value expression becomes one op over the
//! ops of its children, built by the entity's [`OpFamily`]. A `Bool`-typed
//! value is a filter on its entity through its [`Leaf`]; a whole filter is the
//! graph its legs keep, combined by `and`, `or` and `not`.

use super::{
    builder::{Chain, GraphLeaf},
    Agg, EdgeExpr, EdgeLeaf, ExplodedEdgeExpr, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeExpr,
    NodeLeaf, Qualifier, ViewOp,
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
            view::{
                internal::{DynGraphArc, GraphView, IntoDynGraphArc},
                GraphViewOps, LayerOps, TimeOps,
            },
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                edge_expr::{
                    ops::{
                        AllOfEdgeOp, AndEdgeOp, AnyOfEdgeOp, BinaryValueEdgeOp, EdgeEndpointNodeOp,
                        EdgeExistsOp, EdgeMetaOp, EdgePropOp, IsActiveEdgePropOp,
                        IsDeletedEdgePropOp, IsSelfLoopEdgePropOp, IsValidEdgePropOp, NotEdgeOp,
                        OrEdgeOp, TemporalEdgePropOp, UnaryValueEdgeOp,
                    },
                    EdgeOp,
                },
                edge_filter::Endpoint,
                expr::typing::{
                    cmp_kernel, comparison_shape, not_kernel, presence_kernel, qualified_type,
                    require_bool, set_kernel, set_shape, str_kernel, string_shape, BinaryKernel,
                    SetMembers, Shape, ShapeError, UnaryKernel,
                },
                filter_operator::UnaryOp,
                node_expr::ops::{
                    AllEdgeOp, AllNodeOp, AllOfNodeOp, AnyEdgeOp, AnyNodeOp, AnyOfNodeOp,
                    AvgEdgeOp, AvgNodeOp, BinaryValueNodeOp, EarliestEdgeOp, EarliestNodeOp,
                    FirstEdgeOp, FirstNodeOp, LastEdgeOp, LastNodeOp, LatestEdgeOp, LatestNodeOp,
                    LenEdgeOp, LenNodeOp, MaxEdgeOp, MaxNodeOp, MinEdgeOp, MinNodeOp, NodeIdOp,
                    NodeMetaOp, NodePropOp, ShownNodeOp, SumEdgeOp, SumNodeOp, TemporalNodePropOp,
                    UnaryValueNodeOp,
                },
                require_aggregable, resolved_prop_type,
            },
            node_filtered_graph::NodeFilteredGraph,
            not_filtered_graph::NotFilteredGraph,
            or_filtered_graph::OrFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{IntoProp, Prop, PropType};
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

    // What a boolean tree over this leaf type is as a filter; see
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

    /// The filter a boolean expression over this leaf type is, as data.
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

    /// Whether the term reads a property's history, whose updates
    /// `earliest()` and `latest()` pick from.
    fn reads_history(&self) -> bool {
        false
    }
}

/// Whether a term read through `views` can miss an entity the incoming graph
/// shows: every view can hide one.
fn narrows(views: &[ViewOp]) -> bool {
    !views.is_empty()
}

/// A node property read through `views`: the graph through the views, the
/// property's id, and whether the views can hide a node.
fn node_property<'g>(
    views: &[ViewOp],
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<(DynGraphArc<'g>, usize, bool), GraphError> {
    let prop_id = graph
        .node_meta()
        .get_prop_id(name, false)
        .ok_or_else(|| GraphError::PropertyMissingError(name.to_owned()))?;
    Ok((apply_views(graph, views)?, prop_id, narrows(views)))
}

/// A node property read through `views`: its latest value, or its history.
fn node_property_op<'g>(
    views: &[ViewOp],
    name: &str,
    temporal: bool,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let (graph, prop_id, narrows) = node_property(views, name, graph)?;
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

/// A node metadata entry read through `views`.
fn node_metadata_op<'g>(
    views: &[ViewOp],
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let prop_id = graph
        .node_meta()
        .get_prop_id(name, true)
        .ok_or_else(|| GraphError::MetadataMissingError(name.to_owned()))?;
    Ok(Arc::new(NodeMetaOp {
        graph: apply_views(graph, views)?,
        prop_id,
        narrows: narrows(views),
    }))
}

/// An edge property read through `views`: the graph through the views and
/// the property's id.
fn edge_property<'g>(
    views: &[ViewOp],
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<(DynGraphArc<'g>, usize), GraphError> {
    let prop_id = graph
        .edge_meta()
        .get_prop_id(name, false)
        .ok_or_else(|| GraphError::PropertyMissingError(name.to_owned()))?;
    Ok((apply_views(graph, views)?, prop_id))
}

/// An edge property read through `views`: its latest value, or its history.
fn edge_property_op<'g>(
    views: &[ViewOp],
    name: &str,
    temporal: bool,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let (graph, prop_id) = edge_property(views, name, graph)?;
    Ok(if temporal {
        Arc::new(TemporalEdgePropOp { graph, prop_id })
    } else {
        Arc::new(EdgePropOp { graph, prop_id })
    })
}

/// An edge metadata entry read through `views`.
fn edge_metadata_op<'g>(
    views: &[ViewOp],
    name: &str,
    graph: DynGraphArc<'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    let prop_id = graph
        .edge_meta()
        .get_prop_id(name, true)
        .ok_or_else(|| GraphError::MetadataMissingError(name.to_owned()))?;
    let graph = apply_views(graph, views)?;
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
                let view = apply_views(graph, views)?;
                let degree = Degree {
                    dir: *direction,
                    view: view.clone(),
                }
                .map(|a| Some(Prop::U64(a as u64)));
                if narrows(views) {
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
            } => node_property_op(views, name, *temporal, graph)?,
            NodeLeaf::Metadata { views, name } => node_metadata_op(views, name, graph)?,
            // A node the view hides is not active in it.
            NodeLeaf::IsActive { views } => {
                let graph = apply_views(graph, views)?;
                Arc::new(ShownNodeOp {
                    graph: graph.clone(),
                    term: HistoryOp::new(graph).map(|h| Some(Prop::Bool(!h.is_empty()))),
                    hidden: Some(Prop::Bool(false)),
                })
            }
            NodeLeaf::Opaque(value) => value.0.node_value_op(graph)?,
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

    fn reads_history(&self) -> bool {
        matches!(self, NodeLeaf::Property { temporal: true, .. })
    }
}

/// A term an edge and an exploded edge both offer, or an endpoint term, which
/// only an edge offers, borrowed from either leaf so that one body serves
/// both.
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
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(match self {
            EdgeTerm::Property {
                views,
                name,
                temporal,
            } => edge_property_op(views, name, temporal, graph)?,
            EdgeTerm::Metadata { views, name } => edge_metadata_op(views, name, graph)?,
            EdgeTerm::IsActive(views) => Arc::new(IsActiveEdgePropOp {
                graph: apply_views(graph, views)?,
            }),
            EdgeTerm::IsValid(views) => Arc::new(IsValidEdgePropOp {
                graph: apply_views(graph, views)?,
            }),
            EdgeTerm::IsDeleted(views) => Arc::new(IsDeletedEdgePropOp {
                graph: apply_views(graph, views)?,
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

    /// An endpoint term reads the history the node at that end reads.
    fn reads_history(&self) -> bool {
        match self {
            EdgeTerm::Property { temporal, .. } => *temporal,
            EdgeTerm::Endpoint(inner, _) => inner.reads_history(),
            _ => false,
        }
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
        self.edge_term().value_op(graph)
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

    fn reads_history(&self) -> bool {
        self.edge_term().reads_history()
    }
}

impl Leaf for ExplodedEdgeLeaf {
    type Ops = EdgeOps;

    fn value_op<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.edge_term().value_op(graph)
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

    fn reads_history(&self) -> bool {
        self.edge_term().reads_history()
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

    /// The type `op` reports.
    fn prop_type(op: &Self::Value<'_>) -> PropType;

    /// The value of `op`, when it answers the same for every entity.
    fn const_value(op: &Self::Value<'_>) -> Option<Prop>;

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

    /// `and` of boolean values.
    fn all_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>>;

    /// `or` of boolean values.
    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>>;

    /// `agg` over the list value of `inner`.
    fn aggregate<'g>(inner: Arc<Self::Value<'g>>, agg: Agg) -> Arc<Self::Value<'g>>;

    /// `any()` or `all()` over the element-wise boolean value of `inner`.
    fn qualified<'g>(inner: Arc<Self::Value<'g>>, qualifier: Qualifier) -> Arc<Self::Value<'g>>;
}

/// The ops of a node tree.
pub struct NodeOps;

impl OpFamily for NodeOps {
    type Value<'g> = dyn NodeOp<Output = Option<Prop>> + 'g;

    fn prop_type(op: &Self::Value<'_>) -> PropType {
        NodeOp::prop_type(op)
    }

    fn const_value(op: &Self::Value<'_>) -> Option<Prop> {
        NodeOp::const_value(op).flatten()
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
        Arc::new(AllOfNodeOp { items })
    }

    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(AnyOfNodeOp { items })
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

    fn qualified<'g>(inner: Arc<Self::Value<'g>>, qualifier: Qualifier) -> Arc<Self::Value<'g>> {
        match qualifier {
            Qualifier::Any => Arc::new(AnyNodeOp { inner }),
            Qualifier::All => Arc::new(AllNodeOp { inner }),
        }
    }
}

/// The ops of an edge or exploded-edge tree.
pub struct EdgeOps;

impl OpFamily for EdgeOps {
    type Value<'g> = dyn EdgeOp<Output = Option<Prop>> + 'g;

    fn prop_type(op: &Self::Value<'_>) -> PropType {
        EdgeOp::prop_type(op)
    }

    fn const_value(op: &Self::Value<'_>) -> Option<Prop> {
        EdgeOp::const_value(op).flatten()
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
        Arc::new(AllOfEdgeOp { items })
    }

    fn any_of<'g>(items: Vec<Arc<Self::Value<'g>>>) -> Arc<Self::Value<'g>> {
        Arc::new(AnyOfEdgeOp { items })
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

    fn qualified<'g>(inner: Arc<Self::Value<'g>>, qualifier: Qualifier) -> Arc<Self::Value<'g>> {
        match qualifier {
            Qualifier::Any => Arc::new(AnyEdgeOp { inner }),
            Qualifier::All => Arc::new(AllEdgeOp { inner }),
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
            Expr::Any(inner) => inner.qualified_op(Qualifier::Any, graph)?,
            Expr::All(inner) => inner.qualified_op(Qualifier::All, graph)?,
            Expr::And(items) => L::Ops::all_of(Self::bool_operands(items, graph, "and")?),
            Expr::Or(items) => L::Ops::any_of(Self::bool_operands(items, graph, "or")?),
            Expr::Not(inner) => {
                let op = inner.value_op(graph)?;
                require_bool(&inner.resolved_type(&op), "not")?;
                L::Ops::unary(op, (), not_kernel, PropType::Bool)
            }
        })
    }

    /// Whether this expression is a term that reads a property's history.
    pub(crate) fn reads_history(&self) -> bool {
        matches!(self, Expr::Term(leaf) if leaf.reads_history())
    }

    /// The type of this expression compiled to `op`: the one known before the
    /// graph when there is one, else the one the op reports.
    pub(crate) fn resolved_type(&self, op: &<L::Ops as OpFamily>::Value<'_>) -> PropType {
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

    /// `agg` over the list value of this expression. Only a history has
    /// updates for `earliest()` and `latest()` to pick.
    fn aggregate_op<'g>(
        &self,
        agg: Agg,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        if !self.reads_history() {
            require_list_input(agg)?;
        }
        let op = self.value_op(graph)?;
        require_aggregable(&self.resolved_type(&op), agg)?;
        Ok(L::Ops::aggregate(op, agg))
    }

    /// `any()` or `all()` over this element-wise boolean expression.
    fn qualified_op<'g>(
        &self,
        qualifier: Qualifier,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<<L::Ops as OpFamily>::Value<'g>>, GraphError> {
        let op = self.value_op(graph)?;
        qualified_type(&self.resolved_type(&op))?;
        Ok(L::Ops::qualified(op, qualifier))
    }

    /// The ops of the operands of `and` or `or`, each checked to answer
    /// boolean. An empty list is refused.
    fn bool_operands<'g>(
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

// ── filters ──────────────────────────────────────────────────────────────────

/// The edge test a node predicate is: both ends pass it.
pub(crate) fn both_ends(nodes: NodeExpr) -> EdgeExpr {
    Expr::And(vec![
        Expr::Term(EdgeLeaf::Src(Box::new(nodes.clone()))),
        Expr::Term(EdgeLeaf::Dst(Box::new(nodes))),
    ])
}

/// An empty list has no meaning either way: `and` of nothing is not
/// "everything", `or` of nothing is not "nothing" the caller asked for.
pub(crate) fn needs_operand(name: &str) -> GraphError {
    GraphError::invalid_filter(format!("`{name}` needs at least one operand"))
}

/// A view leg with no view in it is not "no restriction".
pub(crate) fn needs_view() -> GraphError {
    GraphError::invalid_filter("a view filter needs at least one view")
}

/// A view under `or` or `not` is refused: the union with a view, or the
/// complement of one, waits until views compose with time semantics.
pub(crate) fn view_below_top_level() -> GraphError {
    GraphError::invalid_filter(
        "a view applies to the whole filter: use it alone or as a leg of the top-level `and`, \
         not under `or` or `not`",
    )
}

/// A filter is the graph its legs keep, each leg read on the same base graph.
/// On a collection it is a test of each item instead.
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

    /// The graph that keeps what the filter keeps. A leaf keeps what its
    /// tree keeps; a view is the graph seen through it; the legs of `and` and
    /// `or` each run on the same graph and their graphs combine. A view under
    /// `or` or `not` is refused.
    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let graph = graph.into_dyn_graph_arc();
        match self {
            FilterExpr::Node(expr) => NodeLeaf::graph_filter(&expr, graph),
            FilterExpr::Edge(expr) => EdgeLeaf::graph_filter(&expr, graph),
            FilterExpr::ExplodedEdge(expr) => ExplodedEdgeLeaf::graph_filter(&expr, graph),
            FilterExpr::View(views) if views.is_empty() => Err(needs_view()),
            FilterExpr::View(views) => apply_views(graph, &views),
            FilterExpr::And(legs) => fold_legs(
                &legs,
                "and",
                |leg| leg.clone().create_graph_filter(graph.clone()),
                |l, r| AndFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc(),
            ),
            FilterExpr::Or(legs) => {
                if legs.iter().any(FilterExpr::has_view) {
                    return Err(view_below_top_level());
                }
                fold_legs(
                    &legs,
                    "or",
                    |leg| leg.clone().create_graph_filter(graph.clone()),
                    |l, r| OrFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc(),
                )
            }
            FilterExpr::Not(inner) => {
                if inner.has_view() {
                    return Err(view_below_top_level());
                }
                let inner = inner.create_graph_filter(graph.clone())?;
                Ok(NotFilteredGraph::new(graph, inner).into_dyn_graph_arc())
            }
        }
    }

    /// An edge test says nothing about which nodes belong in a node
    /// collection, so a filter that tests edges anywhere is refused here.
    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if self.tests_edges() {
            return Err(GraphError::NotNodeFilter);
        }
        self.node_test(graph.into_dyn_graph_arc())
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.edge_test(graph.into_dyn_graph_arc())
    }
}

impl FilterExpr {
    /// The test of each node of a collection: a node predicate tests the node,
    /// a view asks that it exist in the view, and `and`, `or` and `not`
    /// combine the legs' tests.
    fn node_test<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'g>, GraphError> {
        match self {
            FilterExpr::Node(expr) => NodeLeaf::node_filter(expr, graph),
            FilterExpr::Edge(_) | FilterExpr::ExplodedEdge(_) => Err(GraphError::NotNodeFilter),
            FilterExpr::View(views) if views.is_empty() => Err(needs_view()),
            FilterExpr::View(views) => Ok(Arc::new(NodeExistsOp::new(apply_views(graph, views)?))),
            FilterExpr::And(legs) => fold_legs(
                legs,
                "and",
                |leg| leg.node_test(graph.clone()),
                |l, r| Arc::new(l.and(r)),
            ),
            FilterExpr::Or(legs) => fold_legs(
                legs,
                "or",
                |leg| leg.node_test(graph.clone()),
                |l, r| Arc::new(l.or(r)),
            ),
            FilterExpr::Not(inner) => Ok(Arc::new(inner.node_test(graph)?.not())),
        }
    }

    /// The test of each edge of a collection, or of each update on an
    /// exploded one: an edge predicate tests the item, a node predicate asks
    /// that both ends pass, a view asks that the item exist in the view, and
    /// `and`, `or` and `not` combine the legs' tests. Legs that only test nodes
    /// combine into one node predicate first, so `name == "b" | name == "c"`
    /// keeps the edge b→c.
    fn edge_test<'g>(&self, graph: DynGraphArc<'g>) -> Result<DynEdgeFilter<'g>, GraphError> {
        if let Some(nodes) = self.node_only()? {
            return NodeLeaf::edge_filter(&nodes, graph);
        }
        match self {
            FilterExpr::Node(expr) => NodeLeaf::edge_filter(expr, graph),
            FilterExpr::Edge(expr) => EdgeLeaf::edge_filter(expr, graph),
            FilterExpr::ExplodedEdge(expr) => ExplodedEdgeLeaf::edge_filter(expr, graph),
            FilterExpr::View(views) if views.is_empty() => Err(needs_view()),
            FilterExpr::View(views) => Ok(Arc::new(EdgeExistsOp::new(apply_views(graph, views)?))),
            FilterExpr::And(legs) => fold_legs(
                legs,
                "and",
                |leg| leg.edge_test(graph.clone()),
                |left, right| Arc::new(AndEdgeOp { left, right }),
            ),
            FilterExpr::Or(legs) => fold_legs(
                legs,
                "or",
                |leg| leg.edge_test(graph.clone()),
                |left, right| Arc::new(OrEdgeOp { left, right }),
            ),
            FilterExpr::Not(inner) => Ok(Arc::new(NotEdgeOp(inner.edge_test(graph)?))),
        }
    }

    /// This filter as one node predicate, when it tests nodes only. A
    /// constant leg is refused before the legs merge, as it is on its own.
    fn node_only(&self) -> Result<Option<NodeExpr>, GraphError> {
        // The legs' node predicates when every leg has one, `None` when one does not.
        let all = |legs: &[FilterExpr]| -> Result<Option<Vec<NodeExpr>>, GraphError> {
            legs.iter().map(Self::node_only).collect()
        };
        Ok(match self {
            FilterExpr::Node(expr) => {
                expr.refuse_constant()?;
                Some(expr.clone())
            }
            FilterExpr::And(legs) => all(legs)?.map(Expr::And),
            FilterExpr::Or(legs) => all(legs)?.map(Expr::Or),
            FilterExpr::Not(inner) => inner.node_only()?.map(|expr| Expr::Not(Box::new(expr))),
            FilterExpr::Edge(_) | FilterExpr::ExplodedEdge(_) | FilterExpr::View(_) => None,
        })
    }
}

/// The legs tested one by one and joined pairwise, left to right. `and` or
/// `or` of nothing is refused.
fn fold_legs<T>(
    legs: &[FilterExpr],
    name: &str,
    test: impl Fn(&FilterExpr) -> Result<T, GraphError>,
    join: impl Fn(T, T) -> T,
) -> Result<T, GraphError> {
    let (first, rest) = legs.split_first().ok_or_else(|| needs_operand(name))?;
    rest.iter()
        .try_fold(test(first)?, |acc, leg| Ok(join(acc, test(leg)?)))
}

/// The graph seen through `views`, applied in order with the graph's own view
/// methods, as `graph.window(..).layers(..)` does.
fn apply_views<'g>(
    graph: DynGraphArc<'g>,
    views: &[ViewOp],
) -> Result<DynGraphArc<'g>, GraphError> {
    views.iter().try_fold(graph, |graph, view| {
        Ok(match view {
            ViewOp::Window { start, end } => graph.window(*start, *end).into_dyn_graph_arc(),
            ViewOp::At(t) => graph.at(*t).into_dyn_graph_arc(),
            ViewOp::After(t) => graph.after(*t).into_dyn_graph_arc(),
            ViewOp::Before(t) => graph.before(*t).into_dyn_graph_arc(),
            ViewOp::Latest => graph.latest().into_dyn_graph_arc(),
            ViewOp::SnapshotAt(t) => graph.snapshot_at(*t).into_dyn_graph_arc(),
            ViewOp::SnapshotLatest => graph.snapshot_latest().into_dyn_graph_arc(),
            ViewOp::Layers(layer) => graph.layers(layer.clone())?.into_dyn_graph_arc(),
            ViewOp::DefaultLayer => graph.default_layer().into_dyn_graph_arc(),
            ViewOp::ExcludeLayers(layer) => {
                graph.exclude_layers(layer.clone())?.into_dyn_graph_arc()
            }
            ViewOp::ValidLayers(layer) => graph.valid_layers(layer.clone()).into_dyn_graph_arc(),
            ViewOp::ExcludeValidLayers(layer) => graph
                .exclude_valid_layers(layer.clone())
                .into_dyn_graph_arc(),
            ViewOp::ShrinkStart(t) => graph.shrink_start(*t).into_dyn_graph_arc(),
            ViewOp::ShrinkEnd(t) => graph.shrink_end(*t).into_dyn_graph_arc(),
            ViewOp::ExcludeNodes(ids) => graph.exclude_nodes(ids).into_dyn_graph_arc(),
            ViewOp::Subgraph(ids) => graph.subgraph(ids).into_dyn_graph_arc(),
            ViewOp::SubgraphNodeTypes(types) => {
                graph.subgraph_node_types(types).into_dyn_graph_arc()
            }
            ViewOp::Valid => graph.valid().into_dyn_graph_arc(),
        })
    })
}

/// A boolean expression is a filter on its entity. A node tree answers which
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
