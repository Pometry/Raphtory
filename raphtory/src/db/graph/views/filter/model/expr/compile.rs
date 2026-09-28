//! From expression data to a filter the engine can apply.
//!
//! Reads and views replay onto the erased factories exactly as the typed API
//! would build them. Comparisons, tests and combinators become value
//! expressions ([`CmpExpr`], [`QualExpr`], …) whose result type is decided
//! when they are built against a graph, because only then are property types
//! known. A [`Predicate`] turns a `Bool`-typed value expression into a filter
//! on the entity the expression belongs to.

use super::{
    stream::{
        StreamedAggEdgeOp, StreamedAggNodeOp, StreamedQualEdgeOp, StreamedQualNodeOp, ValueTest,
    },
    Agg, CmpOp, DynCreateHistory, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeExpr,
    NodeLeaf, StrOp, ViewOp,
};
use crate::{
    db::{
        api::{
            state::{
                ops::{filter::NodeExistsOp, NodeFilterOp},
                Index, NodeOp,
            },
            view::internal::{DynGraphArc, GraphView, InnerFilterOps, NodeList},
        },
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                and_filter::AndFilter,
                comparable_set_values,
                dyn_factory::{DynEdgeFilterFactory, DynNodeFilterFactory},
                edge_expr::{
                    ops::{AndEdgeOp, EdgeExistsOp},
                    EdgeOp,
                },
                edge_filter::{EdgeEndpointWrapper, EdgeFilter, Endpoint},
                exploded_edge_filter::ExplodedEdgeFilter,
                filter_operator::{BinaryOp, Comparable, StringComparable, StringOp, UnaryOp},
                graph_filter::GraphFilter,
                is_active_edge_filter::IsActiveEdge,
                is_active_node_filter::IsActiveNode,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                node_expr::{
                    ops::{
                        broadcast_binary, broadcast_unary, gid_for_id_lookup, AllEdgeOp, AllNodeOp,
                        AnyEdgeOp, AnyNodeOp, DomainNodeOp,
                    },
                    CreateOp, DynCreateOp, EntityExpr, Scoped,
                },
                node_filter::NodeFilter,
                not_filter::NotFilter,
                or_filter::OrFilter,
                require_aggregable, resolved_prop_type, validate_binary_op,
                validate_const_comparable, validate_string_op, validate_types_comparable,
                DynCreateFilter, DynView, EntityMarker, ViewWrapOps,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
    prelude::{EntityAggOps, Layer},
};
use raphtory_api::core::{
    entities::{
        properties::{
            meta::NODE_ID_PROP_ID,
            prop::{prop_hashable::HashableProp, Prop, PropType},
        },
        LayerId, GID, VID,
    },
    storage::timeindex::EventTime,
    Direction,
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::graph::graph::{GraphStorage, NodePropPredicate, NodePropSemantics};
use std::{collections::HashSet, fmt::Debug, sync::Arc};
use storage::EdgeEntryRef;

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

// ── leaves ───────────────────────────────────────────────────────────────────

/// What an entity can read. Implemented by the leaf enum of each entity.
pub trait Leaf: Clone + Debug + PartialEq + Send + Sync + 'static {
    /// The entity every expression over this leaf type belongs to.
    const ENTITY: EntityMarker;

    /// The erased value this read produces.
    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError>;

    /// The history this read walks, when it is the history of a temporal
    /// property; a consumer that streams it need not build the list.
    fn compile_history(&self) -> Option<Arc<dyn DynCreateHistory>>;

    /// Whether the read is scoped by a view.
    fn has_view(&self) -> bool;

    /// Whether the read is the history of a temporal property.
    fn is_temporal(&self) -> bool;

    /// The latest value of a property, or its history when `temporal`, seen
    /// through `views`.
    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self;

    /// A metadata entry seen through `views`.
    fn metadata(views: Vec<ViewOp>, name: String) -> Self;

    /// Whether the entity is active inside `views`.
    fn is_active(views: Vec<ViewOp>) -> Self;

    /// A view applied around the read: it scopes the read.
    fn push_view(&mut self, op: ViewOp);

    /// The filter a yes/no expression over this leaf type is.
    fn filter(expr: Expr<Self>) -> FilterExpr;
}

impl<L: Leaf> Expr<L> {
    /// Scope every read in this expression by one more view, applied after
    /// the views the reads already carry.
    pub fn push_view(&mut self, op: ViewOp) {
        match self {
            Expr::Const(_) => {}
            Expr::Read(leaf) => leaf.push_view(op),
            Expr::Agg(_, e)
            | Expr::IsSome(e)
            | Expr::IsNone(e)
            | Expr::Any(e)
            | Expr::All(e)
            | Expr::Not(e) => e.push_view(op),
            Expr::In { expr, .. } => expr.push_view(op),
            Expr::Cmp(_, l, r) | Expr::Str(_, l, r) => {
                l.push_view(op.clone());
                r.push_view(op);
            }
            Expr::And(items) | Expr::Or(items) => {
                for item in items {
                    item.push_view(op.clone());
                }
            }
        }
    }
}

fn node_factory(views: &[ViewOp]) -> Arc<dyn DynNodeFilterFactory> {
    let mut f: Arc<dyn DynNodeFilterFactory> = Arc::new(NodeFilter);
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.window(*start, *end),
            ViewOp::At(t) => f.at(*t),
            ViewOp::After(t) => f.after(*t),
            ViewOp::Before(t) => f.before(*t),
            ViewOp::Latest => Arc::new(f.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(f.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(f.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(f.layer(names.clone())),
        };
    }
    f
}

fn edge_factory(exploded: bool, views: &[ViewOp]) -> Arc<dyn DynEdgeFilterFactory> {
    let mut f: Arc<dyn DynEdgeFilterFactory> = if exploded {
        Arc::new(ExplodedEdgeFilter)
    } else {
        Arc::new(EdgeFilter)
    };
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.dyn_window(*start, *end),
            ViewOp::At(t) => f.dyn_at(*t),
            ViewOp::After(t) => f.dyn_after(*t),
            ViewOp::Before(t) => f.dyn_before(*t),
            ViewOp::Latest => f.dyn_latest(),
            ViewOp::SnapshotAt(t) => f.dyn_snapshot_at(*t),
            ViewOp::SnapshotLatest => f.dyn_snapshot_latest(),
            ViewOp::Layers(names) => f.dyn_layer(names.clone()),
        };
    }
    f
}

impl Leaf for NodeLeaf {
    const ENTITY: EntityMarker = EntityMarker::Node;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        Ok(match self {
            NodeLeaf::Field { views, field } => {
                let f = node_factory(views);
                match field {
                    Field::Id => f.dyn_id(),
                    Field::Name => f.dyn_name(),
                    Field::NodeType => f.dyn_node_type(),
                }
            }
            NodeLeaf::Degree { views, direction } => {
                let f = node_factory(views);
                match direction {
                    Direction::BOTH => f.dyn_degree(),
                    Direction::IN => f.dyn_in_degree(),
                    Direction::OUT => f.dyn_out_degree(),
                }
            }
            NodeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = node_factory(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            NodeLeaf::Metadata { views, name } => node_factory(views).dyn_metadata(name.clone()),
            NodeLeaf::IsActive { views } => Arc::new(Scoped {
                view: node_factory(views),
                inner: IsActiveNode,
            }),
        })
    }

    fn compile_history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        match self {
            NodeLeaf::Property {
                views,
                name,
                temporal: true,
            } => Some(node_factory(views).dyn_property(name.clone()).history()),
            _ => None,
        }
    }

    fn has_view(&self) -> bool {
        !self.views().is_empty()
    }

    fn is_temporal(&self) -> bool {
        matches!(self, NodeLeaf::Property { temporal: true, .. })
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

    fn push_view(&mut self, op: ViewOp) {
        self.views_mut().push(op);
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Node(expr)
    }
}

impl NodeLeaf {
    fn views(&self) -> &[ViewOp] {
        match self {
            NodeLeaf::Field { views, .. }
            | NodeLeaf::Degree { views, .. }
            | NodeLeaf::Property { views, .. }
            | NodeLeaf::Metadata { views, .. }
            | NodeLeaf::IsActive { views } => views,
        }
    }

    fn views_mut(&mut self) -> &mut Vec<ViewOp> {
        match self {
            NodeLeaf::Field { views, .. }
            | NodeLeaf::Degree { views, .. }
            | NodeLeaf::Property { views, .. }
            | NodeLeaf::Metadata { views, .. }
            | NodeLeaf::IsActive { views } => views,
        }
    }
}

impl Leaf for EdgeLeaf {
    const ENTITY: EntityMarker = EntityMarker::Edge;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let f = |views| edge_factory(false, views);
        Ok(match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = f(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            EdgeLeaf::Metadata { views, name } => f(views).dyn_metadata(name.clone()),
            EdgeLeaf::IsActive { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsActiveEdge,
            }),
            EdgeLeaf::IsValid { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsValidEdge,
            }),
            EdgeLeaf::IsDeleted { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsDeletedEdge,
            }),
            EdgeLeaf::IsSelfLoop { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsSelfLoopEdge,
            }),
            // An endpoint read is a node expression the edge evaluates on the
            // node at that end; its views scope that node read.
            EdgeLeaf::Src(inner) => Arc::new(EdgeEndpointWrapper::new(
                inner.compile_value()?,
                Endpoint::Src,
            )),
            EdgeLeaf::Dst(inner) => Arc::new(EdgeEndpointWrapper::new(
                inner.compile_value()?,
                Endpoint::Dst,
            )),
        })
    }

    fn compile_history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => Some(
                edge_factory(false, views)
                    .dyn_property(name.clone())
                    .history(),
            ),
            EdgeLeaf::Src(inner) => Some(Arc::new(EdgeEndpointWrapper::new(
                inner.history()?,
                Endpoint::Src,
            ))),
            EdgeLeaf::Dst(inner) => Some(Arc::new(EdgeEndpointWrapper::new(
                inner.history()?,
                Endpoint::Dst,
            ))),
            _ => None,
        }
    }

    fn has_view(&self) -> bool {
        match self {
            EdgeLeaf::Property { views, .. }
            | EdgeLeaf::Metadata { views, .. }
            | EdgeLeaf::IsActive { views }
            | EdgeLeaf::IsValid { views }
            | EdgeLeaf::IsDeleted { views }
            | EdgeLeaf::IsSelfLoop { views } => !views.is_empty(),
            EdgeLeaf::Src(inner) | EdgeLeaf::Dst(inner) => inner.has_view(),
        }
    }

    fn is_temporal(&self) -> bool {
        match self {
            EdgeLeaf::Property { temporal, .. } => *temporal,
            EdgeLeaf::Src(inner) | EdgeLeaf::Dst(inner) => inner.is_temporal_history(),
            _ => false,
        }
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

    /// A view around an endpoint read scopes the node read at that end.
    fn push_view(&mut self, op: ViewOp) {
        match self {
            EdgeLeaf::Property { views, .. }
            | EdgeLeaf::Metadata { views, .. }
            | EdgeLeaf::IsActive { views }
            | EdgeLeaf::IsValid { views }
            | EdgeLeaf::IsDeleted { views }
            | EdgeLeaf::IsSelfLoop { views } => views.push(op),
            EdgeLeaf::Src(inner) | EdgeLeaf::Dst(inner) => inner.push_view(op),
        }
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Edge(expr)
    }
}

impl Leaf for ExplodedEdgeLeaf {
    const ENTITY: EntityMarker = EntityMarker::ExplodedEdge;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let f = |views| edge_factory(true, views);
        Ok(match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = f(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            ExplodedEdgeLeaf::Metadata { views, name } => f(views).dyn_metadata(name.clone()),
            ExplodedEdgeLeaf::IsActive { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsActiveEdge,
            }),
            ExplodedEdgeLeaf::IsValid { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsValidEdge,
            }),
            ExplodedEdgeLeaf::IsDeleted { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsDeletedEdge,
            }),
            ExplodedEdgeLeaf::IsSelfLoop { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsSelfLoopEdge,
            }),
        })
    }

    fn compile_history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal: true,
            } => Some(
                edge_factory(true, views)
                    .dyn_property(name.clone())
                    .history(),
            ),
            _ => None,
        }
    }

    fn has_view(&self) -> bool {
        !self.views().is_empty()
    }

    fn is_temporal(&self) -> bool {
        matches!(self, ExplodedEdgeLeaf::Property { temporal: true, .. })
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

    fn push_view(&mut self, op: ViewOp) {
        self.views_mut().push(op);
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::ExplodedEdge(expr)
    }
}

impl ExplodedEdgeLeaf {
    fn views(&self) -> &[ViewOp] {
        match self {
            ExplodedEdgeLeaf::Property { views, .. }
            | ExplodedEdgeLeaf::Metadata { views, .. }
            | ExplodedEdgeLeaf::IsActive { views }
            | ExplodedEdgeLeaf::IsValid { views }
            | ExplodedEdgeLeaf::IsDeleted { views }
            | ExplodedEdgeLeaf::IsSelfLoop { views } => views,
        }
    }

    fn views_mut(&mut self) -> &mut Vec<ViewOp> {
        match self {
            ExplodedEdgeLeaf::Property { views, .. }
            | ExplodedEdgeLeaf::Metadata { views, .. }
            | ExplodedEdgeLeaf::IsActive { views }
            | ExplodedEdgeLeaf::IsValid { views }
            | ExplodedEdgeLeaf::IsDeleted { views }
            | ExplodedEdgeLeaf::IsSelfLoop { views } => views,
        }
    }
}

// ── values ───────────────────────────────────────────────────────────────────

impl<L: Leaf> Expr<L> {
    /// Whether this is the history of a temporal property, read as is.
    pub fn is_temporal_history(&self) -> bool {
        matches!(self, Expr::Read(leaf) if leaf.is_temporal())
    }

    /// The history this expression reads as is, for a consumer that walks
    /// it instead of taking the list.
    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        match self {
            Expr::Read(leaf) => leaf.compile_history(),
            _ => None,
        }
    }

    /// `any()`/`all()` over a comparison of a history with a constant walks
    /// the history: the test to apply to each value, and the history.
    fn streamed_test(&self) -> Option<(Arc<dyn DynCreateHistory>, QualTest)> {
        match self {
            Expr::Cmp(op, lhs, rhs) => match (&**lhs, &**rhs) {
                (read, Expr::Const(constant)) => Some((
                    read.history()?,
                    QualTest::Cmp(binary_op(*op), constant.clone()),
                )),
                (Expr::Const(constant), read) => Some((
                    read.history()?,
                    QualTest::Cmp(binary_op(flipped(*op)), constant.clone()),
                )),
                _ => None,
            },
            Expr::Str(op, lhs, rhs) => match &**rhs {
                Expr::Const(constant) => Some((
                    lhs.history()?,
                    QualTest::Str(string_op(op), constant.clone()),
                )),
                _ => None,
            },
            Expr::In {
                expr,
                values,
                negated,
            } => Some((expr.history()?, QualTest::In(values.clone(), *negated))),
            _ => None,
        }
    }

    /// The erased, compilable value this expression stands for.
    pub fn compile_value(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let entity = L::ENTITY;
        Ok(match self {
            Expr::Const(value) => Arc::new(value.clone()),
            Expr::Read(leaf) => leaf.compile()?,
            Expr::Agg(agg, inner) => {
                if matches!(agg, Agg::Earliest | Agg::Latest) && !inner.is_temporal_history() {
                    return Err(invalid(
                        "earliest() and latest() pick an update of a temporal history; use \
                         first() or last() for the elements of a list",
                    ));
                }
                if let Some(history) = inner.history() {
                    return Ok(Arc::new(StreamedAggExpr {
                        history,
                        agg: *agg,
                        entity,
                    }));
                }
                let op = inner.compile_value()?;
                match agg {
                    Agg::Sum => Arc::new(op.sum()),
                    Agg::Avg => Arc::new(op.avg()),
                    Agg::Min => Arc::new(op.min()),
                    Agg::Max => Arc::new(op.max()),
                    Agg::First => Arc::new(op.first()),
                    Agg::Last => Arc::new(op.last()),
                    Agg::Len => Arc::new(op.len()),
                    Agg::Earliest => Arc::new(op.earliest()),
                    Agg::Latest => Arc::new(op.latest()),
                }
            }
            Expr::Cmp(op, lhs, rhs) => Arc::new(CmpExpr {
                op: binary_op(*op),
                lhs: lhs.compile_value()?,
                rhs: rhs.compile_value()?,
                entity,
            }),
            Expr::Str(op, lhs, rhs) => Arc::new(StrExpr {
                op: string_op(op),
                lhs: lhs.compile_value()?,
                rhs: rhs.compile_value()?,
                entity,
            }),
            Expr::In {
                expr,
                values,
                negated,
            } => Arc::new(SetExpr {
                inner: expr.compile_value()?,
                values: values.clone(),
                negated: *negated,
                entity,
            }),
            Expr::IsSome(inner) => Arc::new(PresenceExpr {
                inner: inner.compile_value()?,
                op: UnaryOp::IsSome,
                entity,
            }),
            Expr::IsNone(inner) => Arc::new(PresenceExpr {
                inner: inner.compile_value()?,
                op: UnaryOp::IsNone,
                entity,
            }),
            Expr::Any(inner) | Expr::All(inner) => {
                let all = matches!(self, Expr::All(_));
                let fallback = Arc::new(QualExpr {
                    inner: inner.compile_value()?,
                    all,
                    entity,
                });
                match inner.streamed_test() {
                    Some((history, test)) => Arc::new(StreamedQualExpr {
                        history,
                        test,
                        all,
                        fallback,
                        entity,
                    }),
                    None => fallback,
                }
            }
            Expr::And(items) => Arc::new(BoolCombineExpr {
                items: items
                    .iter()
                    .map(Self::compile_value)
                    .collect::<Result<_, _>>()?,
                all: true,
                entity,
            }),
            Expr::Or(items) => Arc::new(BoolCombineExpr {
                items: items
                    .iter()
                    .map(Self::compile_value)
                    .collect::<Result<_, _>>()?,
                all: false,
                entity,
            }),
            Expr::Not(inner) => Arc::new(BoolNotExpr {
                inner: inner.compile_value()?,
                entity,
            }),
        })
    }
}

fn binary_op(op: CmpOp) -> BinaryOp {
    match op {
        CmpOp::Eq => BinaryOp::Eq,
        CmpOp::Ne => BinaryOp::Ne,
        CmpOp::Lt => BinaryOp::Lt,
        CmpOp::Le => BinaryOp::Le,
        CmpOp::Gt => BinaryOp::Gt,
        CmpOp::Ge => BinaryOp::Ge,
    }
}

fn string_op(op: &StrOp) -> StringOp {
    match op {
        StrOp::StartsWith => StringOp::StartsWith,
        StrOp::EndsWith => StringOp::EndsWith,
        StrOp::Contains => StringOp::Contains,
        StrOp::NotContains => StringOp::NotContains,
        StrOp::FuzzySearch {
            levenshtein_distance,
            prefix_match,
        } => StringOp::FuzzySearch {
            levenshtein_distance: *levenshtein_distance,
            prefix_match: *prefix_match,
        },
    }
}

// ── result types ─────────────────────────────────────────────────────────────

/// How a two-sided test evaluates: on the whole values, or once per element
/// of a list-valued side.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Whole,
    Elementwise,
}

fn list(inner: PropType) -> PropType {
    PropType::List(Box::new(inner))
}

/// The result type of comparing `lhs` with `rhs`: `Bool` when the two are
/// comparable as they stand, `List<Bool>` (nested to match) when a list-valued
/// side is compared element by element against the other.
fn comparison_shape(
    op: &BinaryOp,
    lhs: &PropType,
    rhs: &PropType,
    rhs_const: Option<&Prop>,
) -> Result<(PropType, Shape), GraphError> {
    if lhs.is_comparable_with(rhs) {
        validate_binary_op(op, lhs)?;
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = comparison_shape(op, inner, rhs, rhs_const) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    if let PropType::List(inner) = rhs {
        if let Ok((out, _)) = comparison_shape(op, lhs, inner, None) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    let mismatch = match rhs_const {
        Some(value) => validate_const_comparable(lhs, Some(value)),
        None => validate_types_comparable(lhs, rhs),
    };
    Err(mismatch
        .err()
        .unwrap_or_else(|| invalid(format!("type mismatch: lhs is {lhs}, rhs is {rhs}"))))
}

/// The result type of a string test: the left side must be a string, or a
/// list whose elements are, and the right side a string.
fn string_shape(
    lhs: &PropType,
    rhs: &PropType,
    rhs_const: Option<&Prop>,
) -> Result<(PropType, Shape), GraphError> {
    if lhs.is_unknown() || lhs.is_str() {
        match rhs_const {
            Some(value) => validate_const_comparable(&PropType::Str, Some(value))?,
            None => validate_types_comparable(&PropType::Str, rhs)?,
        }
        return Ok((PropType::Bool, Shape::Whole));
    }
    if let PropType::List(inner) = lhs {
        if let Ok((out, _)) = string_shape(inner, rhs, rhs_const) {
            return Ok((list(out), Shape::Elementwise));
        }
    }
    Err(validate_string_op(lhs).err().unwrap_or_else(|| {
        invalid(format!(
            "string operator requires a Str property, but the property type is {lhs}"
        ))
    }))
}

/// The result type of a membership test, and the members that can match. A
/// list-valued side whose whole value no member can equal is tested element
/// by element instead.
fn set_shape(lhs: &PropType, values: &[Prop]) -> (PropType, Shape, Vec<Prop>) {
    let whole = comparable_set_values(lhs, values.to_vec());
    if let PropType::List(inner) = lhs {
        if whole.is_empty() && !values.is_empty() {
            let (out, _, members) = set_shape(inner, values);
            return (list(out), Shape::Elementwise, members);
        }
    }
    (PropType::Bool, Shape::Whole, whole)
}

/// The type `any()`/`all()` produce over `inner`: one list level fewer, and
/// only over an element-wise yes/no result.
fn qualified_type(inner: &PropType) -> Result<PropType, GraphError> {
    match inner {
        PropType::List(elem) if matches!(**elem, PropType::Bool | PropType::List(_)) => {
            Ok((**elem).clone())
        }
        other => Err(invalid(format!(
            "any()/all() collapse an element-wise comparison (a list of yes/no answers), \
             but this expression has type {other}"
        ))),
    }
}

fn require_bool(pt: &PropType, what: &str) -> Result<(), GraphError> {
    match pt {
        PropType::Bool => Ok(()),
        PropType::List(inner) if matches!(**inner, PropType::Bool | PropType::List(_)) => {
            Err(invalid(format!(
                "{what} needs a yes/no answer, but this comparison gives one answer per \
                 element ({pt}); add any() or all() to say which elements must match"
            )))
        }
        other => Err(invalid(format!(
            "{what} needs a yes/no answer, but this expression has type {other}"
        ))),
    }
}

fn truthy(v: &Option<Prop>) -> bool {
    matches!(v, Some(Prop::Bool(true)))
}

// ── runtime ops ──────────────────────────────────────────────────────────────

macro_rules! value_ops {
    ($op_trait:ident, $id:ty, $binary:ident, $unary:ident, $nary:ident $(, $extra:item)*) => {
        struct $binary<'g, K> {
            left: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            right: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            kernel: K,
            out: PropType,
        }

        impl<'g, K> $binary<'g, K>
        where
            K: Fn(Option<Prop>, Option<Prop>) -> Option<Prop>,
        {
            fn eval(
                &self,
                read: impl Fn(&dyn $op_trait<Output = Option<Prop>>) -> Option<Prop>,
            ) -> Option<Prop> {
                (self.kernel)(read(self.left.as_ref()), read(self.right.as_ref()))
            }
        }

        impl<'g, K> $op_trait for $binary<'g, K>
        where
            K: Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Send + Sync,
        {
            type Output = Option<Prop>;
            $($extra)*
            fn prop_type(&self) -> PropType {
                self.out.clone()
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                self.eval(|op| op.apply(storage, id))
            }
        }

        struct $unary<'g, K> {
            inner: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            kernel: K,
            out: PropType,
        }

        impl<'g, K> $unary<'g, K>
        where
            K: Fn(Option<Prop>) -> Option<Prop>,
        {
            fn eval(
                &self,
                read: impl Fn(&dyn $op_trait<Output = Option<Prop>>) -> Option<Prop>,
            ) -> Option<Prop> {
                (self.kernel)(read(self.inner.as_ref()))
            }
        }

        impl<'g, K> $op_trait for $unary<'g, K>
        where
            K: Fn(Option<Prop>) -> Option<Prop> + Send + Sync,
        {
            type Output = Option<Prop>;
            $($extra)*
            fn prop_type(&self) -> PropType {
                self.out.clone()
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                self.eval(|op| op.apply(storage, id))
            }
        }

        /// `and` / `or` over yes/no values, short-circuiting.
        struct $nary<'g> {
            items: Vec<Arc<dyn $op_trait<Output = Option<Prop>> + 'g>>,
            all: bool,
        }

        impl<'g> $nary<'g> {
            fn eval(
                &self,
                read: impl Fn(&dyn $op_trait<Output = Option<Prop>>) -> Option<Prop>,
            ) -> Option<Prop> {
                let hit = if self.all {
                    self.items.iter().all(|item| truthy(&read(item.as_ref())))
                } else {
                    self.items.iter().any(|item| truthy(&read(item.as_ref())))
                };
                Some(Prop::Bool(hit))
            }
        }

        impl<'g> $op_trait for $nary<'g> {
            type Output = Option<Prop>;
            $($extra)*
            fn prop_type(&self) -> PropType {
                PropType::Bool
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                self.eval(|op| op.apply(storage, id))
            }
        }
    };
}

value_ops!(
    NodeOp,
    VID,
    BinaryValueNodeOp,
    UnaryValueNodeOp,
    NaryBoolNodeOp,
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }
);
value_ops!(
    EdgeOp,
    EdgeEntryRef,
    BinaryValueEdgeOp,
    UnaryValueEdgeOp,
    NaryBoolEdgeOp,
    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        self.eval(|op| op.apply_layer(storage, edge, layer))
    },
    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        self.eval(|op| op.apply_exploded(storage, edge, layer, t))
    }
);

/// Adapts a yes/no edge value to the plain boolean the filtered graphs consume.
struct TruthyEdgeOp<'g> {
    inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> EdgeOp for TruthyEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        truthy(&self.inner.apply(storage, edge))
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        truthy(&self.inner.apply_layer(storage, edge, layer))
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        truthy(&self.inner.apply_exploded(storage, edge, layer, t))
    }
}

fn cmp_kernel(
    op: BinaryOp,
    shape: Shape,
) -> impl Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Clone {
    move |l, r| match shape {
        Shape::Whole => Some(Prop::Bool(Option::<Prop>::binary_cmp(&op, &l, &r))),
        Shape::Elementwise => broadcast_binary(l, r, &|l, r| {
            Some(Prop::Bool(Prop::binary_cmp(&op, &l?, &r?)))
        }),
    }
}

fn str_kernel(
    op: StringOp,
    shape: Shape,
) -> impl Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Clone {
    move |l, r| match shape {
        Shape::Whole => Some(Prop::Bool(Option::<Prop>::string_cmp(&op, &l, &r))),
        Shape::Elementwise => broadcast_binary(l, r, &|l, r| {
            Some(Prop::Bool(Option::<Prop>::string_cmp(&op, &l, &r)))
        }),
    }
}

fn set_kernel(
    values: Vec<Prop>,
    negated: bool,
    shape: Shape,
) -> impl Fn(Option<Prop>) -> Option<Prop> + Clone {
    let values: Arc<HashSet<HashableProp>> =
        Arc::new(values.into_iter().map(HashableProp).collect());
    move |v| {
        let member = |v: Option<Prop>| {
            let present = values.contains(&HashableProp(v?));
            Some(Prop::Bool(present != negated))
        };
        match shape {
            Shape::Whole => member(v),
            Shape::Elementwise => broadcast_unary(v, member),
        }
    }
}

// ── value expressions ────────────────────────────────────────────────────────

macro_rules! entity_expr {
    ($name:ident) => {
        impl EntityExpr for $name {
            type Marker = EntityMarker;

            fn entity(&self) -> EntityMarker {
                self.entity
            }

            fn prop_type(&self) -> PropType {
                PropType::Empty
            }

            fn nullable(&self) -> bool {
                false
            }
        }
    };
}

/// A comparison of two values, whole or element-wise as their types decide.
#[derive(Clone)]
struct CmpExpr {
    op: BinaryOp,
    lhs: Arc<dyn DynCreateOp>,
    rhs: Arc<dyn DynCreateOp>,
    entity: EntityMarker,
}
entity_expr!(CmpExpr);

impl CreateOp for CmpExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.lhs.create_node_op(graph.clone())?;
        let right = self.rhs.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(self.lhs.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.rhs.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = comparison_shape(&self.op, &lhs_pt, &rhs_pt, rhs_const.as_ref())?;
        Ok(Arc::new(BinaryValueNodeOp {
            left,
            right,
            kernel: cmp_kernel(self.op, shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.lhs.create_edge_op(graph.clone())?;
        let right = self.rhs.create_edge_op(graph)?;
        let lhs_pt = resolved_prop_type(self.lhs.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.rhs.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = comparison_shape(&self.op, &lhs_pt, &rhs_pt, rhs_const.as_ref())?;
        Ok(Arc::new(BinaryValueEdgeOp {
            left,
            right,
            kernel: cmp_kernel(self.op, shape),
            out,
        }))
    }
}

/// A string test of a string-valued (or list-of-strings-valued) side.
#[derive(Clone)]
struct StrExpr {
    op: StringOp,
    lhs: Arc<dyn DynCreateOp>,
    rhs: Arc<dyn DynCreateOp>,
    entity: EntityMarker,
}
entity_expr!(StrExpr);

impl CreateOp for StrExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.lhs.create_node_op(graph.clone())?;
        let right = self.rhs.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(self.lhs.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.rhs.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = string_shape(&lhs_pt, &rhs_pt, rhs_const.as_ref())?;
        Ok(Arc::new(BinaryValueNodeOp {
            left,
            right,
            kernel: str_kernel(self.op.clone(), shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.lhs.create_edge_op(graph.clone())?;
        let right = self.rhs.create_edge_op(graph)?;
        let lhs_pt = resolved_prop_type(self.lhs.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.rhs.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = string_shape(&lhs_pt, &rhs_pt, rhs_const.as_ref())?;
        Ok(Arc::new(BinaryValueEdgeOp {
            left,
            right,
            kernel: str_kernel(self.op.clone(), shape),
            out,
        }))
    }
}

/// Membership of a value in a fixed set.
#[derive(Clone)]
struct SetExpr {
    inner: Arc<dyn DynCreateOp>,
    values: Vec<Prop>,
    negated: bool,
    entity: EntityMarker,
}
entity_expr!(SetExpr);

impl CreateOp for SetExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_node_op(graph)?;
        let pt = resolved_prop_type(self.inner.prop_type(), inner.prop_type());
        let (out, shape, members) = set_shape(&pt, &self.values);
        Ok(Arc::new(UnaryValueNodeOp {
            inner,
            kernel: set_kernel(members, self.negated, shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_edge_op(graph)?;
        let pt = resolved_prop_type(self.inner.prop_type(), inner.prop_type());
        let (out, shape, members) = set_shape(&pt, &self.values);
        Ok(Arc::new(UnaryValueEdgeOp {
            inner,
            kernel: set_kernel(members, self.negated, shape),
            out,
        }))
    }
}

/// Whether a value is present (`is_some`) or missing (`is_none`).
#[derive(Clone)]
struct PresenceExpr {
    inner: Arc<dyn DynCreateOp>,
    op: UnaryOp,
    entity: EntityMarker,
}
entity_expr!(PresenceExpr);

impl PresenceExpr {
    fn check(&self) -> Result<(), GraphError> {
        if self.inner.nullable() {
            Ok(())
        } else {
            let name = match self.op {
                UnaryOp::IsSome => "is_some",
                UnaryOp::IsNone => "is_none",
            };
            Err(invalid(format!(
                "{name}() is not valid on an expression that always has a value"
            )))
        }
    }

    fn kernel(&self) -> impl Fn(Option<Prop>) -> Option<Prop> + Clone {
        let op = self.op;
        move |v| {
            Some(Prop::Bool(match op {
                UnaryOp::IsSome => v.is_some(),
                UnaryOp::IsNone => v.is_none(),
            }))
        }
    }
}

impl CreateOp for PresenceExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.check()?;
        Ok(Arc::new(UnaryValueNodeOp {
            inner: self.inner.create_node_op(graph)?,
            kernel: self.kernel(),
            out: PropType::Bool,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.check()?;
        Ok(Arc::new(UnaryValueEdgeOp {
            inner: self.inner.create_edge_op(graph)?,
            kernel: self.kernel(),
            out: PropType::Bool,
        }))
    }
}

/// `any()` / `all()` over an element-wise yes/no result.
#[derive(Clone)]
struct QualExpr {
    inner: Arc<dyn DynCreateOp>,
    all: bool,
    entity: EntityMarker,
}
entity_expr!(QualExpr);

impl CreateOp for QualExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_node_op(graph)?;
        qualified_type(&resolved_prop_type(
            self.inner.prop_type(),
            inner.prop_type(),
        ))?;
        Ok(if self.all {
            Arc::new(AllNodeOp { inner })
        } else {
            Arc::new(AnyNodeOp { inner })
        })
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_edge_op(graph)?;
        qualified_type(&resolved_prop_type(
            self.inner.prop_type(),
            inner.prop_type(),
        ))?;
        Ok(if self.all {
            Arc::new(AllEdgeOp { inner })
        } else {
            Arc::new(AnyEdgeOp { inner })
        })
    }
}

/// An aggregation directly over a history: walks the history instead of
/// taking it as a list.
#[derive(Clone)]
struct StreamedAggExpr {
    history: Arc<dyn DynCreateHistory>,
    agg: Agg,
    entity: EntityMarker,
}

impl EntityExpr for StreamedAggExpr {
    type Marker = EntityMarker;

    fn entity(&self) -> EntityMarker {
        self.entity
    }
}

impl StreamedAggExpr {
    fn check(&self, history_type: &PropType) -> Result<(), GraphError> {
        let name = match self.agg {
            Agg::Sum => "sum()",
            Agg::Avg => "avg()",
            Agg::Min => "min()",
            Agg::Max => "max()",
            Agg::First => "first()",
            Agg::Last => "last()",
            Agg::Len => "len()",
            Agg::Earliest => "earliest()",
            Agg::Latest => "latest()",
        };
        require_aggregable(history_type, name)
    }
}

impl CreateOp for StreamedAggExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let history = self.history.create_node_history(Arc::new(graph))?;
        self.check(&history.history_type())?;
        Ok(Arc::new(StreamedAggNodeOp::new(history, self.agg)))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let history = self.history.create_edge_history(Arc::new(graph))?;
        self.check(&history.history_type())?;
        Ok(Arc::new(StreamedAggEdgeOp::new(history, self.agg)))
    }
}

/// A test of each history value against a constant, before the property's
/// type is known.
#[derive(Clone)]
enum QualTest {
    Cmp(BinaryOp, Prop),
    Str(StringOp, Prop),
    In(Vec<Prop>, bool),
}

impl QualTest {
    /// The test to run on each value of a history of `history_type`, when
    /// the history's element-wise result is one yes/no answer per value.
    /// Anything else (a nested list, a mismatch) is left to the list path,
    /// which reports it the way it always has.
    fn value_test(&self, history_type: &PropType) -> Option<ValueTest> {
        let one_per_value = |(out, shape): (PropType, Shape)| {
            shape == Shape::Elementwise && out == list(PropType::Bool)
        };
        match self {
            QualTest::Cmp(op, constant) => {
                let shape =
                    comparison_shape(op, history_type, &constant.dtype(), Some(constant)).ok()?;
                one_per_value(shape).then(|| ValueTest::Cmp(*op, constant.clone()))
            }
            QualTest::Str(op, constant) => {
                let shape = string_shape(history_type, &constant.dtype(), Some(constant)).ok()?;
                one_per_value(shape).then(|| ValueTest::Str(op.clone(), constant.clone()))
            }
            QualTest::In(values, negated) => {
                let (out, shape, members) = set_shape(history_type, values);
                one_per_value((out, shape)).then(|| {
                    ValueTest::In(
                        Arc::new(members.into_iter().map(HashableProp).collect()),
                        *negated,
                    )
                })
            }
        }
    }
}

/// `any()` / `all()` over a comparison of a history with a constant: walks
/// the history and stops at the first value that decides the answer.
#[derive(Clone)]
struct StreamedQualExpr {
    history: Arc<dyn DynCreateHistory>,
    test: QualTest,
    all: bool,
    /// The list path, for a history this test cannot walk value by value.
    fallback: Arc<QualExpr>,
    entity: EntityMarker,
}
entity_expr!(StreamedQualExpr);

impl CreateOp for StreamedQualExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let history = self.history.create_node_history(Arc::new(graph.clone()))?;
        match self.test.value_test(&history.history_type()) {
            Some(test) => Ok(Arc::new(StreamedQualNodeOp {
                history,
                test,
                all: self.all,
            })),
            None => self.fallback.create_node_op(graph),
        }
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let history = self.history.create_edge_history(Arc::new(graph.clone()))?;
        match self.test.value_test(&history.history_type()) {
            Some(test) => Ok(Arc::new(StreamedQualEdgeOp {
                history,
                test,
                all: self.all,
            })),
            None => self.fallback.create_edge_op(graph),
        }
    }
}

/// `and` / `or` of yes/no values.
#[derive(Clone)]
struct BoolCombineExpr {
    items: Vec<Arc<dyn DynCreateOp>>,
    all: bool,
    entity: EntityMarker,
}
entity_expr!(BoolCombineExpr);

impl BoolCombineExpr {
    fn name(&self) -> &'static str {
        if self.all {
            "and"
        } else {
            "or"
        }
    }
}

impl CreateOp for BoolCombineExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        if self.items.is_empty() {
            return Err(invalid(format!(
                "`{}` needs at least one operand",
                self.name()
            )));
        }
        let mut items = Vec::with_capacity(self.items.len());
        for item in &self.items {
            let op = item.create_node_op(graph.clone())?;
            require_bool(
                &resolved_prop_type(item.prop_type(), op.prop_type()),
                self.name(),
            )?;
            items.push(op);
        }
        Ok(Arc::new(NaryBoolNodeOp {
            items,
            all: self.all,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        if self.items.is_empty() {
            return Err(invalid(format!(
                "`{}` needs at least one operand",
                self.name()
            )));
        }
        let mut items = Vec::with_capacity(self.items.len());
        for item in &self.items {
            let op = item.create_edge_op(graph.clone())?;
            require_bool(
                &resolved_prop_type(item.prop_type(), op.prop_type()),
                self.name(),
            )?;
            items.push(op);
        }
        Ok(Arc::new(NaryBoolEdgeOp {
            items,
            all: self.all,
        }))
    }
}

/// `not` of a yes/no value.
#[derive(Clone)]
struct BoolNotExpr {
    inner: Arc<dyn DynCreateOp>,
    entity: EntityMarker,
}
entity_expr!(BoolNotExpr);

fn not_kernel(v: Option<Prop>) -> Option<Prop> {
    Some(Prop::Bool(!truthy(&v)))
}

impl CreateOp for BoolNotExpr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_node_op(graph)?;
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), inner.prop_type()),
            "not",
        )?;
        Ok(Arc::new(UnaryValueNodeOp {
            inner,
            kernel: not_kernel,
            out: PropType::Bool,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.inner.create_edge_op(graph)?;
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), inner.prop_type()),
            "not",
        )?;
        Ok(Arc::new(UnaryValueEdgeOp {
            inner,
            kernel: not_kernel,
            out: PropType::Bool,
        }))
    }
}

// ── predicates ───────────────────────────────────────────────────────────────

/// A yes/no value expression applied as a filter on its entity.
#[derive(Clone)]
struct Predicate {
    entity: EntityMarker,
    inner: Arc<dyn DynCreateOp>,
    /// Where the node filter can start instead of at every node: the nodes the
    /// predicate names by id, or the candidates a property index hands over.
    pushdown: Option<Pushdown>,
}

impl Predicate {
    fn new<L: Leaf>(expr: &Expr<L>, pushdown: Option<Pushdown>) -> Result<Self, GraphError> {
        Ok(Predicate {
            entity: L::ENTITY,
            inner: expr.compile_value()?,
            pushdown,
        })
    }

    /// The nodes the filter starts from, resolved once against `graph`. `None`
    /// when nothing narrows it: the filter then scans every node. Whatever comes
    /// back is a superset of the matches, and `apply` still runs on each node.
    fn narrowed_domain<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        match self.pushdown.as_ref()? {
            Pushdown::Ids(ids) => {
                let id_type = graph.id_type();
                let elems = ids
                    .iter()
                    .map(|v| gid_for_id_lookup(id_type, v))
                    .collect::<Option<Vec<GID>>>()?
                    .into_iter()
                    .filter_map(|gid| graph.internalise_node(gid.as_node_ref()))
                    .collect();
                Some(NodeList::List { elems })
            }
            Pushdown::Index(query) => query.candidates(graph),
        }
    }

    fn node_filter<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        let op = self.inner.create_node_op(graph.clone())?;
        let nodes = self.narrowed_domain(&graph);
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), op.prop_type()),
            "a filter",
        )?;
        let filter: Arc<dyn NodeOp<Output = bool> + 'graph> = Arc::new(op.map(|v| truthy(&v)));
        Ok(match nodes {
            Some(nodes) => Arc::new(DomainNodeOp {
                nodes,
                inner: filter,
            }),
            None => filter,
        })
    }

    fn edge_filter<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = bool> + 'graph>, GraphError> {
        let op = self.inner.create_edge_op(graph)?;
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), op.prop_type()),
            "a filter",
        )?;
        Ok(Arc::new(TruthyEdgeOp { inner: op }))
    }
}

impl CreateFilter for Predicate {
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
        Ok(match self.entity {
            EntityMarker::Node => {
                let filter = self.node_filter(graph.clone())?;
                Arc::new(NodeFilteredGraph::new(graph, filter))
            }
            EntityMarker::Edge => {
                let filter = self.edge_filter(graph.clone())?;
                Arc::new(EdgeExprFilteredGraph::new(graph, filter))
            }
            EntityMarker::ExplodedEdge => {
                let filter = self.edge_filter(graph.clone())?;
                Arc::new(ExplodedEdgeExprFilteredGraph::new(graph, filter))
            }
            EntityMarker::Const => return Err(invalid("a constant is not a filter")),
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if !matches!(self.entity, EntityMarker::Node) {
            return Err(GraphError::NotNodeFilter);
        }
        self.node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        match self.entity {
            EntityMarker::Edge => self.edge_filter(graph),
            // A node or exploded-edge predicate still says which edges survive: the
            // ones the filtered graph keeps.
            EntityMarker::Node | EntityMarker::ExplodedEdge => Ok(Arc::new(EdgeExistsOp::new(
                self.create_graph_filter(graph)?,
            ))),
            EntityMarker::Const => Err(invalid("a constant is not a filter")),
        }
    }
}

/// What lets a node predicate start somewhere smaller than every node.
#[derive(Clone)]
enum Pushdown {
    /// `id == v` or `id in [..]` on the bare id field: those nodes.
    Ids(Vec<Prop>),
    /// A test a property index can answer with a candidate set.
    Index(IndexQuery),
}

/// A property test in the shape the storage's index answers: one read on one
/// side, one constant on the other, no view on the read.
#[derive(Clone)]
struct IndexQuery {
    read: IndexedRead,
    test: IndexTest,
    /// The read is the whole history under `any()`: any value ever held may match.
    ever: bool,
}

#[derive(Clone)]
enum IndexedRead {
    Property {
        name: String,
        metadata: bool,
    },
    /// The node name is its external id, which the id index covers.
    Name,
}

#[derive(Clone)]
enum IndexTest {
    Eq(Prop),
    Lt(Prop),
    Le(Prop),
    Gt(Prop),
    Ge(Prop),
    In(HashSet<HashableProp>),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
}

impl IndexTest {
    fn predicate(&self) -> NodePropPredicate<'_> {
        match self {
            IndexTest::Eq(v) => NodePropPredicate::Eq(v),
            IndexTest::Lt(v) => NodePropPredicate::Lt(v),
            IndexTest::Le(v) => NodePropPredicate::Le(v),
            IndexTest::Gt(v) => NodePropPredicate::Gt(v),
            IndexTest::Ge(v) => NodePropPredicate::Ge(v),
            IndexTest::In(values) => NodePropPredicate::In(values),
            IndexTest::StartsWith(s) => NodePropPredicate::StartsWith(s),
            IndexTest::EndsWith(s) => NodePropPredicate::EndsWith(s),
            IndexTest::Contains(s) => NodePropPredicate::Contains(s),
        }
    }

    fn is_pattern(&self) -> bool {
        matches!(
            self,
            IndexTest::StartsWith(_) | IndexTest::EndsWith(_) | IndexTest::Contains(_)
        )
    }
}

impl IndexQuery {
    /// The candidates the graph's index has for this test, or `None` when no
    /// index can serve it. A restricted view's latest value can differ from the
    /// global one, so under a window or layer the query asks for every value
    /// ever held, a superset, and drops the index's exactness claim.
    fn candidates<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        let plain_view = !graph.window_filtered() && !graph.is_layer_filtered();
        let (prop_id, metadata, semantics, exact_allowed) = match &self.read {
            IndexedRead::Property { name, metadata } => {
                let prop_id = graph.node_meta().get_prop_id(name, *metadata)?;
                let (semantics, exact) = match (self.ever, plain_view) {
                    (true, plain) => (NodePropSemantics::Ever, plain),
                    (false, true) => (NodePropSemantics::Latest, true),
                    (false, false) => (NodePropSemantics::Ever, false),
                };
                (prop_id, *metadata, semantics, exact)
            }
            IndexedRead::Name => (NODE_ID_PROP_ID, true, NodePropSemantics::Latest, false),
        };
        let mut candidates = graph.core_graph().node_prop_candidates(
            prop_id,
            metadata,
            &self.test.predicate(),
            semantics,
        )?;
        candidates.exact &= exact_allowed;
        // index candidates come ascending and deduplicated, as `from_sorted` needs
        Some(NodeList::List {
            elems: Index::from_sorted(candidates.vids, candidates.exact),
        })
    }
}

/// How a node predicate can be narrowed, if at all.
fn pushdown(expr: &NodeExpr) -> Option<Pushdown> {
    if let Some(ids) = named_ids(expr) {
        return Some(Pushdown::Ids(ids));
    }
    let (inner, ever) = match expr {
        Expr::Any(inner) => (&**inner, true),
        other => (other, false),
    };
    let (read, test) = index_test(inner, ever)?;
    // The id index answers patterns on the name; equality on it is a scan.
    if matches!(read, IndexedRead::Name) && !test.is_pattern() {
        return None;
    }
    Some(Pushdown::Index(IndexQuery { read, test, ever }))
}

/// A comparison, string test or membership with an indexable read on one side
/// and a constant on the other.
fn index_test(expr: &NodeExpr, ever: bool) -> Option<(IndexedRead, IndexTest)> {
    match expr {
        Expr::Cmp(op, l, r) => {
            let (read, value, op) = match (&**l, &**r) {
                (read, Expr::Const(v)) => (indexed_read(read, ever)?, v, *op),
                (Expr::Const(v), read) => (indexed_read(read, ever)?, v, flipped(*op)),
                _ => return None,
            };
            let test = match op {
                CmpOp::Eq => IndexTest::Eq(value.clone()),
                CmpOp::Lt => IndexTest::Lt(value.clone()),
                CmpOp::Le => IndexTest::Le(value.clone()),
                CmpOp::Gt => IndexTest::Gt(value.clone()),
                CmpOp::Ge => IndexTest::Ge(value.clone()),
                CmpOp::Ne => return None,
            };
            Some((read, test))
        }
        Expr::Str(op, l, r) => {
            let read = indexed_read(l, ever)?;
            let Expr::Const(Prop::Str(s)) = &**r else {
                return None;
            };
            let test = match op {
                StrOp::StartsWith => IndexTest::StartsWith(s.to_string()),
                StrOp::EndsWith => IndexTest::EndsWith(s.to_string()),
                StrOp::Contains => IndexTest::Contains(s.to_string()),
                StrOp::NotContains | StrOp::FuzzySearch { .. } => return None,
            };
            Some((read, test))
        }
        Expr::In {
            expr,
            values,
            negated: false,
        } => {
            let read = indexed_read(expr, ever)?;
            let values = values.iter().cloned().map(HashableProp).collect();
            Some((read, IndexTest::In(values)))
        }
        _ => None,
    }
}

/// A read the index covers: a property, a metadata entry or the name, without
/// a view. Under `any()` it is the property's history; otherwise its latest
/// value, which is also what the latest update of the history is.
fn indexed_read(expr: &NodeExpr, ever: bool) -> Option<IndexedRead> {
    match expr {
        Expr::Read(NodeLeaf::Property {
            views,
            name,
            temporal,
        }) if views.is_empty() && *temporal == ever => Some(IndexedRead::Property {
            name: name.clone(),
            metadata: false,
        }),
        Expr::Read(NodeLeaf::Metadata { views, name }) if views.is_empty() && !ever => {
            Some(IndexedRead::Property {
                name: name.clone(),
                metadata: true,
            })
        }
        Expr::Read(NodeLeaf::Field {
            views,
            field: Field::Name,
        }) if views.is_empty() && !ever => Some(IndexedRead::Name),
        Expr::Agg(Agg::Latest, inner) if !ever => match &**inner {
            Expr::Read(NodeLeaf::Property {
                views,
                name,
                temporal: true,
            }) if views.is_empty() => Some(IndexedRead::Property {
                name: name.clone(),
                metadata: false,
            }),
            _ => None,
        },
        _ => None,
    }
}

/// The comparison with its sides swapped.
fn flipped(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        same => same,
    }
}

/// The node ids a node predicate names outright, if it is `id == v` or
/// `id in [..]` on the bare id field.
fn named_ids(expr: &NodeExpr) -> Option<Vec<Prop>> {
    fn is_bare_id(e: &NodeExpr) -> bool {
        matches!(
            e,
            Expr::Read(NodeLeaf::Field { views, field: Field::Id }) if views.is_empty()
        )
    }
    match expr {
        Expr::Cmp(CmpOp::Eq, lhs, rhs) => match (&**lhs, &**rhs) {
            (id, Expr::Const(v)) | (Expr::Const(v), id) if is_bare_id(id) => Some(vec![v.clone()]),
            _ => None,
        },
        Expr::In {
            expr,
            values,
            negated: false,
        } if is_bare_id(expr) => Some(values.clone()),
        _ => None,
    }
}

// ── filters ──────────────────────────────────────────────────────────────────

impl FilterExpr {
    /// The erased, applicable form of this filter.
    ///
    /// A view (`View`) applies first: the graph is seen through it and the other
    /// legs run inside it, reads included, the way `graph.window(..).filter(expr)`
    /// does. A view therefore stands alone or is a leg of the top-level `and`
    /// (nested `and`s count as top level); under `or` or `not` it has no meaning the
    /// engine can give it and is refused.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
        let (views, predicates, saw_view) = self.split_top_views();
        if saw_view && views.is_empty() {
            return Err(invalid("a view filter needs at least one view"));
        }
        if views.is_empty() {
            return self.compile_nested();
        }
        let inner: Arc<dyn DynCreateFilter> = if predicates.is_empty() {
            Arc::new(GraphFilter)
        } else {
            combine(
                predicates.iter().map(|p| p.compile_nested()),
                "and",
                |left, right| Arc::new(AndFilter { left, right }),
            )?
        };
        Ok(Arc::new(Viewed { views, inner }))
    }

    /// The view ops at the top of the filter, in order, and the predicates beside
    /// them. `and` nests flatten; anything else is a predicate. The flag says whether
    /// a `View` node was seen at all, so an empty one can be told from none.
    fn split_top_views(&self) -> (Vec<ViewOp>, Vec<&FilterExpr>, bool) {
        fn walk<'a>(
            filter: &'a FilterExpr,
            views: &mut Vec<ViewOp>,
            predicates: &mut Vec<&'a FilterExpr>,
            saw_view: &mut bool,
        ) {
            match filter {
                FilterExpr::View(ops) => {
                    *saw_view = true;
                    views.extend(ops.iter().cloned());
                }
                FilterExpr::And(items) => {
                    for item in items {
                        walk(item, views, predicates, saw_view);
                    }
                }
                other => predicates.push(other),
            }
        }
        let (mut views, mut predicates, mut saw_view) = (Vec::new(), Vec::new(), false);
        walk(self, &mut views, &mut predicates, &mut saw_view);
        (views, predicates, saw_view)
    }

    /// A filter below the top level: every node but a view.
    fn compile_nested(&self) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
        Ok(match self {
            FilterExpr::Node(expr) => Arc::new(Predicate::new(expr, pushdown(expr))?),
            FilterExpr::Edge(expr) => Arc::new(Predicate::new(expr, None)?),
            FilterExpr::ExplodedEdge(expr) => Arc::new(Predicate::new(expr, None)?),
            FilterExpr::View(_) => {
                return Err(invalid(
                    "a view applies to the whole filter: use it alone or as a leg of the \
                     top-level `and`, not under `or` or `not`",
                ))
            }
            FilterExpr::And(items) => combine(
                items.iter().map(Self::compile_nested),
                "and",
                |left, right| Arc::new(AndFilter { left, right }),
            )?,
            FilterExpr::Or(items) => combine(
                items.iter().map(Self::compile_nested),
                "or",
                |left, right| Arc::new(OrFilter { left, right }),
            )?,
            FilterExpr::Not(inner) => Arc::new(NotFilter(inner.compile_nested()?)),
            FilterExpr::Opaque(filter) => filter.0.clone(),
        })
    }
}

/// The graph-level view a chain of view ops describes, applied in order.
fn compile_view(views: &[ViewOp]) -> DynView {
    let mut v: DynView = Arc::new(GraphFilter);
    for op in views {
        v = match op {
            ViewOp::Window { start, end } => v.window(*start, *end),
            ViewOp::At(t) => v.at(*t),
            ViewOp::After(t) => v.after(*t),
            ViewOp::Before(t) => v.before(*t),
            ViewOp::Latest => Arc::new(v.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(v.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(v.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(v.layer(Layer::from(names.clone()))),
        };
    }
    v
}

/// A filter applied inside a view: the graph is seen through `views` first and
/// `inner` runs on that graph, reads included, so `and: [view, pred]` is
/// `graph.view(..).filter(pred)`. As a per-node or per-edge predicate it also asks
/// that the entity exist in the view, the way the filtered graph would.
#[derive(Clone)]
struct Viewed {
    views: Vec<ViewOp>,
    inner: Arc<dyn DynCreateFilter>,
}

impl Viewed {
    fn view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        compile_view(&self.views).create_dyn_graph_filter(Arc::new(graph))
    }
}

impl CreateFilter for Viewed {
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
        let viewed = self.view(graph)?;
        self.inner.create_dyn_graph_filter(viewed)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        let inside = self.inner.create_dyn_node_filter(viewed.clone())?;
        Ok(Arc::new(NodeExistsOp::new(viewed).and(inside)))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        let inside = self.inner.create_dyn_edge_filter(viewed.clone())?;
        Ok(Arc::new(AndEdgeOp {
            left: EdgeExistsOp::new(viewed),
            right: inside,
        }))
    }
}

/// Fold compiled operands pairwise, left to right. An empty list has no
/// meaning either way (`and` of nothing is not "everything", `or` of nothing
/// is not "nothing" the caller asked for), so it is refused.
fn combine(
    mut compiled: impl Iterator<Item = Result<Arc<dyn DynCreateFilter>, GraphError>>,
    name: &str,
    join: impl Fn(Arc<dyn DynCreateFilter>, Arc<dyn DynCreateFilter>) -> Arc<dyn DynCreateFilter>,
) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
    let first = compiled
        .next()
        .ok_or_else(|| invalid(format!("`{name}` needs at least one operand")))??;
    compiled.try_fold(first, |acc, next| Ok(join(acc, next?)))
}

/// A tree is a filter in its own right: applying it compiles it first.
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
        self.compile()?.create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.compile()?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.compile()?.create_edge_filter(graph)
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use raphtory_api::core::entities::properties::prop::IntoProp;

    fn prop(name: &str, temporal: bool) -> NodeExpr {
        Expr::Read(NodeLeaf::property(Vec::new(), name.to_owned(), temporal))
    }

    fn c(v: impl IntoProp) -> NodeExpr {
        Expr::Const(v.into_prop())
    }

    fn cmp(op: CmpOp, l: NodeExpr, r: NodeExpr) -> NodeExpr {
        Expr::Cmp(op, Box::new(l), Box::new(r))
    }

    #[test]
    fn a_history_read_walks_and_a_latest_value_does_not() {
        assert!(prop("score", true).history().is_some());
        assert!(prop("score", false).history().is_none());
        assert!(Expr::Agg(Agg::Sum, Box::new(prop("score", true)))
            .history()
            .is_none());
    }

    #[test]
    fn a_history_compared_with_a_constant_walks_from_either_side() {
        let gt = cmp(CmpOp::Gt, prop("score", true), c(4i64));
        assert!(matches!(
            gt.streamed_test(),
            Some((_, QualTest::Cmp(BinaryOp::Gt, Prop::I64(4))))
        ));
        let mirrored = cmp(CmpOp::Lt, c(4i64), prop("score", true));
        assert!(matches!(
            mirrored.streamed_test(),
            Some((_, QualTest::Cmp(BinaryOp::Gt, Prop::I64(4))))
        ));
        let contains = Expr::Str(
            StrOp::Contains,
            Box::new(prop("name", true)),
            Box::new(c("a")),
        );
        assert!(matches!(
            contains.streamed_test(),
            Some((_, QualTest::Str(StringOp::Contains, _)))
        ));
        let is_in = Expr::In {
            expr: Box::new(prop("score", true)),
            values: vec![1i64.into_prop()],
            negated: true,
        };
        assert!(matches!(
            is_in.streamed_test(),
            Some((_, QualTest::In(_, true)))
        ));
    }

    #[test]
    fn anything_else_under_any_keeps_the_list() {
        let latest = cmp(CmpOp::Gt, prop("score", false), c(4i64));
        assert!(latest.streamed_test().is_none());
        let two_reads = cmp(CmpOp::Gt, prop("score", true), prop("other", true));
        assert!(two_reads.streamed_test().is_none());
        let aggregated = cmp(
            CmpOp::Gt,
            Expr::Agg(Agg::Sum, Box::new(prop("score", true))),
            c(4i64),
        );
        assert!(aggregated.streamed_test().is_none());
    }

    #[test]
    fn only_a_one_answer_per_value_test_walks() {
        let history = list(PropType::I64);
        let gt = QualTest::Cmp(BinaryOp::Gt, 4i64.into_prop());
        assert!(gt.value_test(&history).is_some());
        // A constant list compares against the whole history, not each value.
        let whole = QualTest::Cmp(BinaryOp::Eq, Prop::list([1i64, 2i64]));
        assert!(whole.value_test(&history).is_none());
        // A mismatch is left to the list path, which reports it.
        assert!(gt.value_test(&list(PropType::Str)).is_none());
        // A history of lists answers per element, not per value.
        assert!(gt.value_test(&list(list(PropType::I64))).is_none());
        // A set no value can be in still answers per value, as the list path does.
        let none = QualTest::In(vec!["x".into_prop()], false);
        assert!(
            matches!(none.value_test(&history), Some(ValueTest::In(members, false)) if members.is_empty())
        );
        // An empty set is a whole-history test, which the list path refuses.
        let empty = QualTest::In(Vec::new(), false);
        assert!(empty.value_test(&history).is_none());
    }
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;
    use raphtory_api::core::entities::properties::prop::IntoProp;

    fn prop(name: &str, temporal: bool) -> NodeExpr {
        Expr::Read(NodeLeaf::property(Vec::new(), name.to_owned(), temporal))
    }

    fn c(v: impl IntoProp) -> NodeExpr {
        Expr::Const(v.into_prop())
    }

    fn index_of(expr: &NodeExpr) -> Option<(String, bool, bool)> {
        match pushdown(expr)? {
            Pushdown::Index(q) => Some((
                match q.read {
                    IndexedRead::Property { name, metadata } => {
                        if metadata {
                            format!("metadata {name}")
                        } else {
                            name
                        }
                    }
                    IndexedRead::Name => "name".to_owned(),
                },
                q.ever,
                q.test.is_pattern(),
            )),
            Pushdown::Ids(_) => None,
        }
    }

    #[test]
    fn plain_property_tests_reach_the_index_from_either_side() {
        let gt = Expr::Cmp(CmpOp::Gt, Box::new(prop("score", false)), Box::new(c(4i64)));
        assert_eq!(index_of(&gt), Some(("score".to_owned(), false, false)));
        let flipped = Expr::Cmp(CmpOp::Lt, Box::new(c(4i64)), Box::new(prop("score", false)));
        assert!(matches!(
            pushdown(&flipped),
            Some(Pushdown::Index(IndexQuery {
                test: IndexTest::Gt(_),
                ..
            }))
        ));
        let contains = Expr::Str(
            StrOp::Contains,
            Box::new(prop("name", false)),
            Box::new(c("acme")),
        );
        assert_eq!(index_of(&contains), Some(("name".to_owned(), false, true)));
        let members = Expr::In {
            expr: Box::new(prop("tag", false)),
            values: vec!["a".into_prop(), "b".into_prop()],
            negated: false,
        };
        assert_eq!(index_of(&members), Some(("tag".to_owned(), false, false)));
    }

    #[test]
    fn a_history_under_any_asks_for_every_value_ever_held() {
        let any = Expr::Any(Box::new(Expr::Cmp(
            CmpOp::Eq,
            Box::new(prop("score", true)),
            Box::new(c(4i64)),
        )));
        assert_eq!(index_of(&any), Some(("score".to_owned(), true, false)));
        // The latest update of a history is the property's latest value.
        let latest = Expr::Cmp(
            CmpOp::Eq,
            Box::new(Expr::Agg(Agg::Latest, Box::new(prop("score", true)))),
            Box::new(c(4i64)),
        );
        assert_eq!(index_of(&latest), Some(("score".to_owned(), false, false)));
    }

    #[test]
    fn what_the_index_cannot_answer_scans() {
        let viewed = Expr::Cmp(
            CmpOp::Eq,
            Box::new(Expr::Read(NodeLeaf::property(
                vec![ViewOp::Latest],
                "score".to_owned(),
                false,
            ))),
            Box::new(c(4i64)),
        );
        assert!(pushdown(&viewed).is_none());
        let ne = Expr::Cmp(CmpOp::Ne, Box::new(prop("score", false)), Box::new(c(4i64)));
        assert!(pushdown(&ne).is_none());
        let not_in = Expr::In {
            expr: Box::new(prop("tag", false)),
            values: vec!["a".into_prop()],
            negated: true,
        };
        assert!(pushdown(&not_in).is_none());
        let name_eq = Expr::Cmp(
            CmpOp::Eq,
            Box::new(Expr::Read(NodeLeaf::Field {
                views: Vec::new(),
                field: Field::Name,
            })),
            Box::new(c("bob")),
        );
        assert!(pushdown(&name_eq).is_none());
        let name_prefix = Expr::Str(
            StrOp::StartsWith,
            Box::new(Expr::Read(NodeLeaf::Field {
                views: Vec::new(),
                field: Field::Name,
            })),
            Box::new(c("bo")),
        );
        assert_eq!(
            index_of(&name_prefix),
            Some(("name".to_owned(), false, true))
        );
        let both_sides = Expr::Cmp(
            CmpOp::Eq,
            Box::new(prop("a", false)),
            Box::new(prop("b", false)),
        );
        assert!(pushdown(&both_sides).is_none());
    }
}
