//! From expression data to a filter the engine can apply.
//!
//! Reads and views replay onto the erased factories exactly as the typed API
//! would build them. Comparisons, tests and combinators become value
//! expressions ([`CmpExpr`], [`QualExpr`], …) whose result type is decided
//! when they are built against a graph, because only then are property types
//! known. A [`Predicate`] turns a `Bool`-typed value expression into a filter
//! on the entity the expression belongs to.

use super::{
    Agg, CmpOp, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeExpr, NodeLeaf, StrOp,
    ViewOp,
};
use crate::{
    db::{
        api::{
            state::NodeOp,
            view::internal::{DynGraphArc, GraphView, NodeList},
        },
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                and_filter::AndFilter,
                comparable_set_values,
                dyn_factory::{DynEdgeFilterFactory, DynNodeFilterFactory},
                edge_expr::EdgeOp,
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
                        AnyEdgeOp, AnyNodeOp, IdDomainNodeOp,
                    },
                    CreateOp, DynCreateOp, EntityExpr, Scoped,
                },
                node_filter::NodeFilter,
                not_filter::NotFilter,
                or_filter::OrFilter,
                resolved_prop_type, validate_binary_op, validate_const_comparable,
                validate_string_op, validate_types_comparable, DynCreateFilter, DynView,
                EntityMarker, ViewWrapOps,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{EntityAggOps, Layer},
};
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::prop::{Prop, PropType},
        GID, VID,
    },
    Direction,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{fmt::Debug, sync::Arc};

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

    /// Whether the read is scoped by a view.
    fn has_view(&self) -> bool;
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

    fn has_view(&self) -> bool {
        match self {
            NodeLeaf::Field { views, .. }
            | NodeLeaf::Degree { views, .. }
            | NodeLeaf::Property { views, .. }
            | NodeLeaf::Metadata { views, .. }
            | NodeLeaf::IsActive { views } => !views.is_empty(),
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

    fn has_view(&self) -> bool {
        match self {
            ExplodedEdgeLeaf::Property { views, .. }
            | ExplodedEdgeLeaf::Metadata { views, .. }
            | ExplodedEdgeLeaf::IsActive { views }
            | ExplodedEdgeLeaf::IsValid { views }
            | ExplodedEdgeLeaf::IsDeleted { views }
            | ExplodedEdgeLeaf::IsSelfLoop { views } => !views.is_empty(),
        }
    }
}

// ── values ───────────────────────────────────────────────────────────────────

impl<L: Leaf> Expr<L> {
    /// The erased, compilable value this expression stands for.
    pub fn compile_value(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let entity = L::ENTITY;
        Ok(match self {
            Expr::Const(value) => Arc::new(value.clone()),
            Expr::Read(leaf) => leaf.compile()?,
            Expr::Agg(agg, inner) => {
                let op = inner.compile_value()?;
                match agg {
                    Agg::Sum => Arc::new(op.sum()),
                    Agg::Avg => Arc::new(op.avg()),
                    Agg::Min => Arc::new(op.min()),
                    Agg::Max => Arc::new(op.max()),
                    Agg::First => Arc::new(op.first()),
                    Agg::Last => Arc::new(op.last()),
                    Agg::Len => Arc::new(op.len()),
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
            Expr::Any(inner) => Arc::new(QualExpr {
                inner: inner.compile_value()?,
                all: false,
                entity,
            }),
            Expr::All(inner) => Arc::new(QualExpr {
                inner: inner.compile_value()?,
                all: true,
                entity,
            }),
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
    if *pt == PropType::Bool {
        Ok(())
    } else {
        Err(invalid(format!(
            "{what} needs a yes/no answer, but this expression has type {pt}"
        )))
    }
}

fn truthy(v: &Option<Prop>) -> bool {
    matches!(v, Some(Prop::Bool(true)))
}

// ── runtime ops ──────────────────────────────────────────────────────────────

macro_rules! value_ops {
    ($op_trait:ident, $id:ty, $binary:ident, $unary:ident, $nary:ident $(, $domain:item)?) => {
        struct $binary<'g, K> {
            left: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            right: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            kernel: K,
            out: PropType,
        }

        impl<'g, K> $op_trait for $binary<'g, K>
        where
            K: Fn(Option<Prop>, Option<Prop>) -> Option<Prop> + Send + Sync,
        {
            type Output = Option<Prop>;
            $($domain)?
            fn prop_type(&self) -> PropType {
                self.out.clone()
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                (self.kernel)(self.left.apply(storage, id), self.right.apply(storage, id))
            }
        }

        struct $unary<'g, K> {
            inner: Arc<dyn $op_trait<Output = Option<Prop>> + 'g>,
            kernel: K,
            out: PropType,
        }

        impl<'g, K> $op_trait for $unary<'g, K>
        where
            K: Fn(Option<Prop>) -> Option<Prop> + Send + Sync,
        {
            type Output = Option<Prop>;
            $($domain)?
            fn prop_type(&self) -> PropType {
                self.out.clone()
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                (self.kernel)(self.inner.apply(storage, id))
            }
        }

        /// `and` / `or` over yes/no values, short-circuiting.
        struct $nary<'g> {
            items: Vec<Arc<dyn $op_trait<Output = Option<Prop>> + 'g>>,
            all: bool,
        }

        impl<'g> $op_trait for $nary<'g> {
            type Output = Option<Prop>;
            $($domain)?
            fn prop_type(&self) -> PropType {
                PropType::Bool
            }
            fn apply(&self, storage: &GraphStorage, id: $id) -> Option<Prop> {
                let hit = if self.all {
                    self.items.iter().all(|item| truthy(&item.apply(storage, id)))
                } else {
                    self.items.iter().any(|item| truthy(&item.apply(storage, id)))
                };
                Some(Prop::Bool(hit))
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
    EdgeRef,
    BinaryValueEdgeOp,
    UnaryValueEdgeOp,
    NaryBoolEdgeOp
);

/// Adapts a yes/no edge value to the plain boolean the filtered graphs consume.
struct TruthyEdgeOp<'g> {
    inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> EdgeOp for TruthyEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        truthy(&self.inner.apply(storage, edge))
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
    let values = Arc::new(values);
    move |v| {
        let member = |v: Option<Prop>| {
            let v = v?;
            let present = values.iter().any(|x| x.equals(&v));
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
    /// Node ids the predicate names outright (`id == v`, `id in [..]`), so the
    /// node filter can start from those nodes instead of scanning every one.
    ids: Option<Vec<Prop>>,
}

impl Predicate {
    fn new<L: Leaf>(expr: &Expr<L>, ids: Option<Vec<Prop>>) -> Result<Self, GraphError> {
        Ok(Predicate {
            entity: L::ENTITY,
            inner: expr.compile_value()?,
            ids,
        })
    }

    fn node_filter<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        let id_type = graph.id_type();
        let op = self.inner.create_node_op(graph)?;
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), op.prop_type()),
            "a filter",
        )?;
        let filter: Arc<dyn NodeOp<Output = bool> + 'graph> = Arc::new(op.map(|v| truthy(&v)));
        let gids: Option<Vec<GID>> = self
            .ids
            .as_ref()
            .and_then(|ids| ids.iter().map(|v| gid_for_id_lookup(id_type, v)).collect());
        Ok(match gids {
            Some(gids) => Arc::new(IdDomainNodeOp {
                gids: Arc::from(gids),
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(match self.entity {
            EntityMarker::Node => {
                Arc::new(NodeFilteredGraph::new(graph, self.node_filter(filtered)?))
            }
            EntityMarker::Edge => Arc::new(EdgeExprFilteredGraph::new(
                graph,
                self.edge_filter(filtered)?,
            )),
            EntityMarker::ExplodedEdge => Arc::new(ExplodedEdgeExprFilteredGraph::new(
                graph,
                self.edge_filter(filtered)?,
            )),
            EntityMarker::Const => return Err(invalid("a constant is not a filter")),
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        if !matches!(self.entity, EntityMarker::Node) {
            return Err(GraphError::NotNodeFilter);
        }
        self.node_filter(filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
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
            FilterExpr::Node(expr) => Arc::new(Predicate::new(expr, named_ids(expr))?),
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
/// `graph.view(..).filter(pred)`.
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
        compile_view(&self.views).dyn_filter_graph_view(Arc::new(graph))
    }
}

impl CreateFilter for Viewed {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let viewed = self.view(graph)?;
        self.inner.create_dyn_filter(viewed, Arc::new(filtered))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        let viewed = self.view(graph)?;
        self.inner
            .create_dyn_node_filter(viewed, Arc::new(filtered))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        self.inner.dyn_filter_graph_view(viewed)
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
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = DynGraphArc<'graph>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        Arc<dyn NodeOp<Output = bool> + 'graph>;

    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        self.compile()?.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        self.compile()?.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.compile()?.filter_graph_view(graph)
    }
}
