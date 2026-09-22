//! From the typed expression API to the tree.
//!
//! The typed builders (`NodeFilter.property("score").gt(4)`) stay the way rust
//! callers write a filter; here they turn into the same data a python object
//! or a GraphQL request carries, and compile through the one compiler in
//! [`super::compile`]. A factory chain names its entity and its views
//! ([`FactoryLeaf`]); every typed expression converts to an [`Expr`]
//! ([`ToExpr`]); a typed predicate converts to a [`FilterExpr`]
//! ([`ToFilterExpr`]) and gets its `CreateFilter` from that.

use super::{
    Agg, CmpOp, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, Leaf, NodeLeaf, StrOp, ViewOp,
};
use crate::{
    db::{
        api::{
            state::{
                ops::node::{Id, Name, Type},
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                edge_filter::{EdgeEndpointWrapper, EdgeFilter, Endpoint},
                exploded_edge_filter::ExplodedEdgeFilter,
                filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
                is_active_edge_filter::IsActiveEdge,
                is_active_node_filter::IsActiveNode,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                latest_filter::Latest,
                layered_filter::Layered,
                node_expr::{
                    AllExpr, AnyExpr, AvgExpr, BinaryCmpExpr, ConstExpr, DegreeExpr, EntityExpr,
                    FirstExpr, LastExpr, LenExpr, Marker, MaxExpr, MinExpr, PropValueSetExpr,
                    StringExpr, SumExpr, TemporalPropExpr, UnaryExpr,
                },
                node_filter::NodeFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                windowed_filter::Windowed,
                MetadataExpr, PropertyExpr,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::Layer,
};
use raphtory_api::core::entities::{
    properties::prop::{IntoProp, Prop},
    GID,
};
use std::sync::Arc;

// ── factories ────────────────────────────────────────────────────────────────

/// A factory chain: the entity it reads from and the views on the way,
/// outermost wrapper last.
pub trait FactoryLeaf: Clone {
    type Leaf: Leaf;

    fn views(&self) -> Vec<ViewOp>;
}

impl FactoryLeaf for NodeFilter {
    type Leaf = NodeLeaf;

    fn views(&self) -> Vec<ViewOp> {
        Vec::new()
    }
}

impl FactoryLeaf for EdgeFilter {
    type Leaf = EdgeLeaf;

    fn views(&self) -> Vec<ViewOp> {
        Vec::new()
    }
}

impl FactoryLeaf for ExplodedEdgeFilter {
    type Leaf = ExplodedEdgeLeaf;

    fn views(&self) -> Vec<ViewOp> {
        Vec::new()
    }
}

impl<T: FactoryLeaf> FactoryLeaf for Windowed<T> {
    type Leaf = T::Leaf;

    fn views(&self) -> Vec<ViewOp> {
        let mut views = self.inner.views();
        views.push(ViewOp::Window {
            start: self.start,
            end: self.end,
        });
        views
    }
}

impl<T: FactoryLeaf> FactoryLeaf for Layered<T> {
    type Leaf = T::Leaf;

    fn views(&self) -> Vec<ViewOp> {
        let mut views = self.inner.views();
        views.extend(layer_view(&self.layer));
        views
    }
}

impl<T: FactoryLeaf> FactoryLeaf for Latest<T> {
    type Leaf = T::Leaf;

    fn views(&self) -> Vec<ViewOp> {
        let mut views = self.inner.views();
        views.push(ViewOp::Latest);
        views
    }
}

impl<T: FactoryLeaf> FactoryLeaf for SnapshotAt<T> {
    type Leaf = T::Leaf;

    fn views(&self) -> Vec<ViewOp> {
        let mut views = self.inner.views();
        views.push(ViewOp::SnapshotAt(self.time));
        views
    }
}

impl<T: FactoryLeaf> FactoryLeaf for SnapshotLatest<T> {
    type Leaf = T::Leaf;

    fn views(&self) -> Vec<ViewOp> {
        let mut views = self.inner.views();
        views.push(ViewOp::SnapshotLatest);
        views
    }
}

/// The view op a layer restriction is; `Layer::All` restricts nothing.
fn layer_view(layer: &Layer) -> Option<ViewOp> {
    let names = match layer {
        Layer::All => return None,
        Layer::None => Vec::new(),
        Layer::Default => vec!["_default".to_string()],
        Layer::One(name) => vec![name.to_string()],
        Layer::Multiple(names) => names.iter().map(ToString::to_string).collect(),
    };
    Some(ViewOp::Layers(names))
}

/// The leaf type a predicate's entity marker stands for.
pub trait MarkerLeaf: Marker {
    type Leaf: Leaf;
}

impl MarkerLeaf for NodeFilter {
    type Leaf = NodeLeaf;
}

impl MarkerLeaf for EdgeFilter {
    type Leaf = EdgeLeaf;
}

impl MarkerLeaf for ExplodedEdgeFilter {
    type Leaf = ExplodedEdgeLeaf;
}

// ── values ───────────────────────────────────────────────────────────────────

/// A typed expression as tree data over the leaves of one entity.
pub trait ToExpr<L: Leaf> {
    fn to_expr(&self) -> Expr<L>;
}

macro_rules! const_to_expr {
    ($($t:ty),* $(,)?) => {$(
        impl<L: Leaf> ToExpr<L> for $t {
            fn to_expr(&self) -> Expr<L> {
                Expr::Const(self.clone().into_prop())
            }
        }
    )*};
}

const_to_expr!(
    Prop,
    String,
    &'static str,
    bool,
    u8,
    u16,
    u32,
    u64,
    i32,
    i64,
    f32,
    f64
);

impl<L: Leaf> ToExpr<L> for usize {
    fn to_expr(&self) -> Expr<L> {
        Expr::Const(Prop::U64(*self as u64))
    }
}

impl<L: Leaf> ToExpr<L> for GID {
    fn to_expr(&self) -> Expr<L> {
        Expr::Const(match self {
            GID::U64(id) => Prop::U64(*id),
            GID::Str(name) => Prop::str(name.clone()),
        })
    }
}

impl<L: Leaf, T: Into<Prop> + Clone> ToExpr<L> for ConstExpr<T> {
    fn to_expr(&self) -> Expr<L> {
        Expr::Const(self.0.clone().into())
    }
}

impl<E: FactoryLeaf> ToExpr<E::Leaf> for PropertyExpr<E> {
    fn to_expr(&self) -> Expr<E::Leaf> {
        Expr::Read(E::Leaf::property(
            self.view_expr.views(),
            self.name.clone(),
            false,
        ))
    }
}

impl<E: FactoryLeaf> ToExpr<E::Leaf> for TemporalPropExpr<E> {
    fn to_expr(&self) -> Expr<E::Leaf> {
        Expr::Read(E::Leaf::property(
            self.view_expr.views(),
            self.name.clone(),
            true,
        ))
    }
}

impl<E: FactoryLeaf> ToExpr<E::Leaf> for MetadataExpr<E> {
    fn to_expr(&self) -> Expr<E::Leaf> {
        Expr::Read(E::Leaf::metadata(self.view_expr.views(), self.name.clone()))
    }
}

impl<E: FactoryLeaf<Leaf = NodeLeaf>> ToExpr<NodeLeaf> for DegreeExpr<E> {
    fn to_expr(&self) -> Expr<NodeLeaf> {
        Expr::Read(NodeLeaf::Degree {
            views: self.view_expr.views(),
            direction: self.dir,
        })
    }
}

macro_rules! field_to_expr {
    ($($t:ident => $field:ident),* $(,)?) => {$(
        impl ToExpr<NodeLeaf> for $t {
            fn to_expr(&self) -> Expr<NodeLeaf> {
                Expr::Read(NodeLeaf::Field {
                    views: Vec::new(),
                    field: Field::$field,
                })
            }
        }
    )*};
}

field_to_expr!(Id => Id, Name => Name, Type => NodeType);

impl ToExpr<NodeLeaf> for IsActiveNode {
    fn to_expr(&self) -> Expr<NodeLeaf> {
        Expr::Read(NodeLeaf::is_active(Vec::new()))
    }
}

macro_rules! edge_structural_to_expr {
    ($($t:ident => $variant:ident),* $(,)?) => {$(
        impl ToExpr<EdgeLeaf> for $t {
            fn to_expr(&self) -> Expr<EdgeLeaf> {
                Expr::Read(EdgeLeaf::$variant { views: Vec::new() })
            }
        }

        impl ToExpr<ExplodedEdgeLeaf> for $t {
            fn to_expr(&self) -> Expr<ExplodedEdgeLeaf> {
                Expr::Read(ExplodedEdgeLeaf::$variant { views: Vec::new() })
            }
        }
    )*};
}

edge_structural_to_expr!(
    IsActiveEdge => IsActive,
    IsValidEdge => IsValid,
    IsDeletedEdge => IsDeleted,
    IsSelfLoopEdge => IsSelfLoop,
);

// A view wrapped around an expression scopes every read inside it.

impl<L: Leaf, T: ToExpr<L>> ToExpr<L> for Windowed<T> {
    fn to_expr(&self) -> Expr<L> {
        let mut expr = self.inner.to_expr();
        expr.push_view(ViewOp::Window {
            start: self.start,
            end: self.end,
        });
        expr
    }
}

impl<L: Leaf, T: ToExpr<L>> ToExpr<L> for Layered<T> {
    fn to_expr(&self) -> Expr<L> {
        let mut expr = self.inner.to_expr();
        if let Some(op) = layer_view(&self.layer) {
            expr.push_view(op);
        }
        expr
    }
}

impl<L: Leaf, T: ToExpr<L>> ToExpr<L> for Latest<T> {
    fn to_expr(&self) -> Expr<L> {
        let mut expr = self.inner.to_expr();
        expr.push_view(ViewOp::Latest);
        expr
    }
}

impl<L: Leaf, T: ToExpr<L>> ToExpr<L> for SnapshotAt<T> {
    fn to_expr(&self) -> Expr<L> {
        let mut expr = self.inner.to_expr();
        expr.push_view(ViewOp::SnapshotAt(self.time));
        expr
    }
}

impl<L: Leaf, T: ToExpr<L>> ToExpr<L> for SnapshotLatest<T> {
    fn to_expr(&self) -> Expr<L> {
        let mut expr = self.inner.to_expr();
        expr.push_view(ViewOp::SnapshotLatest);
        expr
    }
}

macro_rules! agg_to_expr {
    ($($t:ident => $agg:ident),* $(,)?) => {$(
        impl<L: Leaf, E: ToExpr<L>> ToExpr<L> for $t<E> {
            fn to_expr(&self) -> Expr<L> {
                Expr::Agg(Agg::$agg, Box::new(self.0.to_expr()))
            }
        }
    )*};
}

agg_to_expr!(
    SumExpr => Sum,
    AvgExpr => Avg,
    MinExpr => Min,
    MaxExpr => Max,
    FirstExpr => First,
    LastExpr => Last,
    LenExpr => Len,
);

impl<L: Leaf, E: ToExpr<L>> ToExpr<L> for AnyExpr<E> {
    fn to_expr(&self) -> Expr<L> {
        Expr::Any(Box::new(self.0.to_expr()))
    }
}

impl<L: Leaf, E: ToExpr<L>> ToExpr<L> for AllExpr<E> {
    fn to_expr(&self) -> Expr<L> {
        Expr::All(Box::new(self.0.to_expr()))
    }
}

impl<L: Leaf, Lhs: ToExpr<L>, Rhs: ToExpr<L>, M> ToExpr<L> for BinaryCmpExpr<Lhs, Rhs, M> {
    fn to_expr(&self) -> Expr<L> {
        let op = match self.op {
            BinaryOp::Eq => CmpOp::Eq,
            BinaryOp::Ne => CmpOp::Ne,
            BinaryOp::Lt => CmpOp::Lt,
            BinaryOp::Le => CmpOp::Le,
            BinaryOp::Gt => CmpOp::Gt,
            BinaryOp::Ge => CmpOp::Ge,
        };
        Expr::Cmp(
            op,
            Box::new(self.left.to_expr()),
            Box::new(self.right.to_expr()),
        )
    }
}

impl<L: Leaf, Lhs: ToExpr<L>, Rhs: ToExpr<L>, M> ToExpr<L> for StringExpr<Lhs, Rhs, M> {
    fn to_expr(&self) -> Expr<L> {
        let op = match &self.op {
            StringOp::StartsWith => StrOp::StartsWith,
            StringOp::EndsWith => StrOp::EndsWith,
            StringOp::Contains => StrOp::Contains,
            StringOp::NotContains => StrOp::NotContains,
            StringOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => StrOp::FuzzySearch {
                levenshtein_distance: *levenshtein_distance,
                prefix_match: *prefix_match,
            },
        };
        Expr::Str(
            op,
            Box::new(self.left.to_expr()),
            Box::new(self.right.to_expr()),
        )
    }
}

impl<L: Leaf, E: ToExpr<L>, M> ToExpr<L> for UnaryExpr<E, M> {
    fn to_expr(&self) -> Expr<L> {
        let inner = Box::new(self.expr.to_expr());
        match self.op {
            UnaryOp::IsSome => Expr::IsSome(inner),
            UnaryOp::IsNone => Expr::IsNone(inner),
        }
    }
}

impl<L: Leaf, E: ToExpr<L>, M> ToExpr<L> for PropValueSetExpr<E, M> {
    fn to_expr(&self) -> Expr<L> {
        Expr::In {
            expr: Box::new(self.expr.to_expr()),
            values: self.values.clone(),
            negated: matches!(self.op, SetOp::IsNotIn),
        }
    }
}

/// A node expression read at one end of the edge.
impl<T: ToExpr<NodeLeaf>> ToExpr<EdgeLeaf> for EdgeEndpointWrapper<T> {
    fn to_expr(&self) -> Expr<EdgeLeaf> {
        let inner = Box::new(self.inner.to_expr());
        Expr::Read(match self.endpoint() {
            Endpoint::Src => EdgeLeaf::Src(inner),
            Endpoint::Dst => EdgeLeaf::Dst(inner),
        })
    }
}

// ── filters ──────────────────────────────────────────────────────────────────

/// A typed predicate as a filter tree.
pub trait ToFilterExpr {
    fn to_filter_expr(&self) -> FilterExpr;
}

impl<Lhs, Rhs, M: MarkerLeaf> ToFilterExpr for BinaryCmpExpr<Lhs, Rhs, M>
where
    Self: ToExpr<M::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        M::Leaf::filter(self.to_expr())
    }
}

impl<Lhs, Rhs, M: MarkerLeaf> ToFilterExpr for StringExpr<Lhs, Rhs, M>
where
    Self: ToExpr<M::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        M::Leaf::filter(self.to_expr())
    }
}

impl<E, M: MarkerLeaf> ToFilterExpr for UnaryExpr<E, M>
where
    Self: ToExpr<M::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        M::Leaf::filter(self.to_expr())
    }
}

impl<E, M: MarkerLeaf> ToFilterExpr for PropValueSetExpr<E, M>
where
    Self: ToExpr<M::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        M::Leaf::filter(self.to_expr())
    }
}

impl<E: EntityExpr> ToFilterExpr for AnyExpr<E>
where
    E::Marker: MarkerLeaf,
    Self: ToExpr<<E::Marker as MarkerLeaf>::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        <E::Marker as MarkerLeaf>::Leaf::filter(self.to_expr())
    }
}

impl<E: EntityExpr> ToFilterExpr for AllExpr<E>
where
    E::Marker: MarkerLeaf,
    Self: ToExpr<<E::Marker as MarkerLeaf>::Leaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        <E::Marker as MarkerLeaf>::Leaf::filter(self.to_expr())
    }
}

impl<T> ToFilterExpr for EdgeEndpointWrapper<T>
where
    Self: ToExpr<EdgeLeaf>,
{
    fn to_filter_expr(&self) -> FilterExpr {
        EdgeLeaf::filter(self.to_expr())
    }
}

/// A typed predicate is applied by converting it to its tree and compiling
/// that: one compiler, one set of checks, whatever built the filter.
macro_rules! compile_through_tree {
    ($(impl<$($g:ident),*> for $ty:ty;)+) => {$(
        impl<$($g),*> CreateFilter for $ty
        where
            Self: ToFilterExpr + Clone + Send + Sync + 'static,
        {
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
                self.to_filter_expr().compile()?.create_filter(graph, filtered)
            }

            fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
                self,
                graph: G,
                filtered: F,
            ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
                self.to_filter_expr()
                    .compile()?
                    .create_node_filter(graph, filtered)
            }

            fn filter_graph_view<'graph, G: GraphView + 'graph>(
                &self,
                graph: G,
            ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
                self.to_filter_expr().compile()?.filter_graph_view(graph)
            }
        }
    )+};
}

compile_through_tree! {
    impl<L, R, M> for BinaryCmpExpr<L, R, M>;
    impl<L, R, M> for StringExpr<L, R, M>;
    impl<E, M> for UnaryExpr<E, M>;
    impl<E, M> for PropValueSetExpr<E, M>;
    impl<E> for AnyExpr<E>;
    impl<E> for AllExpr<E>;
    impl<T> for EdgeEndpointWrapper<T>;
}
