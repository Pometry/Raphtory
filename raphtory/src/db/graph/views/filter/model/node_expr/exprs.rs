//! The value expressions a filter tree compiles to on the node side.
//!
//! Filters are built as trees (`expr::builder`): `NodeFilter.degree()`,
//! `NodeFilter.property("score").temporal().sum()` and the rest return an
//! `Expr`, and any value that converts into a `Prop` is accepted as a
//! literal, so `NodeFilter.degree().gt(2u64)` and `NodeFilter.name().eq("alice")`
//! need no wrapper. The compiler (`expr::compile`) turns each term and
//! aggregate of a tree into one of the types here: `DegreeExpr`,
//! `TemporalPropExpr`, the aggregate expressions and the field expressions.
//! Each is a pure description; [`CreateOp::create_node_op`] compiles it
//! against a graph view, resolving names to ids once.

use super::{
    ops::{
        AvgNodeOp, EarliestNodeOp, FirstNodeOp, InViewNodeOp, LastNodeOp, LatestNodeOp, LenNodeOp,
        MaxNodeOp, MinNodeOp, NodeIdOp, SumNodeOp, TemporalNodePropOp,
    },
    AvgEdgeOp, CreateOp, EarliestEdgeOp, EntityExpr, FirstEdgeOp, IndexTerm, LastEdgeOp,
    LatestEdgeOp, LenEdgeOp, MaxEdgeOp, MinEdgeOp, SumEdgeOp,
};
use crate::{
    db::{
        api::{
            state::ops::{Const, Degree, Id, Name, NodeOp, Type},
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::model::{
            edge_expr::{ops::TemporalEdgePropOp, EdgeOp},
            expr::{
                stream::{StreamedAggEdgeOp, StreamedAggNodeOp},
                Agg, DynCreateHistory, EdgeHistory, NodeHistory,
            },
            require_aggregable, resolved_prop_type, CreateView, EntityMarker,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::{IntoProp, Prop, PropType},
    Direction,
};
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// Node field expressions — identity, name, type
//
// Id, Name, Type are zero-sized structs defined in db::api::state::ops.
// NodeExpr is implemented here so they can appear as LHS or RHS in filter expressions.
// All map their native types into Option<Prop> via into_prop():
//   NodeFilter.id()        uses Id   — produces Option<Prop> (GID mapped to Prop)
//   NodeFilter.name()      uses Name — produces Option<Prop> (String as Prop::Str)
//   NodeFilter.node_type() uses Type — produces Option<Prop> (ArcStr as Prop::Str, "_default" if unset)
// ─────────────────────────────────────────────────────────────────────────────

impl EntityExpr for Id {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Node
    }
}

impl CreateOp for Id {
    /// The id is a node field the id table answers outright. Read through an
    /// edge endpoint the field is the node's, but the endpoint does not forward
    /// this, since an edge predicate never narrows by a node index.
    fn index_term(&self) -> Option<IndexTerm> {
        Some(IndexTerm::Id)
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(NodeIdOp {
            id_type: graph.id_type(),
        }))
    }
}

impl EntityExpr for Name {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Node
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl CreateOp for Name {
    /// The name is the node's external id, which the id index answers patterns
    /// on; as for [`Id`], an edge endpoint does not forward this.
    fn index_term(&self) -> Option<IndexTerm> {
        Some(IndexTerm::Name)
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Name.map(|a| Some(a.into_prop()))))
    }
}

impl EntityExpr for Type {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Node
    }

    /// Every node has a type; an unset one reads as `"_default"`.
    fn nullable(&self) -> bool {
        false
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl CreateOp for Type {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        // Untyped nodes carry the storage's default type key, so a type test
        // sees the same key the node-type mask is built over.
        Ok(Arc::new(Type.map(|a| {
            Some(a.map_or_else(|| Prop::str("_default"), |b| b.into_prop()))
        })))
    }
}

/// A built-in node field (`Id`, `Name` or `Type`) read through a factory's
/// view chain: `NodeFilter.window(1, 5).name()`. The field's value does not
/// change with the view, but a node the view does not hold has no field there,
/// so under a view that can hide nodes the term is `None` for such a node.
#[derive(Clone)]
pub struct NodeFieldExpr<E, F> {
    pub(crate) view_expr: E,
    pub(crate) field: F,
}

impl<E, F> EntityExpr for NodeFieldExpr<E, F>
where
    E: CreateView,
    F: EntityExpr,
{
    fn entity(&self) -> EntityMarker {
        EntityMarker::Node
    }

    fn prop_type(&self) -> PropType {
        self.field.prop_type()
    }

    fn nullable(&self) -> bool {
        self.view_expr.narrows() || self.field.nullable()
    }
}

impl<E, F> CreateOp for NodeFieldExpr<E, F>
where
    E: CreateView,
    F: EntityExpr + CreateOp,
{
    fn index_term(&self) -> Option<IndexTerm> {
        if self.view_expr.narrows() {
            return None;
        }
        self.field.index_term()
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        if !self.view_expr.narrows() {
            return self.field.create_node_op(graph);
        }
        let graph = self.view_expr.create_view(graph)?;
        let term = self.field.create_node_op(graph.clone())?;
        Ok(Arc::new(InViewNodeOp { graph, term }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Prop scalar — NodeExpr impl
//
// All exprs produce Option<Prop>, so Prop itself (and numeric/string primitives)
// implement NodeExpr directly. Pass them as the RHS of any comparison:
//   .eq("Alice"), .gt(30i64), .eq(NodeFilter.property("x")) all share the same type.
// ─────────────────────────────────────────────────────────────────────────────

impl EntityExpr for Prop {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Const
    }

    fn prop_type(&self) -> PropType {
        self.dtype()
    }

    fn constant(&self) -> Option<Prop> {
        Some(self.clone())
    }
}

impl CreateOp for Prop {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Named property / degree expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Degree of a node in a given direction.
///
/// Created by `NodeFilter.degree()` / `.in_degree()` / `.out_degree()`.
/// `E` is the view expression that scopes the edges counted (window / layer / etc.).
/// Compiles to `Degree { dir, view }.map(|a| Some(Prop::U64(a as u64)))`.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegreeExpr<E> {
    pub dir: Direction,
    pub view_expr: E,
}

impl<E: CreateView + Clone + Send + Sync + 'static> EntityExpr for DegreeExpr<E> {
    fn entity(&self) -> EntityMarker {
        EntityMarker::Node
    }

    fn prop_type(&self) -> PropType {
        PropType::U64
    }
    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> CreateOp for DegreeExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(
            Degree {
                dir: self.dir,
                view: self.view_expr.create_view(graph)?,
            }
            .map(|a| Some(Prop::U64(a as u64))),
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalExpr<E> — all temporal values of a property over the view window
//
// One type for node and edge histories.
// Implements NodeExpr when E: NodeFilterFactory, EdgeExpr when E: EdgeFilterFactory.
// ─────────────────────────────────────────────────────────────────────────────

/// All temporal values of a named property over the current view window.
///
/// Implements `NodeExpr` when `E: NodeFilterFactory` and `EdgeExpr` when `E: EdgeFilterFactory`.
/// Constructed by `PropertyExpr::temporal()`. Implements `EntityExpr` so all
/// `EntityExprFilterOps` chain methods (`.gt()`, `.contains()`, `.any()`, etc.)
/// are available, plus `EntityAggOps` for aggregators (`.sum()`, `.last()`,
/// `.len()`, etc.).
#[derive(Clone)]
pub struct TemporalPropExpr<E: Clone> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
    pub(crate) entity: EntityMarker,
}

impl<E: CreateView> EntityExpr for TemporalPropExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }
}

impl<E: CreateView> DynCreateHistory for TemporalPropExpr<E> {
    fn create_node_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn NodeHistory + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalNodePropOp {
            graph,
            prop_id,
            narrows: self.view_expr.narrows(),
        }))
    }

    fn create_edge_history<'g>(
        &self,
        graph: DynGraphArc<'g>,
    ) -> Result<Arc<dyn EdgeHistory + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalEdgePropOp { graph, prop_id }))
    }
}

impl<E: CreateView> CreateOp for TemporalPropExpr<E> {
    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        Some(Arc::new(self.clone()))
    }

    fn index_term(&self) -> Option<IndexTerm> {
        if self.view_expr.narrows() {
            return None;
        }
        Some(IndexTerm::Property {
            name: self.name.clone(),
            metadata: false,
            ever: true,
        })
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(
            TemporalNodePropOp {
                graph,
                prop_id,
                narrows: self.view_expr.narrows(),
            }
            .map(|a| Some(a)),
        ))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalEdgePropOp { graph, prop_id }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator Exprs — NodeExpr wrappers producing a single scalar
//
// Each wraps an inner expression (typically TemporalPropExpr) and reduces
// the Prop::List it produces.  Not constructed directly —
// EntityAggOps methods on TemporalExpr return these exprs directly:
//
//   .property("v").temporal().sum()  → SumExpr<TemporalPropExpr<..>>
//   .property("v").temporal().len()  → LenExpr<TemporalPropExpr<..>>
//   .property("v").temporal().any()  → AnyExpr<TemporalPropExpr<..>>
//
// Calling .gt() / .eq() etc. on any of these (via EntityExprFilterOps) produces:
//   BinaryCmpExpr<SumExpr<TemporalPropExpr<..>>, RHS>
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// EntityAggOps — secondary aggregate operators on filter expression types
//
// Scoped narrowly (not blanket-impl) to avoid name collisions with stdlib methods
// like `Ord::min` / `Ord::max` / `Iterator::sum` on primitive `EntityExpr` types
// (`u64`, `i64`, etc. all implement `EntityExpr` as constant values).
// ─────────────────────────────────────────────────────────────────────────────

fn aggregates_a_list(agg: Agg) -> Result<(), GraphError> {
    if matches!(agg, Agg::Earliest | Agg::Latest) {
        return Err(GraphError::InvalidFilter(
            "earliest() and latest() pick an update of a temporal history; use first() or \
             last() for the elements of a list"
                .to_owned(),
        ));
    }
    Ok(())
}

/// The node op of an aggregate over `inner`. Over a history it walks the
/// values instead of taking them as one list; over anything else it reduces
/// the list value with `list_op`.
fn aggregate_node_op<'g, E: CreateOp, G: GraphView + 'g>(
    inner: &E,
    graph: G,
    agg: Agg,
    name: &str,
    list_op: impl FnOnce(
        Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    ) -> Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    if let Some(history) = inner.history() {
        let history = history.create_node_history(graph.into_dyn_graph_arc())?;
        require_aggregable(&history.history_type(), name)?;
        return Ok(Arc::new(StreamedAggNodeOp::new(history, agg)));
    }
    aggregates_a_list(agg)?;
    let op = inner.create_node_op(graph)?;
    require_aggregable(&resolved_prop_type(inner.prop_type(), op.prop_type()), name)?;
    Ok(list_op(op))
}

/// The edge op of an aggregate over `inner`; see [`aggregate_node_op`].
fn aggregate_edge_op<'g, E: CreateOp, G: GraphView + 'g>(
    inner: &E,
    graph: G,
    agg: Agg,
    name: &str,
    list_op: impl FnOnce(
        Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    ) -> Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    if let Some(history) = inner.history() {
        let history = history.create_edge_history(graph.into_dyn_graph_arc())?;
        require_aggregable(&history.history_type(), name)?;
        return Ok(Arc::new(StreamedAggEdgeOp::new(history, agg)));
    }
    aggregates_a_list(agg)?;
    let op = inner.create_edge_op(graph)?;
    require_aggregable(&resolved_prop_type(inner.prop_type(), op.prop_type()), name)?;
    Ok(list_op(op))
}

macro_rules! impl_agg_expr {
    ($expr:ident, $node_op_ty:ident, $edge_op_ty:ident, $agg:expr, $name:literal) => {
        impl_agg_expr!(@common $expr);

        impl<E: CreateOp> CreateOp for $expr<E> {
            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
                aggregate_node_op(&self.0, graph, $agg, $name, |inner| {
                    Arc::new($node_op_ty { inner })
                })
            }

            fn create_edge_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
                aggregate_edge_op(&self.0, graph, $agg, $name, |inner| {
                    Arc::new($edge_op_ty { inner })
                })
            }
        }
    };
    ($expr:ident) => {
        impl_agg_expr!(@common $expr);

    };
    (@common $expr:ident) => {
        #[derive(Clone)]
        pub struct $expr<E>(pub E);

        impl<E: EntityExpr> EntityExpr for $expr<E> {
            fn entity(&self) -> EntityMarker {
                self.0.entity()
            }
        }

    };
}

impl_agg_expr!(SumExpr, SumNodeOp, SumEdgeOp, Agg::Sum, "sum()");
impl_agg_expr!(AvgExpr, AvgNodeOp, AvgEdgeOp, Agg::Avg, "avg()");
impl_agg_expr!(MinExpr, MinNodeOp, MinEdgeOp, Agg::Min, "min()");
impl_agg_expr!(MaxExpr, MaxNodeOp, MaxEdgeOp, Agg::Max, "max()");
impl_agg_expr!(FirstExpr, FirstNodeOp, FirstEdgeOp, Agg::First, "first()");
impl_agg_expr!(LastExpr, LastNodeOp, LastEdgeOp, Agg::Last, "last()");
impl_agg_expr!(LenExpr, LenNodeOp, LenEdgeOp, Agg::Len, "len()");
impl_agg_expr!(
    EarliestExpr,
    EarliestNodeOp,
    EarliestEdgeOp,
    Agg::Earliest,
    "earliest()"
);

// `latest()` is the one aggregate that is also an indexable term: the latest
// update of a history is the property's latest value, which the index covers.
impl_agg_expr!(@common LatestExpr);

impl<E: CreateOp> CreateOp for LatestExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        aggregate_node_op(&self.0, graph, Agg::Latest, "latest()", |inner| {
            Arc::new(LatestNodeOp { inner })
        })
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        aggregate_edge_op(&self.0, graph, Agg::Latest, "latest()", |inner| {
            Arc::new(LatestEdgeOp { inner })
        })
    }

    fn index_term(&self) -> Option<IndexTerm> {
        self.0.index_term()?.latest_value()
    }
}

// `any()` / `all()` after a comparison: they collapse an element-wise result.
impl_agg_expr!(AnyExpr);
impl_agg_expr!(AllExpr);
