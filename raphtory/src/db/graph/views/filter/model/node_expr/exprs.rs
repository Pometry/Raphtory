//! Node expressions — what value a node can produce.
//!
//! An expression is a pure data structure (no graph reference). It describes *what to compute*
//! without computing it. Call [`NodeExpr::create_node_op`] to compile it against a specific graph
//! view, performing name→ID resolution once.
//!
//! # Field expressions
//!
//! All expressions produce `Option<Prop>` — field values are mapped via `into_prop()`.
//!
//! ```rust,ignore
//! NodeFilter.id()           // Id    — e.g. .eq(GID::Str("v1".into()))
//! NodeFilter.name()         // Name  — e.g. .eq("Alice")
//! NodeFilter.node_type()    // Type  — e.g. .is_some::<Prop>()
//! ```
//!
//! # Degree expressions
//!
//! ```rust,ignore
//! NodeFilter.degree()      // DegreeExpr — e.g. .gt(2usize)
//! NodeFilter.in_degree()   // DegreeExpr — e.g. .eq(0usize)
//! NodeFilter.out_degree()  // DegreeExpr — e.g. .gt(NodeFilter.in_degree())
//! ```
//!
//! # Property expressions
//!
//! ```rust,ignore
//! NodeFilter.property("age")                         // Property — e.g. .gt(30i64)
//! NodeFilter.property("score").is_some::<Prop>()     // nodes where "score" is set
//! NodeFilter.metadata("region")                      // Metadata — e.g. .eq(Prop::Str("EU".into()))
//! ```
//!
//! # Temporal property expressions
//!
//! Accessed via `.temporal()` on `PropertyExpr<E>` (returned by `.property("name")`):
//!
//! ```rust,ignore
//! // Quantifiers — compare element-wise then reduce with .any() / .all():
//! NodeFilter.property("score").temporal().gt(10i64).any()  // pass if any value > 10
//! NodeFilter.property("score").temporal().gt(0i64).all()   // pass if every value > 0
//!
//! // Aggregators:
//! NodeFilter.property("price").temporal().sum().gt(100i64)             // SumExpr  — pass if total > 100
//! NodeFilter.property("price").temporal().avg().lt(50i64)              // AvgExpr  — pass if average < 50
//! NodeFilter.property("ts").temporal().len().gt(3usize)                // LenExpr  — pass if more than 3 updates
//! NodeFilter.property("ts").temporal().first().eq(Prop::I64(0))        // FirstExpr — pass if first value == 0
//! NodeFilter.property("ts").temporal().last().eq(Prop::I64(1))         // LastExpr  — pass if last value == 1
//! NodeFilter.property("v").temporal().min().gt(0i64)                   // MinExpr  — pass if minimum > 0
//! NodeFilter.property("v").temporal().max().lt(100i64)                 // MaxExpr  — pass if maximum < 100
//! ```
//!
//! # Literal (RHS) expressions
//!
//! ```rust,ignore
//! // Plain Rust values implement NodeExpr and produce Option<Prop> — pass directly as RHS:
//! NodeFilter.degree().gt(2usize)                   // usize → Prop::U64
//! NodeFilter.name().eq("Alice")                    // &str  → Prop::Str
//! NodeFilter.name().eq("Bob".to_string())          // String → Prop::Str
//! NodeFilter.property("age").gt(30i64)             // i64   → Prop::I64
//! NodeFilter.property("score").eq(Prop::F64(9.5)) // Prop  → passed as-is
//! // ConstExpr<T: Comparable> for custom comparable types not covered above
//! ```

use super::{ops::{
    AvgNodeOp, FirstNodeOp, LastNodeOp, LenNodeOp, MaxNodeOp, MinNodeOp, NodeMetaOp,
    NodePropOp, SumNodeOp, TemporalNodePropOp,
}, AllEdgeOp, AllNodeOp, AnyEdgeOp, AnyNodeOp, AvgEdgeOp, EntityExpr, FirstEdgeOp, LastEdgeOp, LenEdgeOp, MaxEdgeOp, MinEdgeOp, NodeExpr, SumEdgeOp};
use crate::{
    db::{
        api::{
            state::ops::{Const, Degree, Id, Name, NodeOp, Type},
            view::internal::GraphView,
        },
        graph::views::filter::model::{
            filter_operator::Comparable, node_filter::NodeFilter, CreateView,
            Metadata, Property,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::IntoProp;
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        GID,
    },
    storage::arc_str::ArcStr,
    Direction,
};
use std::sync::Arc;
use crate::db::graph::views::filter::model::edge_expr::{EdgeExpr, EdgeOp};
// ─────────────────────────────────────────────────────────────────────────────
// Node field expressions — identity, name, type
//
// Id, Name, Type are zero-sized structs defined in db::api::state::ops.
// NodeExpr is implemented here so they can appear as LHS or RHS in filter expressions.
// All map their native types into Option<Prop> via into_prop():
//   NodeFilter.id()        uses Id   — produces Option<Prop> (GID mapped to Prop)
//   NodeFilter.name()      uses Name — produces Option<Prop> (String as Prop::Str)
//   NodeFilter.node_type() uses Type — produces Option<Prop> (ArcStr as Prop::Str, None if unset)
// ─────────────────────────────────────────────────────────────────────────────

impl EntityExpr for Id {}

impl NodeExpr for Id {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Id.map(|a| Some(a.into_prop()))))
    }
}

impl EntityExpr for GID {}

impl NodeExpr for GID {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for Name {
    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for Name {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Name.map(|a| Some(a.into_prop()))))
    }
}

impl EntityExpr for Type {
    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for Type {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Type.map(|a| a.map(|b| b.into_prop()))))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Constant value expressions — literal RHS values
//
// Allows passing raw values directly to filter operators:
//   NodeFilter.degree().gt(2usize)
//   NodeFilter.name().eq("Alice")
//   NodeFilter.property("age").gt(30i64)
// ─────────────────────────────────────────────────────────────────────────────

impl EntityExpr for usize {
    fn prop_type(&self) -> PropType {
        PropType::U64
    }
}

impl NodeExpr for usize {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::U64(*self as u64)))))
    }
}

impl EntityExpr for String {
    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for String {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for ArcStr {
    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for ArcStr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for &'static str {
    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for &'static str {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some((*self).into_prop()))))
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
    fn prop_type(&self) -> PropType {
        self.dtype()
    }
}

impl NodeExpr for Prop {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

macro_rules! impl_node_expr_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl EntityExpr for $prim {
            fn prop_type(&self) -> PropType {
                PropType::$variant
            }
        }

        impl NodeExpr for $prim {
            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                _graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
                Ok(Arc::new(Const(Some(Prop::$variant(*self)))))
            }
        }
    };
}

impl_node_expr_for_numeric!(i32, I32);
impl_node_expr_for_numeric!(i64, I64);
impl_node_expr_for_numeric!(u32, U32);
impl_node_expr_for_numeric!(u64, U64);
impl_node_expr_for_numeric!(f32, F32);
impl_node_expr_for_numeric!(f64, F64);
impl_node_expr_for_numeric!(bool, Bool);
impl_node_expr_for_numeric!(u8, U8);
impl_node_expr_for_numeric!(u16, U16);

/// A constant expression for custom output types not covered by the built-in impls.
///
/// Built-in types (`usize`, `String`, `Prop`, numerics, `&'static str`) implement
/// [`NodeExpr`] directly and can be passed as-is. `ConstExpr<T>` is only needed
/// for custom comparable types.
///
/// ```rust,ignore
/// some_expr.gt(ConstExpr(my_custom_value))
/// ```
#[derive(Clone)]
pub struct ConstExpr<T>(pub T);

impl<T: Comparable + Clone + Send + Sync + 'static> EntityExpr for ConstExpr<T> {}

impl<T: Comparable + Into<Prop> + Clone + Send + Sync + 'static> NodeExpr for ConstExpr<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.0.clone().into()))))
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
    fn prop_type(&self) -> PropType {
        PropType::U64
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> NodeExpr for DegreeExpr<E> {
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

impl EntityExpr for Property {}

impl NodeExpr for Property {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        Ok(Arc::new(NodePropOp { graph, prop_id }))
    }
}

impl EntityExpr for Metadata {}

impl NodeExpr for Metadata {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        Ok(Arc::new(NodeMetaOp { graph, prop_id }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Temporal property expression — returns Prop::List of all values in the window
// ─────────────────────────────────────────────────────────────────────────────

/// All temporal values of a named property over the current view window.
///
/// Produces `Some(Prop::List([...]))` of every recorded value within the view.
///
/// Not constructed directly — obtained from `NodeTemporalPropOps::into_expr()`,
/// or implicitly via `.sum()` / `.any()` / etc. on `TemporalProp`:
///
/// ```rust,ignore
/// // NodeFilter.property("score").temporal() returns TemporalProp, not this type.
/// // TemporalPropertyExpr is produced implicitly by NodeTemporalPropOps methods:
/// //   .gt(10i64).any()  → BinaryCmpNodeFilter<AnyExpr<BinaryCmpNodeFilter<TemporalPropertyExpr, i64>>, Prop>
/// //   .sum().gt(100i64) → BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr<..>>, i64>
/// ```
#[derive(Clone)]
pub struct TemporalPropertyExpr<E: Clone> {
    pub view_expr: E,
    pub name: String,
}

impl TemporalPropertyExpr<NodeFilter> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            view_expr: NodeFilter,
            name: name.into(),
        }
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> EntityExpr for TemporalPropertyExpr<E> {}

impl<E: CreateView + Clone + Send + Sync + 'static> NodeExpr for TemporalPropertyExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(
            TemporalNodePropOp { graph, prop_id }.map(|a| Some(a)),
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator Exprs — NodeExpr wrappers producing a single scalar
//
// Each wraps an inner NodeExpr (typically TemporalPropertyExpr) and reduces
// the Prop::List it produces.  Not constructed directly —
// TemporalProp methods return these exprs directly:
//
//   .property("v").temporal().sum()  → SumExpr<TemporalPropertyExpr<..>>
//   .property("v").temporal().len()  → LenExpr<TemporalPropertyExpr<..>>
//   .property("v").temporal().any()  → AnyExpr<TemporalPropertyExpr<..>>
//
// Calling .gt() / .eq() etc. on any of these (via NodeExprFilterOps) produces:
//   BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr<..>>, RHS>
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! impl_agg_expr {
    ($expr:ident, $node_op_ty:ident,  $edge_op_ty:ident) => {
        #[derive(Clone)]
        pub struct $expr<E>(pub E);

        impl<E: EntityExpr> EntityExpr for $expr<E> {}

        impl<E: NodeExpr> NodeExpr for $expr<E> {
            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
                let inner = self.0.create_node_op(graph)?;
                Ok(Arc::new($node_op_ty { inner }))
            }
        }

        impl<E: EdgeExpr> EdgeExpr for $expr<E> {
            fn create_edge_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
                let inner = self.0.create_edge_op(graph)?;
                Ok(Arc::new($edge_op_ty { inner }))
            }
        }
    };
}

impl_agg_expr!(SumExpr, SumNodeOp, SumEdgeOp);
impl_agg_expr!(AvgExpr, AvgNodeOp, AvgEdgeOp);
impl_agg_expr!(MinExpr, MinNodeOp, MinEdgeOp);
impl_agg_expr!(MaxExpr, MaxNodeOp, MaxEdgeOp);
impl_agg_expr!(FirstExpr, FirstNodeOp, FirstEdgeOp);
impl_agg_expr!(LastExpr, LastNodeOp, LastEdgeOp);
impl_agg_expr!(LenExpr, LenNodeOp, LenEdgeOp);
impl_agg_expr!(AnyExpr, AnyNodeOp, AnyEdgeOp);
impl_agg_expr!(AllExpr, AllNodeOp, AllEdgeOp);
