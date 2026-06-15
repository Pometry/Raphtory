//! Node expressions — what value a node can produce.
//!
//! An expression is a pure data structure (no graph reference). It describes *what to compute*
//! without computing it. Call [`NodeExpr::create_node_op`] to compile it against a specific graph
//! view, performing name→ID resolution once.
//!
//! # Field expressions
//!
//! ```rust,ignore
//! NodeFilter.id()         // Id        — NodeExpr<Output = GID>         — e.g. .eq(GID::Str("v1".into()))
//! NodeFilter::name()       // Name      — NodeExpr<Output = String>       — e.g. .eq("Alice")
//! NodeFilter::node_type()  // Type      — NodeExpr<Output = Option<ArcStr>> — e.g. .is_some()
//! ```
//!
//! # Degree expressions
//!
//! ```rust,ignore
//! NodeFilter::degree()      // DegreeExpr — NodeExpr<Output = usize> — e.g. .gt(2usize)
//! NodeFilter::in_degree()   // DegreeExpr — NodeExpr<Output = usize> — e.g. .eq(0usize)  (no in-edges)
//! NodeFilter::out_degree()  // DegreeExpr — NodeExpr<Output = usize> — e.g. .gt(NodeFilter::in_degree())
//! ```
//!
//! # Property expressions
//!
//! ```rust,ignore
//! NodeFilter::property("age")              // Property — NodeExpr<Output = Option<Prop>> — e.g. .gt(30i64)
//! NodeFilter::property("score").is_some()  // Property — nodes where "score" is set
//! NodeFilter::metadata("region")           // Metadata — NodeExpr<Output = Option<Prop>> — e.g. .eq(Prop::Str("EU".into()))
//! ```
//!
//! # Temporal property expressions
//!
//! ```rust,ignore
//! NodeFilter::temporal_property("score")  // TemporalPropertyExpr — NodeExpr<Output = Prop> (Prop::List of all values in window)
//!
//! // Quantifiers (QuantifiedNodeFilter via AnyMode / AllMode):
//! NodeFilter::temporal_property("score").any().gt(10i64)   // pass if any value > 10
//! NodeFilter::temporal_property("score").all().gt(0i64)    // pass if every value > 0
//!
//! // Aggregators (BinaryCmpNodeFilter via SumExpr / AvgExpr / etc.):
//! NodeFilter::temporal_property("price").sum().gt(100i64)      // SumExpr  — pass if total > 100
//! NodeFilter::temporal_property("price").avg().lt(50i64)       // AvgExpr  — pass if average < 50
//! NodeFilter::temporal_property("ts").len().gt(3usize)         // LenExpr  — pass if more than 3 updates
//! NodeFilter::temporal_property("ts").first().eq(Prop::I64(0)) // FirstExpr — pass if first value == 0
//! NodeFilter::temporal_property("ts").last().eq(Prop::I64(1))  // LastExpr  — pass if last value == 1
//! NodeFilter::temporal_property("v").min().gt(0i64)            // MinExpr  — pass if minimum > 0
//! NodeFilter::temporal_property("v").max().lt(100i64)          // MaxExpr  — pass if maximum < 100
//! ```
//!
//! # Literal (RHS) expressions
//!
//! ```rust,ignore
//! // Plain Rust values implement NodeExpr — pass them directly as the RHS of any comparison:
//! NodeFilter::degree().gt(2usize)                    // usize  — NodeExpr<Output = usize>
//! NodeFilter::name().eq("Alice")                     // &str   — NodeExpr<Output = &'static str>
//! NodeFilter::name().eq("Bob".to_string())           // String — NodeExpr<Output = String>
//! NodeFilter::property("age").gt(30i64)              // i64    — NodeExpr<Output = Option<Prop>>
//! NodeFilter::property("score").eq(Prop::F64(9.5))  // Prop   — NodeExpr<Output = Option<Prop>>
//! // ConstExpr<T> for custom comparable types not covered above
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
// ─────────────────────────────────────────────────────────────────────────────
// Node field expressions — identity, name, type
//
// Id, Name, Type are zero-sized structs defined in db::api::state::ops.
// NodeExpr is implemented here so they can appear as LHS or RHS in filter expressions.
//   NodeFilter::id()         uses Id    — NodeExpr<Output = GID>
//   NodeFilter::name()       uses Name  — NodeExpr<Output = String>
//   NodeFilter::node_type()  uses Type  — NodeExpr<Output = Option<ArcStr>>
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
//   NodeFilter::degree().gt(2usize)
//   NodeFilter::name().eq("Alice")
//   NodeFilter::property("age").gt(30i64)
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
        Ok(Arc::new(Const(*self.into())))
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
        Ok(Arc::new(Const(self.clone().into_prop())))
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
        Ok(Arc::new(Const(Some(*self.into_prop()))))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IntoPropNodeExpr — normalises any RHS value to NodeExpr<Output = Option<Prop>>
//
// Used as the bound on Quantified::eq/ne/gt/ge/lt/le and NodeAggregated::eq/ne/…
// so that .eq("Alice"), .eq(30i64), and .eq(NodeFilter::property("x")) all work
// with a single method name.
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
                Ok(Arc::new(Const(Some(*self))))
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

impl<T: Comparable + Clone + Send + Sync + 'static> NodeExpr for ConstExpr<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.0.clone())))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Named property / degree expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Degree of a node in a given direction.
///
/// Created by `NodeFilter::degree()` / `::in_degree()` / `::out_degree()`.
/// `E` is the view expression that scopes the edges counted (window / layer / etc.).
/// Compiles to the `Degree` op from `db::api::state::ops`.
///
/// ```rust,ignore
/// NodeFilter::degree().gt(2usize)
/// NodeFilter::out_degree().gt(NodeFilter::in_degree())
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
            .map(|a| Some(Prop::U64(a.into()))),
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
/// Produces `Prop::List` of every recorded value within the view.
///
/// Not constructed directly — created internally by the fluent chain started
/// by `NodeFilter::temporal_property(name)`:
///
/// ```rust,ignore
/// // NodeFilter::temporal_property("score") returns TemporalProp, not this type.
/// // TemporalPropertyExpr is created inside .any() / .all() / .sum() etc., e.g.:
/// //   .any().gt(10i64)  →  QuantifiedNodeFilter<TemporalPropertyExpr<..>, AnyMode, i64>
/// //   .sum().gt(100i64) →  BinaryCmpNodeFilter<SumExpr<TemporalPropertyExpr<..>>, i64>
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
// Each wraps a NodeExpr<Output = Prop> (typically TemporalPropertyExpr) and reduces
// the Prop::List it produces to a scalar.  Not constructed directly —
// TemporalProp / TemporalExprOps methods return NodeAggregated<XxxExpr<..>>:
//
//   .temporal_property("v").sum()  → NodeAggregated<SumExpr<TemporalPropertyExpr<..>>>
//   .temporal_property("v").len()  → NodeAggregated<LenExpr<TemporalPropertyExpr<..>>>
//
// Calling .gt() / .eq() etc. on NodeAggregated then produces:
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
