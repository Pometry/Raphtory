//! Node expressions — what value a node can produce.
//!
//! An expression is a pure data structure (no graph reference). It describes *what to compute*
//! without computing it. Call [`NodeExpr::create_node_op`] to compile it against a specific graph
//! view, performing name→ID resolution once.
//!
//! # Field expressions
//!
//! ```rust,ignore
//! NodeFilter::id()         // Id        — NodeExpr<Output = GID>         — e.g. .eq(GID::Str("v1".into()))
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

use super::{
    filters::{
        BinaryCmpNodeFilter, SetNodeFilter, StringNodeFilter, UnaryNodeFilter,
    },
    ops::{
        AvgNodeOp, FirstNodeOp, LastNodeOp, LenNodeOp, MaxNodeOp, MinNodeOp, NestedMapNodeOp,
        NodeMetaOp, NodePropOp, SumNodeOp, TemporalNodePropOp, UnwrapOptPropOp,
    },
    NodeExpr,
};
use crate::{
    db::{
        api::{
            state::ops::{Const, Degree, Id, Name, NodeOp, Type},
            view::internal::GraphView,
        },
        graph::views::filter::model::{
            filter_operator::{BinaryOp, Comparable, SetOp, StringOp, UnaryOp},
            node_filter::NodeFilter,
            property_filter::Op,
            CreateView, Metadata, Property,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        GID,
    },
    storage::arc_str::ArcStr,
    Direction,
};
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

// ─────────────────────────────────────────────────────────────────────────────
// Node field expressions — identity, name, type
//
// Id, Name, Type are zero-sized structs defined in db::api::state::ops.
// NodeExpr is implemented here so they can appear as LHS or RHS in filter expressions.
//   NodeFilter::id()         uses Id    — NodeExpr<Output = GID>
//   NodeFilter::name()       uses Name  — NodeExpr<Output = String>
//   NodeFilter::node_type()  uses Type  — NodeExpr<Output = Option<ArcStr>>
// ─────────────────────────────────────────────────────────────────────────────

impl NodeExpr for Id {
    type Output = GID;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = GID> + 'g>, GraphError> {
        Ok(Arc::new(Id))
    }
}

impl NodeExpr for GID {
    type Output = GID;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Self::Output> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.clone())))
    }
}

impl NodeExpr for Name {
    type Output = String;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = String> + 'g>, GraphError> {
        Ok(Arc::new(Name))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for Type {
    type Output = Option<ArcStr>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<ArcStr>> + 'g>, GraphError> {
        Ok(Arc::new(Type))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
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

impl NodeExpr for usize {
    type Output = usize;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = usize> + 'g>, GraphError> {
        Ok(Arc::new(Const(*self)))
    }

    fn prop_type(&self) -> PropType {
        PropType::U64
    }
}

impl NodeExpr for String {
    type Output = String;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = String> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.clone())))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for ArcStr {
    type Output = Option<ArcStr>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<ArcStr>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl NodeExpr for &'static str {
    type Output = &'static str;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = &'static str> + 'g>, GraphError> {
        Ok(Arc::new(Const(*self)))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IntoPropNodeExpr — normalises any RHS value to NodeExpr<Output = Option<Prop>>
//
// Used as the bound on Quantified::eq/ne/gt/ge/lt/le and NodeAggregated::eq/ne/…
// so that .eq("Alice"), .eq(30i64), and .eq(NodeFilter::property("x")) all work
// with a single method name.
// ─────────────────────────────────────────────────────────────────────────────

pub trait IntoPropNodeExpr {
    type Expr: NodeExpr<Output = Option<Prop>>;
    fn into_prop_node_expr(self) -> Self::Expr;
}

// Blanket: anything already NodeExpr<Output = Option<Prop>> passes through unchanged.
// Covers Prop, i64, u64, i32, u32, f32, f64, bool, u8, u16, Property, Metadata, etc.
impl<T: NodeExpr<Output = Option<Prop>>> IntoPropNodeExpr for T {
    type Expr = T;
    fn into_prop_node_expr(self) -> T {
        self
    }
}

// &'static str has Output = &'static str, so the blanket above does NOT cover it.
// Convert to Prop::Str so .eq("Alice") works transparently.
impl IntoPropNodeExpr for &'static str {
    type Expr = Prop;
    fn into_prop_node_expr(self) -> Prop {
        Prop::Str(ArcStr::from(self))
    }
}

impl IntoPropNodeExpr for String {
    type Expr = Prop;
    fn into_prop_node_expr(self) -> Prop {
        Prop::Str(ArcStr::from(self))
    }
}

impl NodeExpr for Prop {
    type Output = Option<Prop>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }

    fn prop_type(&self) -> PropType {
        self.dtype()
    }
}

macro_rules! impl_node_expr_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl NodeExpr for $prim {
            type Output = Option<Prop>;

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

impl<T: Comparable + Clone + Send + Sync + 'static> NodeExpr for ConstExpr<T> {
    type Output = T;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = T> + 'g>, GraphError> {
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

impl<E: CreateView + Clone + Send + Sync + 'static> NodeExpr for DegreeExpr<E> {
    type Output = usize;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = usize> + 'g>, GraphError> {
        Ok(Arc::new(Degree {
            dir: self.dir,
            view: self.view_expr.create_view(graph)?,
        }))
    }
}

impl NodeExpr for Property {
    type Output = Option<Prop>;

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

impl NodeExpr for Metadata {
    type Output = Option<Prop>;

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

impl<E: CreateView + Clone + Send + Sync + 'static> NodeExpr for TemporalPropertyExpr<E> {
    type Output = Prop;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Prop> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalNodePropOp { graph, prop_id }))
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
    ($expr:ident, $op_ty:ident, $output:ty) => {
        pub struct $expr<E: NodeExpr<Output = Prop>>(pub E);

        impl<E: NodeExpr<Output = Prop>> Clone for $expr<E> {
            fn clone(&self) -> Self {
                $expr(self.0.clone())
            }
        }

        impl<E: NodeExpr<Output = Prop>> NodeExpr for $expr<E> {
            type Output = $output;

            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = $output> + 'g>, GraphError> {
                let inner = self.0.create_node_op(graph)?;
                Ok(Arc::new($op_ty { inner }))
            }
        }
    };
}

impl_agg_expr!(SumExpr, SumNodeOp, Option<Prop>);
impl_agg_expr!(AvgExpr, AvgNodeOp, Option<Prop>);
impl_agg_expr!(MinExpr, MinNodeOp, Option<Prop>);
impl_agg_expr!(MaxExpr, MaxNodeOp, Option<Prop>);
impl_agg_expr!(FirstExpr, FirstNodeOp, Option<Prop>);
impl_agg_expr!(LastExpr, LastNodeOp, Option<Prop>);
impl_agg_expr!(LenExpr, LenNodeOp, usize);

// ─────────────────────────────────────────────────────────────────────────────
// UnwrapOptPropNodeExpr<E> — bridges Option<Prop> → Prop for nested aggregation
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// NestedMapExpr<E> — per-element aggregation / quantification on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Applies a per-element operation to each element of a `Prop::List` produced by `E`.
///
/// Used for chained expressions like `.temporal().any().sum()`:
/// `E` produces `Prop::List([list_t1, list_t2, …])` and each inner `list_ti` is
/// aggregated, yielding `Prop::List([result_t1, result_t2, …])` which is then
/// further quantified by `AnyNodeOp` / `AllNodeOp`.
#[derive(Clone)]
pub struct NestedMapExpr<E: NodeExpr<Output = Prop>> {
    pub inner: E,
    pub op: Op,
}

impl<E: NodeExpr<Output = Prop>> NodeExpr for NestedMapExpr<E> {
    type Output = Prop;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Prop> + 'g>, GraphError> {
        let inner = self.inner.create_node_op(graph)?;
        Ok(Arc::new(NestedMapNodeOp {
            inner,
            op: self.op,
        }))
    }
}

/// Bridges `E: NodeExpr<Output = Option<Prop>>` to `NodeExpr<Output = Prop>`,
/// enabling aggregator exprs (`SumExpr`, `AnyMode`, etc.) to operate on values
/// produced by a prior aggregation step.
///
/// Used when chaining e.g. `.temporal().last().sum()`:
/// `last()` produces `NodeAggregated<LastExpr<...>>` with `Output = Option<Prop>`;
/// `sum()` on that wraps in `SumExpr<UnwrapOptPropNodeExpr<LastExpr<...>>>`.
#[derive(Clone)]
pub struct UnwrapOptPropNodeExpr<E: NodeExpr<Output = Option<Prop>>>(pub E);

impl<E: NodeExpr<Output = Option<Prop>>> NodeExpr for UnwrapOptPropNodeExpr<E> {
    type Output = Prop;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Prop> + 'g>, GraphError> {
        let inner = self.0.create_node_op(graph)?;
        Ok(Arc::new(UnwrapOptPropOp { inner }))
    }
}
