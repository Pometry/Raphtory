//! Node expressions — what value a node can produce.
//!
//! An expression is a pure data structure (no graph reference). It describes *what to compute*
//! without computing it. Call [`CreateOp::create_node_op`] to compile it against a specific graph
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

use super::{
    ops::{
        AvgNodeOp, FirstNodeOp, LastNodeOp, LenNodeOp, MaxNodeOp, MinNodeOp, SumNodeOp,
        TemporalNodePropOp, WithPropType,
    },
    AllEdgeOp, AllNodeOp, AnyEdgeOp, AnyNodeOp, AvgEdgeOp, CreateOp, EntityExpr, EntityExprBuilder,
    FirstEdgeOp, LastEdgeOp, LenEdgeOp, MaxEdgeOp, MinEdgeOp, SumEdgeOp,
};
use crate::{
    db::{
        api::{
            state::ops::{Const, Degree, Id, Name, NodeOp, Type},
            view::internal::GraphView,
        },
        graph::views::filter::model::{
            edge_expr::{ops::TemporalEdgePropOp, EdgeOp},
            elem_prop_type,
            filter_operator::{Comparable, ElemQual},
            node_filter::NodeFilter,
            require_aggregable, resolved_prop_type, CreateView, EntityMarker,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{IntoProp, Prop, PropType},
        GidType, GID,
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
// All map their native types into Option<Prop> via into_prop():
//   NodeFilter.id()        uses Id   — produces Option<Prop> (GID mapped to Prop)
//   NodeFilter.name()      uses Name — produces Option<Prop> (String as Prop::Str)
//   NodeFilter.node_type() uses Type — produces Option<Prop> (ArcStr as Prop::Str, None if unset)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Copy, Clone, Debug, Default)]
pub struct ConstFilter;

impl From<ConstFilter> for EntityMarker {
    fn from(_value: ConstFilter) -> Self {
        EntityMarker::Const
    }
}

impl EntityExpr for Id {
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }
}

impl EntityExprBuilder for Id {}

impl CreateOp for Id {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let pt = match graph.id_type() {
            Some(GidType::Str) => PropType::Str,
            Some(GidType::U64) => PropType::U64,
            None => PropType::Empty,
        };
        Ok(Arc::new(WithPropType {
            inner: Id.map(|a| Some(a.into_prop())),
            pt,
        }))
    }
}

impl EntityExpr for GID {
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }
}

impl CreateOp for GID {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for Name {
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl EntityExprBuilder for Name {}

impl CreateOp for Name {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(WithPropType {
            inner: Name.map(|a| Some(a.into_prop())),
            pt: PropType::Str,
        }))
    }
}

impl EntityExpr for Type {
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl EntityExprBuilder for Type {}

impl CreateOp for Type {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        // Untyped nodes carry the storage's default type key, matching how the
        // composite path builds its type mask over the node-type meta keys.
        Ok(Arc::new(WithPropType {
            inner: Type.map(|a| Some(a.map_or_else(|| Prop::str("_default"), |b| b.into_prop()))),
            pt: PropType::Str,
        }))
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
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::U64
    }
}

impl CreateOp for usize {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::U64(*self as u64)))))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::U64(*self as u64)))))
    }
}

impl EntityExpr for String {
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl CreateOp for String {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for ArcStr {
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl CreateOp for ArcStr {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone().into_prop()))))
    }
}

impl EntityExpr for &'static str {
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl CreateOp for &'static str {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some((*self).into_prop()))))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::Str(ArcStr::from(*self))))))
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
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }

    fn prop_type(&self) -> PropType {
        self.dtype()
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

macro_rules! impl_create_op_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl EntityExpr for $prim {
            type Marker = ConstFilter;
            fn entity(&self) -> Self::Marker {
                ConstFilter
            }
            fn prop_type(&self) -> PropType {
                PropType::$variant
            }
        }

        impl CreateOp for $prim {
            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                _graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
                Ok(Arc::new(Const(Some(Prop::$variant(*self)))))
            }
            fn create_edge_op<'g, G: GraphView + 'g>(
                &self,
                _graph: G,
            ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
                Ok(Arc::new(Const(Some(Prop::$variant(*self)))))
            }
        }
    };
}

impl_create_op_for_numeric!(i32, I32);
impl_create_op_for_numeric!(i64, I64);
impl_create_op_for_numeric!(u32, U32);
impl_create_op_for_numeric!(u64, U64);
impl_create_op_for_numeric!(f32, F32);
impl_create_op_for_numeric!(f64, F64);
impl_create_op_for_numeric!(bool, Bool);
impl_create_op_for_numeric!(u8, U8);
impl_create_op_for_numeric!(u16, U16);

/// A constant expression for custom output types not covered by the built-in impls.
///
/// Built-in types (`usize`, `String`, `Prop`, numerics, `&'static str`) implement
/// [`CreateOp`] directly and can be passed as-is. `ConstExpr<T>` is only needed
/// for custom comparable types.
///
/// ```rust,ignore
/// some_expr.gt(ConstExpr(my_custom_value))
/// ```
#[derive(Clone)]
pub struct ConstExpr<T>(pub T);

impl<T: Comparable + Clone + Send + Sync + 'static> EntityExpr for ConstExpr<T> {
    type Marker = ConstFilter;

    fn entity(&self) -> Self::Marker {
        ConstFilter
    }
}

impl<T: Comparable + Into<Prop> + Clone + Send + Sync + 'static> CreateOp for ConstExpr<T> {
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
    type Marker = NodeFilter;

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn prop_type(&self) -> PropType {
        PropType::U64
    }
    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateView + Clone + Send + Sync + 'static> EntityExprBuilder for DegreeExpr<E> {}

impl<E: CreateView + Clone + Send + Sync + 'static> CreateOp for DegreeExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(WithPropType {
            inner: Degree {
                dir: self.dir,
                view: self.view_expr.create_view(graph)?,
            }
            .map(|a| Some(Prop::U64(a as u64))),
            pt: PropType::U64,
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalExpr<E> — all temporal values of a property over the view window
//
// Unified replacement for TemporalPropertyExpr (node) and TemporalEdgePropExpr (edge).
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
}

impl<E: EntityExpr + Clone + Send + Sync + 'static> EntityExpr for TemporalPropExpr<E> {
    type Marker = E::Marker;
    fn entity(&self) -> Self::Marker {
        self.view_expr.entity()
    }
}

impl<E: EntityExpr + Clone + Send + Sync + 'static> EntityExprBuilder for TemporalPropExpr<E> {}

impl<E: EntityExpr + Clone + Send + Sync + 'static> EntityAggOps for TemporalPropExpr<E> {
    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }
    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }
    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }
    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }
    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }
    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }
    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}

impl<E: EntityExpr + CreateView + Clone + Send + Sync + 'static> CreateOp for TemporalPropExpr<E> {
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
            TemporalNodePropOp { graph, prop_id }.map(|a| Some(a)),
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
// Each wraps an inner NodeExpr (typically TemporalPropertyExpr) and reduces
// the Prop::List it produces.  Not constructed directly —
// EntityAggOps methods on TemporalExpr return these exprs directly:
//
//   .property("v").temporal().sum()  → SumExpr<TemporalPropertyExpr<..>>
//   .property("v").temporal().len()  → LenExpr<TemporalPropertyExpr<..>>
//   .property("v").temporal().any()  → AnyExpr<TemporalPropertyExpr<..>>
//
// Calling .gt() / .eq() etc. on any of these (via NodeExprFilterOps) produces:
//   BinaryCmpExpr<SumExpr<TemporalPropertyExpr<..>>, RHS>
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// EntityAggOps — secondary aggregate operators on filter expression types
//
// Scoped narrowly (not blanket-impl) to avoid name collisions with stdlib methods
// like `Ord::min` / `Ord::max` / `Iterator::sum` on primitive `EntityExpr` types
// (`u64`, `i64`, etc. all implement `EntityExpr` as constant values).
// ─────────────────────────────────────────────────────────────────────────────

pub trait EntityAggOps: EntityExpr + Sized {
    fn sum(self) -> SumExpr<Self>;
    fn avg(self) -> AvgExpr<Self>;
    fn min(self) -> MinExpr<Self>;
    fn max(self) -> MaxExpr<Self>;
    fn first(self) -> FirstExpr<Self>;
    fn last(self) -> LastExpr<Self>;
    fn len(self) -> LenExpr<Self>;
}

macro_rules! impl_agg_expr {
    ($expr:ident, $node_op_ty:ident, $edge_op_ty:ident, $name:literal, $qual:expr) => {
        impl_agg_expr!(@common $expr, $node_op_ty, $edge_op_ty);

        impl<E: CreateOp> CreateOp for $expr<E> {
            impl_agg_expr!(@create $node_op_ty, $edge_op_ty, $name);

            fn create_qualified_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>
            {
                let (inner, mut quals) = self.0.create_qualified_node_op(graph)?;
                quals.push($qual);
                Ok((inner, quals))
            }

            fn create_qualified_edge_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>
            {
                let (inner, mut quals) = self.0.create_qualified_edge_op(graph)?;
                quals.push($qual);
                Ok((inner, quals))
            }
        }
    };
    ($expr:ident, $node_op_ty:ident, $edge_op_ty:ident, $name:literal) => {
        impl_agg_expr!(@common $expr, $node_op_ty, $edge_op_ty);

        impl<E: CreateOp> CreateOp for $expr<E> {
            impl_agg_expr!(@create $node_op_ty, $edge_op_ty, $name);

            // Leading qualifiers float through aggregates: the aggregate
            // applies per element (aggregate_list_values recurses into
            // nested lists) and the qualifiers collapse afterwards.
            fn create_qualified_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>
            {
                let (inner, quals) = self.0.create_qualified_node_op(graph)?;
                let pt = resolved_prop_type(self.0.prop_type(), inner.prop_type());
                require_aggregable(&elem_prop_type(&pt, quals.len())?, $name)?;
                Ok((Arc::new($node_op_ty { inner }), quals))
            }

            fn create_qualified_edge_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>
            {
                let (inner, quals) = self.0.create_qualified_edge_op(graph)?;
                let pt = resolved_prop_type(self.0.prop_type(), inner.prop_type());
                require_aggregable(&elem_prop_type(&pt, quals.len())?, $name)?;
                Ok((Arc::new($edge_op_ty { inner }), quals))
            }
        }
    };
    (@common $expr:ident, $node_op_ty:ident, $edge_op_ty:ident) => {
        #[derive(Clone)]
        pub struct $expr<E>(pub E);

        impl<E: EntityExpr> EntityExpr for $expr<E> {
            type Marker = E::Marker;
            fn entity(&self) -> Self::Marker {
                self.0.entity()
            }
        }

        impl<E: EntityExpr> EntityExprBuilder for $expr<E> {}

        impl<E: EntityExpr> EntityAggOps for $expr<E> {
            fn sum(self) -> SumExpr<Self> {
                SumExpr(self)
            }
            fn avg(self) -> AvgExpr<Self> {
                AvgExpr(self)
            }
            fn min(self) -> MinExpr<Self> {
                MinExpr(self)
            }
            fn max(self) -> MaxExpr<Self> {
                MaxExpr(self)
            }
            fn first(self) -> FirstExpr<Self> {
                FirstExpr(self)
            }
            fn last(self) -> LastExpr<Self> {
                LastExpr(self)
            }
            fn len(self) -> LenExpr<Self> {
                LenExpr(self)
            }
        }

    };
    (@create $node_op_ty:ident, $edge_op_ty:ident, $name:literal) => {
        fn create_node_op<'g, G: GraphView + 'g>(
            &self,
            graph: G,
        ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
            let inner = self.0.create_node_op(graph)?;
            let pt = resolved_prop_type(self.0.prop_type(), inner.prop_type());
            require_aggregable(&pt, $name)?;
            Ok(Arc::new($node_op_ty { inner }))
        }

        fn create_edge_op<'g, G: GraphView + 'g>(
            &self,
            graph: G,
        ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
            let inner = self.0.create_edge_op(graph)?;
            let pt = resolved_prop_type(self.0.prop_type(), inner.prop_type());
            require_aggregable(&pt, $name)?;
            Ok(Arc::new($edge_op_ty { inner }))
        }
    };
}

impl_agg_expr!(SumExpr, SumNodeOp, SumEdgeOp, "sum()");
impl_agg_expr!(AvgExpr, AvgNodeOp, AvgEdgeOp, "avg()");
impl_agg_expr!(MinExpr, MinNodeOp, MinEdgeOp, "min()");
impl_agg_expr!(MaxExpr, MaxNodeOp, MaxEdgeOp, "max()");
impl_agg_expr!(FirstExpr, FirstNodeOp, FirstEdgeOp, "first()");
impl_agg_expr!(LastExpr, LastNodeOp, LastEdgeOp, "last()");
impl_agg_expr!(LenExpr, LenNodeOp, LenEdgeOp, "len()");
impl_agg_expr!(AnyExpr, AnyNodeOp, AnyEdgeOp, "any()", ElemQual::Any);
impl_agg_expr!(AllExpr, AllNodeOp, AllEdgeOp, "all()", ElemQual::All);
