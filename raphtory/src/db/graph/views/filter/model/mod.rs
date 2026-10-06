pub use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::{EdgeEndpointWrapper, EdgeFilter},
                    exploded_edge_filter::ExplodedEdgeFilter,
                    filter_operator::{
                        BinaryOp, Comparable, FilterOperator, SetOp, StringComparable, StringOp,
                        UnaryOp,
                    },
                    node_expr::{
                        AvgExpr, BinaryCmpExpr, EntityAggOps, FirstExpr, IndexTerm, LastExpr,
                        LenExpr, MaxExpr, MinExpr, Predicate, PropValueSetExpr, StringExpr,
                        SumExpr, TemporalPropExpr, UnaryExpr,
                    },
                    node_filter::{NodeFilter, NodeFilterFactory},
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use crate::{
    db::{
        api::{
            state::NodeOp,
            view::{
                internal::{DynGraphArc, IntoDynGraphArc},
                BoxableGraphView,
            },
        },
        graph::views::{
            filter::{
                model::{
                    layered_filter::Layered,
                    node_expr::{NodeMetaOp, NodePropOp},
                },
                DynEdgeFilter,
            },
            layer_graph::LayeredGraph,
        },
    },
    prelude::LayerOps,
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
};
use std::{ops::Deref, sync::Arc};

pub mod and_filter;
pub mod answer;
pub mod edge_expr;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod expr;
pub mod filter_operator;
pub mod graph_filter;
pub mod is_active_edge_filter;
pub mod is_active_node_filter;
pub mod is_deleted_filter;
pub mod is_self_loop_filter;
pub mod is_valid_filter;
pub mod latest_filter;
pub mod layered_filter;
pub mod node_expr;
pub mod node_filter;
pub mod node_state_filter;
pub mod or_filter;
pub mod property_filter;
pub mod snapshot_filter;
pub mod subgraph_filter;
pub mod windowed_filter;

pub use expr::builder::{
    ComposableFilter, EdgeViewFilterOps, EntityExprFilterOps, PropertyExprFactory, ViewWrapOps,
};

pub trait DynCreateFilter: Send + Sync + 'static {
    fn create_dyn_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError>;

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError>;

    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynEdgeFilter<'graph>, GraphError>;
}

impl<T> DynCreateFilter for T
where
    T: CombinedFilter,
{
    fn create_dyn_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        Ok(self
            .clone()
            .create_graph_filter(graph)?
            .into_dyn_graph_arc())
    }

    fn create_dyn_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }

    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynEdgeFilter<'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_edge_filter(graph)?))
    }
}

impl<T: DynCreateFilter + ?Sized + 'static> CreateFilter for Arc<T> {
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
        self.deref()
            .create_dyn_graph_filter(graph.into_dyn_graph_arc())
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        self.deref()
            .create_dyn_node_filter(graph.into_dyn_graph_arc())
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.deref()
            .create_dyn_edge_filter(graph.into_dyn_graph_arc())
    }
}

#[derive(Copy, Clone)]
pub enum EntityMarker {
    Node,
    Edge,
    ExplodedEdge,
    Const,
}

#[derive(Clone)]
pub struct PropertyExpr<E> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
    pub(crate) entity: EntityMarker,
}

impl<E: CreateView> EntityExpr for PropertyExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }
}

#[derive(Clone)]
pub struct MetadataExpr<E> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
    pub(crate) entity: EntityMarker,
}

impl<E: CreateView> EntityExpr for MetadataExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }
}

impl<E: CreateView> CreateOp for PropertyExpr<E> {
    fn index_term(&self) -> Option<IndexTerm> {
        if self.view_expr.narrows() {
            return None;
        }
        Some(IndexTerm::Property {
            name: self.name.clone(),
            metadata: false,
            ever: false,
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
        Ok(Arc::new(NodePropOp {
            graph,
            prop_id,
            narrows: self.view_expr.narrows(),
        }))
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
        Ok(Arc::new(EdgePropOp { graph, prop_id }))
    }
}

impl<E: CreateView> CreateOp for MetadataExpr<E> {
    fn index_term(&self) -> Option<IndexTerm> {
        if self.view_expr.narrows() {
            return None;
        }
        Some(IndexTerm::Property {
            name: self.name.clone(),
            metadata: true,
            ever: false,
        })
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .node_meta()
            .get_prop_id(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(NodeMetaOp {
            graph,
            prop_id,
            narrows: self.view_expr.narrows(),
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(EdgeMetaOp { graph, prop_id }))
    }
}

impl<E: CreateView> PropertyExpr<E> {
    pub fn temporal(&self) -> TemporalPropExpr<E> {
        TemporalPropExpr {
            view_expr: self.view_expr.clone(),
            name: self.name.clone(),
            entity: self.entity,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropertyExpr<E> / MetadataExpr<E> — EdgeExpr impls
// ─────────────────────────────────────────────────────────────────────────────

use crate::db::graph::views::filter::model::{
    edge_expr::ops::{EdgeMetaOp, EdgePropOp},
    expr::Agg,
    node_expr::{CreateOp, EntityExpr},
};
use edge_expr::EdgeOp;
use raphtory_api::core::entities::properties::prop::PropType;

/// The window `at(t)` means: every event at the timestamp `t`, whatever its
/// position within that timestamp.
pub(crate) fn at_bounds(t: EventTime) -> (EventTime, EventTime) {
    (
        EventTime::start(t.t()),
        EventTime::start(t.t().saturating_add(1)),
    )
}

/// The window `after(t)` means: everything strictly after `t`.
pub(crate) fn after_bounds(t: EventTime) -> (EventTime, EventTime) {
    (
        EventTime::start(t.t().saturating_add(1)),
        EventTime::end(i64::MAX),
    )
}

/// The window `before(t)` means: everything strictly before `t`. Events at
/// the timestamp `t` itself are excluded, matching `GraphViewOps::before`.
pub(crate) fn before_bounds(t: EventTime) -> (EventTime, EventTime) {
    (EventTime::start(i64::MIN), EventTime::start(t.t()))
}

pub trait CreateView: Clone + Send + Sync + 'static {
    type View<'graph, G: GraphView + 'graph>: GraphView + 'graph;
    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError>;

    /// Whether the view can hide an entity the incoming graph shows. A term
    /// through a view that cannot is only ever asked about entities the
    /// enclosing filter has already found in that graph, so it need not check
    /// them again.
    fn narrows(&self) -> bool {
        true
    }
}

pub trait DynCreateView: Send + Sync + 'static {
    fn dyn_create_view<'graph>(
        &self,
        view: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;

    fn dyn_narrows(&self) -> bool;
}

impl<T: CreateView> DynCreateView for T {
    fn dyn_create_view<'graph>(
        &self,
        view: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(self.create_view(view)?.into_dyn_graph_arc())
    }

    fn dyn_narrows(&self) -> bool {
        self.narrows()
    }
}

impl<T: DynCreateView + ?Sized> CreateView for Arc<T> {
    type View<'graph, G: GraphView + 'graph> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        self.deref().dyn_create_view(view.into_dyn_graph_arc())
    }

    fn narrows(&self) -> bool {
        self.deref().dyn_narrows()
    }
}

impl CreateView for NodeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
    fn narrows(&self) -> bool {
        false
    }
}

impl CreateView for EdgeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
    fn narrows(&self) -> bool {
        false
    }
}

impl CreateView for ExplodedEdgeFilter {
    type View<'graph, G: GraphView + 'graph> = G;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        Ok(view)
    }
    fn narrows(&self) -> bool {
        false
    }
}

impl<T: CreateView> CreateView for Layered<T> {
    type View<'graph, G: GraphView + 'graph> = LayeredGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<LayeredGraph<T::View<'graph, G>>, GraphError> {
        let inner = self.inner.create_view(view)?;
        inner.layers(self.layer.clone())
    }
}

pub type DynFilter = Arc<dyn DynCreateFilter>;

/// Reject ordering operators on a type that has no ordering.
///
/// An unresolved type (`PropType::Empty`) passes; the check runs again once
/// the type is known.
pub fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    let ordering = matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    );
    if ordering && !prop_type.has_cmp() {
        let kind = match prop_type {
            PropType::List(_) => "list".to_string(),
            PropType::Map(_) => "map".to_string(),
            other => other.to_string(),
        };
        return Err(GraphError::InvalidFilter(format!(
            "operator {op} is not valid for {kind} properties"
        )));
    }
    Ok(())
}

/// A string operator applied to a value that is not a string.
pub fn not_a_string_error(prop_type: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!(
        "string operator requires a Str property, but the property type is {prop_type}"
    ))
}

/// Pick the more specific of the two known prop types.
///
/// Compiled `NodeOp`s and `EntityExpr`s may both have a known prop type, but
/// expression-level info (e.g. `DegreeExpr::prop_type()` → U64) is not always
/// propagated through generic wrappers like `Map<Op, V>`. Prefer whichever side
/// has a concrete type so validation can fire early.
pub fn resolved_prop_type(expr_pt: PropType, op_pt: PropType) -> PropType {
    if expr_pt != PropType::Empty {
        expr_pt
    } else {
        op_pt
    }
}

/// Reject a constant compared against an expression whose type it can never
/// equal.
///
/// Constants are never converted: a numeric constant compares by value with
/// any numeric expression (`degree() > 2.5` keeps the `.5`), a string constant
/// never compares with a number, and so on. A missing constant (`None`) and an
/// unresolved expression type both pass.
pub fn validate_const_comparable(
    lhs_pt: &PropType,
    value: Option<&Prop>,
) -> Result<(), GraphError> {
    match value {
        Some(v) if !lhs_pt.is_comparable_with(&v.dtype()) => Err(const_mismatch_error(v, lhs_pt)),
        _ => Ok(()),
    }
}

/// A constant compared with an expression whose type it can never equal.
pub fn const_mismatch_error(value: &Prop, expected: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!(
        "value {value} of type {} cannot be compared with {expected}",
        value.dtype()
    ))
}

/// Two expressions whose types can never be equal.
pub fn types_mismatch_error(lhs_pt: &PropType, rhs_pt: &PropType) -> GraphError {
    GraphError::InvalidFilter(format!("type mismatch: lhs is {lhs_pt}, rhs is {rhs_pt}"))
}

/// Reject an aggregate the expression's type cannot support.
///
/// Aggregates collapse a list, so a declared scalar type (`IsActiveNode` ->
/// `Bool`, `DegreeExpr` -> `U64`) is refused up front. `sum()`, `avg()`,
/// `min()` and `max()` also need numeric elements, as they always have: a
/// string or boolean history has no sum and no least value. An unresolved
/// type (`PropType::Empty`) passes and is checked again once known.
pub fn require_aggregable(pt: &PropType, agg: Agg, op: &str) -> Result<(), GraphError> {
    match pt {
        PropType::Empty => Ok(()),
        PropType::List(_) => {
            let elem = innermost_element(pt);
            if matches!(agg, Agg::Sum | Agg::Avg | Agg::Min | Agg::Max)
                && !(elem.is_numeric() || elem.is_unknown())
            {
                return Err(GraphError::InvalidFilter(format!(
                    "{op} requires numeric values, but the elements are {elem}"
                )));
            }
            Ok(())
        }
        _ => Err(GraphError::InvalidFilter(format!(
            "{op} is not valid on a scalar expression of type {pt}"
        ))),
    }
}

/// The element type under every list level of `pt`.
fn innermost_element(pt: &PropType) -> &PropType {
    match pt {
        PropType::List(inner) => innermost_element(inner),
        other => other,
    }
}

/// Narrow an `is_in`/`is_not_in` set to the members that could equal the LHS.
///
/// Set membership asks whether a value is present, so a member of a type the
/// LHS can never equal simply is not present: it is dropped rather than
/// rejected, leaving `is_in` answering "no" where a comparison would refuse
/// the question. The members that remain are kept exactly as written; the
/// runtime comparison handles mixed numeric widths by value. An unresolved
/// LHS type keeps every member.
pub fn comparable_set_values(lhs_pt: &PropType, values: Vec<Prop>) -> Vec<Prop> {
    values
        .into_iter()
        .filter(|v| lhs_pt.is_comparable_with(&v.dtype()))
        .collect()
}

pub trait CombinedFilter: CreateFilter + Clone + Send + Sync + 'static {}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CombinedFilter for T {}
