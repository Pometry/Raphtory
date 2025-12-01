use crate::{
    db::{
        api::{
            state::{
                ops::{
                    filter::{
                        AndOp, MaskOp, NodeIdFilterOp, NodeNameFilterOp, NodeTypeFilterOp, NotOp,
                        OrOp,
                    },
                    TypeId,
                },
                NodeOp,
            },
            view::{internal::GraphView, BoxableGraphView},
        },
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                edge_filter::CompositeEdgeFilter,
                filter::{Filter, FilterValue},
                node_filter::builders::{
                    NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder,
                },
                property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
                windowed_filter::Windowed,
                ComposableFilter, CompositeExplodedEdgeFilter, EntityMarker,
                InternalPropertyFilterFactory, TryAsCompositeFilter, Wrap,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::entities::GidType;
use raphtory_core::utils::time::IntoTime;
use raphtory_storage::core_ops::CoreGraphOps;
use std::{fmt, fmt::Display, sync::Arc};

pub mod builders;
pub mod ops;

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl NodeFilter {
    pub fn id() -> NodeIdFilterBuilder {
        NodeIdFilterBuilder
    }

    pub fn name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }

    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<NodeFilter> {
        Windowed::from_times(start, end, NodeFilter)
    }

    pub fn validate(id_dtype: Option<GidType>, filter: &Filter) -> Result<(), GraphError> {
        use crate::db::graph::views::filter::model::FilterOperator::*;
        use raphtory_api::core::entities::{GidType::*, GID};

        let Some(kind) = id_dtype else {
            return Ok(());
        };

        fn filter_value_kind(fv: &FilterValue) -> &'static str {
            match fv {
                FilterValue::ID(GID::U64(_)) => "U64",
                FilterValue::ID(GID::Str(_)) => "Str",
                FilterValue::IDSet(set) => {
                    if set.iter().all(|g| matches!(g, GID::U64(_))) {
                        "U64"
                    } else if set.iter().all(|g| matches!(g, GID::Str(_))) {
                        "Str"
                    } else {
                        "heterogeneous id set"
                    }
                }
                FilterValue::Single(_) => "Str",
                FilterValue::Set(_) => "Str",
            }
        }

        let value_matches_kind = |fv: &FilterValue, expect: GidType| -> bool {
            match (fv, expect) {
                (FilterValue::ID(GID::U64(_)), U64) => true,
                (FilterValue::IDSet(set), U64) => set.iter().all(|g| matches!(g, GID::U64(_))),

                (FilterValue::ID(GID::Str(_)), Str) => true,
                (FilterValue::IDSet(set), Str) => set.iter().all(|g| matches!(g, GID::Str(_))),
                (FilterValue::Single(_), Str) => true,
                (FilterValue::Set(_), Str) => true,

                _ => false,
            }
        };

        let op_allowed = match kind {
            U64 => matches!(
                filter.operator,
                Eq | Ne | Lt | Le | Gt | Ge | IsIn | IsNotIn
            ),
            Str => matches!(
                filter.operator,
                Eq | Ne
                    | StartsWith
                    | EndsWith
                    | Contains
                    | NotContains
                    | FuzzySearch { .. }
                    | IsIn
                    | IsNotIn
            ),
        };

        if !op_allowed {
            return Err(GraphError::InvalidGqlFilter(format!(
                "Operator {} not allowed for {:?} ID",
                filter.operator, kind
            )));
        }

        if !value_matches_kind(&filter.field_value, kind) {
            return Err(GraphError::InvalidGqlFilter(format!(
                "Filter value type does not match node ID type. Expected {:?} but got {:?}",
                kind,
                filter_value_kind(&filter.field_value)
            )));
        }

        match filter.operator {
            IsIn | IsNotIn => {
                if !matches!(
                    filter.field_value,
                    FilterValue::IDSet(_) | FilterValue::Set(_)
                ) {
                    return Err(GraphError::InvalidGqlFilter(
                        "IN/NOT_IN on ID expects a set of IDs".into(),
                    ));
                }
            }
            StartsWith | EndsWith | Contains | NotContains | FuzzySearch { .. } => {
                if !matches!(
                    filter.field_value,
                    FilterValue::ID(GID::Str(_)) | FilterValue::Single(_)
                ) {
                    return Err(GraphError::InvalidGqlFilter(
                        "String operators on ID expect a single string ID".into(),
                    ));
                }
            }
            Lt | Le | Gt | Ge => {
                if !matches!(filter.field_value, FilterValue::ID(GID::U64(_))) {
                    return Err(GraphError::InvalidGqlFilter(
                        "Numeric operators on ID expect a single numeric (u64) ID".into(),
                    ));
                }
            }
            IsSome | IsNone => {
                return Err(GraphError::InvalidGqlFilter(
                    "IsSome/IsNone are not supported as filter operators".into(),
                ));
            }
            Eq | Ne => {
                // Eq/Ne already type-checked above
            }
        }

        Ok(())
    }
}

impl Wrap for NodeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl EntityMarker for NodeFilter {}

impl InternalPropertyFilterFactory for NodeFilter {
    type Entity = NodeFilter;
    type PropertyBuilder = PropertyFilterBuilder<NodeFilter>;
    type MetadataBuilder = MetadataFilterBuilder<NodeFilter>;

    fn entity(&self) -> Self::Entity {
        NodeFilter
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        builder
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        builder
    }
}

#[derive(Debug, Clone)]
pub struct NodeIdFilter(pub Filter);

impl Display for NodeIdFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeIdFilter {
    fn from(filter: Filter) -> Self {
        NodeIdFilter(filter)
    }
}

impl CreateFilter for NodeIdFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeIdFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeIdFilterOp;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        NodeFilter::validate(graph.id_type(), &self.0)?;
        Ok(NodeFilteredGraph::new(graph, NodeIdFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        NodeFilter::validate(graph.id_type(), &self.0)?;
        Ok(NodeIdFilterOp::new(self.0))
    }
}

impl TryAsCompositeFilter for NodeIdFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

#[derive(Debug, Clone)]
pub struct NodeNameFilter(pub Filter);

impl Display for NodeNameFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeNameFilter {
    fn from(filter: Filter) -> Self {
        NodeNameFilter(filter)
    }
}

impl ComposableFilter for NodeNameFilter {}

impl CreateFilter for NodeNameFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeNameFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeNameFilterOp;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        Ok(NodeFilteredGraph::new(graph, NodeNameFilterOp::new(self.0)))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeNameFilterOp::new(self.0))
    }
}

impl TryAsCompositeFilter for NodeNameFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

#[derive(Debug, Clone)]
pub struct NodeTypeFilter(pub Filter);

impl Display for NodeTypeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeTypeFilter {
    fn from(filter: Filter) -> Self {
        NodeTypeFilter(filter)
    }
}

impl ComposableFilter for NodeTypeFilter {}

impl CreateFilter for NodeTypeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, NodeTypeFilterOp>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeTypeFilterOp;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .get_keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(NodeFilteredGraph::new(
            graph,
            TypeId.mask(node_types_filter.into()),
        ))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let node_types_filter = graph
            .node_meta()
            .node_type_meta()
            .get_keys()
            .iter()
            .map(|k| self.0.matches(Some(k))) // TODO: _default check
            .collect::<Vec<_>>();
        Ok(TypeId.mask(node_types_filter.into()))
    }
}

impl TryAsCompositeFilter for NodeTypeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter<NodeFilter>),
    Windowed(Box<Windowed<CompositeNodeFilter>>),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Not(Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Windowed(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
        }
    }
}

impl CreateFilter for CompositeNodeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, Self::NodeFilter<'graph, G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = Arc<dyn NodeOp<Output = bool> + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        match self {
            CompositeNodeFilter::Node(i) => match i.field_name.as_str() {
                "node_id" => Ok(Arc::new(NodeIdFilter(i).create_node_filter(graph)?)),
                "node_name" => Ok(Arc::new(NodeNameFilter(i).create_node_filter(graph)?)),
                "node_type" => Ok(Arc::new(NodeTypeFilter(i).create_node_filter(graph)?)),
                _ => {
                    unreachable!()
                }
            },
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.create_node_filter(graph)?)),
            CompositeNodeFilter::Windowed(i) => {
                let dyn_graph: Arc<dyn BoxableGraphView + 'graph> = Arc::new(graph);
                i.create_node_filter(dyn_graph)
            }
            CompositeNodeFilter::And(l, r) => Ok(Arc::new(AndOp {
                left: l.clone().create_node_filter(graph.clone())?,
                right: r.clone().create_node_filter(graph.clone())?,
            })),
            CompositeNodeFilter::Or(l, r) => Ok(Arc::new(OrOp {
                left: l.clone().create_node_filter(graph.clone())?,
                right: r.clone().create_node_filter(graph.clone())?,
            })),
            CompositeNodeFilter::Not(filter) => {
                Ok(Arc::new(NotOp(filter.clone().create_node_filter(graph)?)))
            }
        }
    }
}

impl TryAsCompositeFilter for CompositeNodeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.clone())
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
