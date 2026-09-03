use crate::{
    db::{
        api::state::ops::{filter::NodeDegreeFilterOp, GraphView},
        graph::views::filter::{
            model,
            model::{
                property_filter::{
                    builders::PropertyExprBuilderInput, Op, PropertyFilter, PropertyFilterInput,
                    PropertyFilterValue, PropertyRef,
                },
                CombinedFilter, ComposableFilter, CompositeNodeFilter, FilterOperator,
                InternalPropertyFilterBuilder, NodeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    Direction,
};
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};

#[derive(Clone)]
pub struct DegreeFilterBuilder {
    direction: Direction,
    ops: Vec<Op>,
}

impl DegreeFilterBuilder {
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            ops: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DegreeFilter {
    pub direction: Direction,
    pub operator: FilterOperator,
    pub value: PropertyFilterValue,
    pub ops: Vec<Op>,
}

impl CreateFilter for DegreeFilter {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        NodeFilteredGraph<G, NodeDegreeFilterOp<F>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NodeDegreeFilterOp<F>;

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
        let filter = self.create_node_filter(graph.clone(), filtered)?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        if !self.ops.is_empty() {
            return Err(GraphError::InvalidFilter(
                "degree filter does not support expressions".to_string(),
            ));
        }
        match self.operator {
            FilterOperator::Eq
            | FilterOperator::Ne
            | FilterOperator::Gt
            | FilterOperator::Ge
            | FilterOperator::Lt
            | FilterOperator::Le
            | FilterOperator::IsIn
            | FilterOperator::IsNotIn => {}
            _ => {
                return Err(GraphError::InvalidFilter(format!(
                    "degree filter does not support operator {:?}",
                    self.operator
                )));
            }
        }
        let value = match self.value {
            PropertyFilterValue::Single(ref prop_val) => {
                let casted_val = prop_val.clone().cast(PropType::U64).ok_or_else(|| {
                    GraphError::InvalidFilter(format!(
                        "degree filter expects an integer value, got {}",
                        prop_val
                    ))
                })?;

                PropertyFilterValue::Single(casted_val)
            }
            PropertyFilterValue::Set(ref prop_vals) => {
                let casted_set = prop_vals
                    .iter()
                    .map(|val| {
                        val.clone().cast(PropType::U64).ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "degree filter expects an integer value, got {}",
                                val
                            ))
                        })
                    })
                    .collect::<Result<HashSet<Prop>, GraphError>>()?;

                PropertyFilterValue::Set(Arc::new(casted_set))
            }
            PropertyFilterValue::None => {
                return Err(GraphError::InvalidFilter(
                    "degree filter requires a value".to_string(),
                ));
            }
        };
        let mut filter = self.clone();
        filter.value = value;
        Ok(NodeDegreeFilterOp::new(filtered, filter))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

fn property_ref(direction: &Direction) -> PropertyRef {
    match direction {
        Direction::IN => PropertyRef::Property("in_degree".to_string()),
        Direction::OUT => PropertyRef::Property("out_degree".to_string()),
        Direction::BOTH => PropertyRef::Property("degree".to_string()),
    }
}

impl InternalPropertyFilterBuilder for DegreeFilterBuilder
where
    DegreeFilter: CombinedFilter,
{
    type Filter = DegreeFilter;
    type ExprBuilder = DegreeFilterBuilder;
    type Marker = NodeFilter;

    fn property_ref(&self) -> PropertyRef {
        property_ref(&self.direction)
    }

    fn ops(&self) -> &[Op] {
        &self.ops
    }

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        DegreeFilter {
            value: filter.prop_value,
            direction: self.direction,
            operator: filter.operator,
            ops: filter.ops,
        }
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        let mut filter = self.clone();
        filter.ops = builder.ops;
        filter
    }
}

impl ComposableFilter for DegreeFilter {}

pub trait DegreeFilterFactory {
    fn in_degree(&self) -> DegreeFilterBuilder;
    fn out_degree(&self) -> DegreeFilterBuilder;
    fn degree(&self) -> DegreeFilterBuilder;
}

impl Display for DegreeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property_filter = PropertyFilter {
            prop_ref: property_ref(&self.direction),
            prop_value: self.value.clone(),
            operator: self.operator,
            ops: self.ops.clone(),
            entity: NodeFilter,
        };
        property_filter.fmt(f)
    }
}
