use std::collections::HashSet;
use std::sync::Arc;

use raphtory_api::core::entities::properties::prop::PropType;
use raphtory_api::core::{Direction, entities::properties::prop::Prop};
use raphtory_core::entities::{VID};
use raphtory_storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps};
use crate::db::api::state::ops::GraphView;
use crate::db::api::state::ops::filter::NodeDegreeFilterOp;
use crate::db::graph::assertions::TestGraphVariants::Graph;
use crate::db::graph::node::NodeView;
use crate::db::graph::views::filter::CreateFilter;
use crate::db::graph::views::filter::model::{CompositeNodeFilter, NodeFilter};
use crate::db::graph::views::filter::model::property_filter::{Op, PropertyFilterInput, PropertyRef, PropertyFilter};
use crate::db::graph::views::filter::model::property_filter::builders::{PropertyExprBuilder, PropertyExprBuilderInput};
use crate::db::graph::views::filter::model::{CombinedFilter, EntityMarker, InternalPropertyFilterBuilder, TryAsCompositeFilter};
use crate::db::graph::views::filter::model;
use crate::db::graph::views::filter::node_filtered_graph::NodeFilteredGraph;
use crate::db::{api::view::{GraphViewOps, NodeViewOps}, graph::views::filter::model::{FilterOperator, property_filter::PropertyFilterValue}};
use crate::errors::GraphError;
use std::{fmt, fmt::Display};


#[derive(Clone)]
pub struct DegreeFilterBuilder {
    direction: Direction,
    error: Option<String>
}

impl DegreeFilterBuilder {
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            error: None
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DegreeFilter {
    pub direction: Direction,
    pub operator: FilterOperator,
    pub value: PropertyFilterValue,
    pub error: Option<String>
}


impl CreateFilter for DegreeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, NodeDegreeFilterOp<G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeDegreeFilterOp<G>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

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
        if self.error.is_some() {
            return Err(GraphError::InvalidFilter(self.error.unwrap()));
        }
        let value = match self.value {
            PropertyFilterValue::Single(ref prop_val) => {
                let casted_val = prop_val.clone().try_cast(PropType::U64).ok_or_else(|| {
                    GraphError::InvalidFilter(format!(
                        "degree filter expects an integer value, got {:?}", 
                        prop_val
                    ))
                })?;
                
                PropertyFilterValue::Single(casted_val)
            }
            PropertyFilterValue::Set(ref prop_vals) => {
                let casted_set = prop_vals
                    .iter()
                    .map(|val| {
                        val.clone().try_cast(PropType::U64).ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "degree filter expects an integer value, got {:?}", 
                                val
                            ))
                        })
                    })
                    .collect::<Result<HashSet<Prop>, GraphError>>()?;

                PropertyFilterValue::Set(Arc::new(casted_set))
            }
            PropertyFilterValue::None => {
                return Err(GraphError::InvalidFilter(
                    "degree filter requires a value".to_string()
                ));
            }
        }; 
        let mut filter = self.clone(); 
        filter.value = value;
        Ok(NodeDegreeFilterOp::new(graph, filter))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for DegreeFilter {
    fn try_as_composite_edge_filter(&self) -> Result<model::edge_filter::CompositeEdgeFilter, GraphError> {
         Err(GraphError::NotSupported)
    }
    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<model::CompositeExplodedEdgeFilter, GraphError>
    {
       Err(GraphError::NotSupported) 
    } 
    fn try_as_composite_node_filter(&self) -> Result<model::CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Degree(self.clone()))
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
    DegreeFilter: CombinedFilter
{
    type Filter = DegreeFilter;
    type ExprBuilder = DegreeFilterBuilder;
    type Marker = NodeFilter;

    fn property_ref(&self) -> PropertyRef {
        if let Some(ref error) = self.error {
            PropertyRef::Property(error.clone())
        } else {
            property_ref(&self.direction)
        }
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        let error = if filter.prop_ref.name().starts_with("Error") {
            Some(filter.prop_ref.name().to_string())
        } else {
            None
        };
        DegreeFilter {
             value: filter.prop_value,
             direction: self.direction,
             operator: filter.operator,
             error 
        }
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        let mut builder = self.clone(); 
        builder.error = Some("Error: DegreeFilter does not support expressions".to_string());
        builder
    }
}

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
            operator: self.operator.clone(),
            ops: vec![],
            entity: NodeFilter,
        };
        property_filter.fmt(f)
    }
} 
