use std::collections::HashSet;
use std::sync::Arc;

use raphtory_api::core::entities::properties::prop::PropType;
use raphtory_api::core::{Direction, entities::properties::prop::Prop};
use raphtory_core::entities::{VID};
use crate::db::api::state::ops::GraphView;
use crate::db::api::state::ops::filter::NodeDegreeFilterOp;
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
}

impl DegreeFilterBuilder {
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DegreeFilter {
    pub direction: Direction,
    pub operator: FilterOperator,
    pub value: PropertyFilterValue
}

impl DegreeFilter {
    pub fn matches<'graph, G: GraphViewOps<'graph>>(&self, graph: &G, node: VID) -> bool {
        let node_view = graph.node(node).unwrap();
        let node_degree = match self.direction {
            Direction::IN => {
                node_view.in_degree()
            },          
            Direction::OUT => {
                node_view.out_degree()
            }, 
            Direction::BOTH => {
                node_view.degree()
            }
        };
        let node_degree_prop =  Prop::U64(node_degree as u64);
        self.operator.apply_to_property(&self.value, Some(&node_degree_prop))
    }
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
        Ok(NodeDegreeFilterOp::new(graph, self))
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
        property_ref(&self.direction)
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        NodeFilter
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        let value = match filter.prop_value {
           PropertyFilterValue::Single(ref prop_val) => PropertyFilterValue::Single(prop_val.clone().try_cast(PropType::U64).unwrap_or(prop_val.clone())),
           PropertyFilterValue::Set(ref prop_vals) => PropertyFilterValue::Set(Arc::new(prop_vals.iter().map(|val| val.clone().try_cast(PropType::U64).unwrap_or(val.clone())).collect::<HashSet<Prop>>())),
           PropertyFilterValue::None => PropertyFilterValue::None
        };  
        DegreeFilter {
             value,
             direction: self.direction,
             operator: filter.operator 
        }
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        panic!("DegreeFilter does not support expression builders");
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
