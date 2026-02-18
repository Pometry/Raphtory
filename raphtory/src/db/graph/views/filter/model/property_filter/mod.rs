use crate::{
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, Metadata, Properties},
            state::ops::{filter::NodePropertyFilterOp, NotANodeFilter},
            view::internal::{GraphView, NodeTimeSemanticsOps},
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::filter::{
                edge_property_filtered_graph::EdgePropertyFilteredGraph,
                exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph,
                model::{
                    edge_filter::CompositeEdgeFilter, ComposableFilter,
                    CompositeExplodedEdgeFilter, CompositeNodeFilter, ExplodedEdgeFilter,
                    FilterOperator, TryAsCompositeFilter,
                },
                node_filtered_graph::NodeFilteredGraph,
                CreateFilter,
            },
        },
    },
    errors::GraphError,
    prelude::{
        EdgeFilter, EdgeViewOps, GraphViewOps, LayerOps, NodeFilter, NodeViewOps, PropertiesOps,
    },
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        properties::{
            meta::Meta,
            prop::{sort_comparable_props, Prop, PropType},
        },
        EID,
    },
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeEntryRef, edge_storage_ops::EdgeStorageOps},
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};

pub mod builders;
mod evaluate;
pub mod ops;
mod validate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    // Selectors
    First,
    Last,
    // Aggregators
    Len,
    Sum,
    Avg,
    Min,
    Max,
    // Qualifiers (quantifiers)
    Any,
    All,
}

impl Op {
    #[inline]
    pub fn is_selector(self) -> bool {
        matches!(self, Op::First | Op::Last)
    }

    #[inline]
    pub fn is_aggregator(self) -> bool {
        matches!(self, Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max)
    }

    #[inline]
    pub fn is_qualifier(self) -> bool {
        matches!(self, Op::Any | Op::All)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRef {
    Property(String),
    Metadata(String),
    TemporalProperty(String),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name) => write!(f, "TemporalProperty({})", name),
            PropertyRef::Metadata(name) => write!(f, "Metadata({})", name),
            PropertyRef::Property(name) => write!(f, "Property({})", name),
        }
    }
}

impl PropertyRef {
    pub fn name(&self) -> &str {
        match self {
            PropertyRef::Property(name)
            | PropertyRef::Metadata(name)
            | PropertyRef::TemporalProperty(name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

pub struct PropertyFilterInput {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
    pub ops: Vec<Op>, // validated by validate_chain_and_infer_effective_dtype
}

impl PropertyFilterInput {
    pub fn with_entity<M>(self, entity: M) -> PropertyFilter<M> {
        PropertyFilter {
            prop_ref: self.prop_ref,
            prop_value: self.prop_value,
            operator: self.operator,
            ops: self.ops,
            entity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyFilter<M> {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
    pub ops: Vec<Op>, // validated by validate_chain_and_infer_effective_dtype
    pub entity: M,
}

impl<M> Display for PropertyFilter<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut expr = match &self.prop_ref {
            PropertyRef::Property(name) => name.to_string(),
            PropertyRef::Metadata(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name) => format!("temporal({})", name),
        };

        for op in &self.ops {
            expr = match op {
                Op::First => format!("first({})", expr),
                Op::Last => format!("last({})", expr),
                Op::Len => format!("len({})", expr),
                Op::Sum => format!("sum({})", expr),
                Op::Avg => format!("avg({})", expr),
                Op::Min => format!("min({})", expr),
                Op::Max => format!("max({})", expr),
                Op::Any => format!("any({})", expr),
                Op::All => format!("all({})", expr),
            };
        }

        match &self.prop_value {
            PropertyFilterValue::None => write!(f, "{} {}", expr, self.operator),
            PropertyFilterValue::Single(value) => write!(f, "{} {} {}", expr, self.operator, value),
            PropertyFilterValue::Set(values) => {
                let sorted = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "{} {} [{}]", expr, self.operator, values_str)
            }
        }
    }
}

impl<M> PropertyFilter<M> {
    #[inline]
    pub fn with_op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    #[inline]
    pub fn with_ops(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.ops.extend(ops);
        self
    }

    pub fn resolve_prop_id(&self, meta: &Meta, expect_map: bool) -> Result<usize, GraphError> {
        let (name, is_static, is_temporal) = match &self.prop_ref {
            PropertyRef::Metadata(n) => (n.as_str(), true, false),
            PropertyRef::Property(n) => (n.as_str(), false, false),
            PropertyRef::TemporalProperty(n) => (n.as_str(), false, true),
        };

        let (id, original_dtype) = match meta.get_prop_id_and_type(name, is_static) {
            None => {
                return if is_static {
                    Err(GraphError::MetadataMissingError(name.to_string()))
                } else {
                    Err(GraphError::PropertyMissingError(name.to_string()))
                }
            }
            Some((id, dtype)) => (id, dtype),
        };

        let is_original_map = matches!(original_dtype, PropType::Map(..));
        let rhs_is_map = matches!(self.prop_value, PropertyFilterValue::Single(Prop::Map(_)));
        let expect_map_now = if is_static && rhs_is_map {
            true
        } else {
            is_static && expect_map && is_original_map
        };

        let eff = self.validate_chain_and_infer_effective_dtype(&original_dtype, is_temporal)?;

        self.validate_operator_against_dtype(&eff, expect_map_now)?;

        Ok(id)
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        if self.ops.is_empty() {
            return self.operator.apply_to_property(&self.prop_value, other);
        }
        self.eval_scalar_and_apply(other.cloned())
    }

    fn is_property_matched<I: InternalPropertiesOps + Clone>(
        &self,
        t_prop_id: usize,
        props: Properties<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Property(_) => {
                let prop = props.get_by_id(t_prop_id);
                self.matches(prop.as_ref())
            }
            PropertyRef::Metadata(_) => false,
            PropertyRef::TemporalProperty(_) => {
                let Some(tview) = props.temporal().get_by_id(t_prop_id) else {
                    return false;
                };
                let seq = tview.values().collect();
                self.eval_temporal_and_apply(seq)
            }
        }
    }

    fn is_metadata_matched<I: InternalPropertiesOps + Clone>(
        &self,
        c_prop_id: usize,
        props: Metadata<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let prop_value = props.get_by_id(c_prop_id);
                self.matches(prop_value.as_ref())
            }
            _ => false,
        }
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: usize,
        node: NodeStorageRef,
    ) -> bool {
        let node_view = NodeView::new_internal(graph, node.vid());

        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = node_view.metadata();
                self.is_metadata_matched(prop_id, props)
            }

            PropertyRef::Property(_) => {
                let props = node_view.properties();
                self.is_property_matched(prop_id, props)
            }

            PropertyRef::TemporalProperty(_) => {
                let props = node_view.properties();
                let Some(tview) = props.temporal().get_by_id(prop_id) else {
                    return false;
                };

                let seq = tview.values().collect();

                if self.ops.is_empty() {
                    return self.eval_temporal_and_apply(seq);
                }

                // If the FIRST quantifier is a temporal quantifier and it's ALL,
                // require the temporal property to be present on EVERY node update time.
                if self
                    .ops
                    .iter()
                    .copied()
                    .find(|op| matches!(op, Op::Any | Op::All))
                    == Some(Op::All)
                {
                    let semantics = graph.node_time_semantics();
                    let core_node = graph.core_node(node_view.node);

                    let node_update_count =
                        semantics.node_updates(core_node.as_ref(), graph).count();
                    let prop_time_count = seq.len();

                    // Missing at any timepoint? Leading temporal ALL must fail.
                    if prop_time_count < node_update_count {
                        return false;
                    }
                }

                self.eval_temporal_and_apply(seq)
            }
        }
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: usize,
        edge: EdgeEntryRef,
    ) -> bool {
        let edge = EdgeView::new(graph, edge.out_ref());
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = edge.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_) => {
                let seq: Vec<Prop> = edge
                    .properties()
                    .temporal()
                    .get_by_id(prop_id)
                    .map(|tv| tv.values().collect())
                    .unwrap_or_default();
                self.eval_temporal_and_apply(seq)
            }
            PropertyRef::Property(_) => {
                let props = edge.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }

    pub fn matches_exploded_edge<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
        e: EID,
        t: EventTime,
        layer: usize,
    ) -> bool {
        let edge = EdgeView::new(graph, graph.core_edge(e).out_ref().at(t, layer));
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = edge.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_) | PropertyRef::Property(_) => {
                let props = edge.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }
}

impl CreateFilter for PropertyFilter<NodeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, NodePropertyFilterOp<G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodePropertyFilterOp<G>;

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
        let prop_id = self.resolve_prop_id(graph.node_meta(), false)?;
        Ok(NodePropertyFilterOp::new(graph, prop_id, self))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl CreateFilter for PropertyFilter<EdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = EdgePropertyFilteredGraph<G>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        Ok(EdgePropertyFilteredGraph::new(graph, prop_id, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl CreateFilter for PropertyFilter<ExplodedEdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = ExplodedEdgePropertyFilteredGraph<G>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        Ok(ExplodedEdgePropertyFilteredGraph::new(graph, prop_id, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<M> ComposableFilter for PropertyFilter<M> {}

impl TryAsCompositeFilter for PropertyFilter<NodeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Property(self.clone()))
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

impl TryAsCompositeFilter for PropertyFilter<EdgeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Property(self.clone()))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl TryAsCompositeFilter for PropertyFilter<ExplodedEdgeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Property(self.clone()))
    }
}
