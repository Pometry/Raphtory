use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::nodes::node_ref::NodeStorageRef,
            view::{
                internal::{
                    Immutable, InheritCoreOps, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                    InheritStorageOps, InheritTimeSemantics, NodeFilterOps, Static,
                },
                Base,
            },
        },
        graph::views::filter::{internal::InternalNodeFilterOps, CompositeNodeFilter, Filter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{properties::props::Meta, LayerIds};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct ResolvedCompositeNodeFilter {
    pub filter: CompositeNodeFilter,
    pub t_prop_ids: HashMap<String, usize>,
    pub c_prop_ids: HashMap<String, usize>,
    pub node_types_filter: Arc<[bool]>,
}

impl ResolvedCompositeNodeFilter {
    pub fn resolve(filter: CompositeNodeFilter, meta: &Meta) -> Result<Self, GraphError> {
        let mut t_prop_ids = HashMap::new();
        let mut c_prop_ids = HashMap::new();

        fn traverse(
            filter: &CompositeNodeFilter,
            meta: &Meta,
            t_prop_ids: &mut HashMap<String, usize>,
            c_prop_ids: &mut HashMap<String, usize>,
            node_filters: &mut Vec<Filter>,
        ) -> Result<(), GraphError> {
            match filter {
                CompositeNodeFilter::Property(pf) => {
                    let prop_name = pf.prop_ref.name().to_string();

                    if !t_prop_ids.contains_key(&prop_name) {
                        if let Some(id) = pf.resolve_temporal_prop_id(meta)? {
                            t_prop_ids.insert(prop_name.clone(), id);
                        }
                    }

                    if !c_prop_ids.contains_key(&prop_name) {
                        if let Some(id) = pf.resolve_constant_prop_id(meta)? {
                            c_prop_ids.insert(prop_name.clone(), id);
                        }
                    }
                }
                CompositeNodeFilter::And(l, r) | CompositeNodeFilter::Or(l, r) => {
                    traverse(l, meta, t_prop_ids, c_prop_ids, node_filters)?;
                    traverse(r, meta, t_prop_ids, c_prop_ids, node_filters)?;
                }
                CompositeNodeFilter::Node(nf) => {
                    node_filters.push(nf.clone());
                }
            }
            Ok(())
        }

        let mut node_filters = Vec::new();
        traverse(
            &filter,
            meta,
            &mut t_prop_ids,
            &mut c_prop_ids,
            &mut node_filters,
        )?;

        let node_types_filter: Arc<[bool]> = if let Some(nf) = node_filters.first() {
            Arc::from(
                meta.node_type_meta()
                    .get_keys()
                    .iter()
                    .map(|k| nf.matches(Some(k)))
                    .collect::<Vec<bool>>(),
            )
        } else {
            Arc::from(Vec::<bool>::new())
        };

        Ok(ResolvedCompositeNodeFilter {
            filter,
            t_prop_ids,
            c_prop_ids,
            node_types_filter,
        })
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        layer_ids: &LayerIds,
        node: NodeStorageRef,
    ) -> bool {
        self.filter.matches_node(
            graph,
            &self.t_prop_ids,
            &self.c_prop_ids,
            &self.node_types_filter,
            layer_ids,
            node,
        )
    }
}

#[derive(Debug, Clone)]
pub struct NodeCompositeFilteredGraph<G> {
    graph: G,
    resolved_filter: ResolvedCompositeNodeFilter,
}

impl<'graph, G> NodeCompositeFilteredGraph<G> {
    pub(crate) fn new(graph: G, resolved_filter: ResolvedCompositeNodeFilter) -> Self {
        Self {
            graph,
            resolved_filter,
        }
    }
}

impl InternalNodeFilterOps for CompositeNodeFilter {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>> = NodeCompositeFilteredGraph<G>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        let meta = graph.node_meta();
        let resolved_filter = ResolvedCompositeNodeFilter::resolve(self, meta)?;
        Ok(NodeCompositeFilteredGraph::new(graph, resolved_filter))
    }
}

impl<'graph, G> Base for NodeCompositeFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for NodeCompositeFilteredGraph<G> {}
impl<G> Immutable for NodeCompositeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for NodeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for NodeCompositeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeCompositeFilteredGraph<G> {
    #[inline]
    fn nodes_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn node_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn edge_filter_includes_node_filter(&self) -> bool {
        false
    }

    #[inline]
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.filter_node(node, layer_ids) {
            self.resolved_filter
                .matches_node(&self.graph, layer_ids, node)
        } else {
            false
        }
    }
}
