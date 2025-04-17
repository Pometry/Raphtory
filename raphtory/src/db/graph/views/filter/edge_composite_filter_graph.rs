use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::edges::edge_ref::EdgeStorageRef,
            view::{
                internal::{
                    EdgeFilterOps, Immutable, InheritCoreOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
                    InheritNodeHistoryFilter, InheritTimeSemantics, Static,
                },
                Base,
            },
        },
        graph::views::filter::internal::InternalEdgeFilterOps,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::props::Meta;
use std::collections::HashMap;

use crate::db::{
    api::view::internal::InheritStorageOps, graph::views::filter::CompositeEdgeFilter,
};

#[derive(Debug, Clone)]
pub struct ResolvedCompositeEdgeFilter {
    pub filter: CompositeEdgeFilter,
    pub t_prop_ids: HashMap<String, usize>,
    pub c_prop_ids: HashMap<String, usize>,
}

impl ResolvedCompositeEdgeFilter {
    pub fn resolve(filter: CompositeEdgeFilter, meta: &Meta) -> Result<Self, GraphError> {
        let mut t_prop_ids = HashMap::new();
        let mut c_prop_ids = HashMap::new();

        fn traverse(
            filter: &CompositeEdgeFilter,
            meta: &Meta,
            t_prop_ids: &mut HashMap<String, usize>,
            c_prop_ids: &mut HashMap<String, usize>,
        ) -> Result<(), GraphError> {
            match filter {
                CompositeEdgeFilter::Property(pf) => {
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
                CompositeEdgeFilter::And(l, r) | CompositeEdgeFilter::Or(l, r) => {
                    traverse(l, meta, t_prop_ids, c_prop_ids)?;
                    traverse(r, meta, t_prop_ids, c_prop_ids)?;
                }
                CompositeEdgeFilter::Edge(_) => {}
            }
            Ok(())
        }

        traverse(&filter, meta, &mut t_prop_ids, &mut c_prop_ids)?;

        Ok(ResolvedCompositeEdgeFilter {
            filter,
            t_prop_ids,
            c_prop_ids,
        })
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        edge: EdgeStorageRef,
    ) -> bool {
        self.filter
            .matches_edge(graph, &self.t_prop_ids, &self.c_prop_ids, edge)
    }
}

#[derive(Debug, Clone)]
pub struct EdgeCompositeFilteredGraph<G> {
    graph: G,
    resolved_filter: ResolvedCompositeEdgeFilter,
}

impl<'graph, G> EdgeCompositeFilteredGraph<G> {
    pub(crate) fn new(graph: G, resolved_filter: ResolvedCompositeEdgeFilter) -> Self {
        Self {
            graph,
            resolved_filter,
        }
    }
}

impl InternalEdgeFilterOps for CompositeEdgeFilter {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>> = EdgeCompositeFilteredGraph<G>;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        let meta = graph.edge_meta();
        let resolve_filter = ResolvedCompositeEdgeFilter::resolve(self, meta)?;
        Ok(EdgeCompositeFilteredGraph::new(graph, resolve_filter))
    }
}

impl<'graph, G> Base for EdgeCompositeFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeCompositeFilteredGraph<G> {}
impl<G> Immutable for EdgeCompositeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgeCompositeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgeCompositeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for EdgeCompositeFilteredGraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.filter_edge(edge, layer_ids) {
            self.resolved_filter.matches_edge(&self.graph, edge)
        } else {
            false
        }
    }
}
