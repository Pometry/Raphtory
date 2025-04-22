use crate::{
    core::{entities::LayerIds, utils::errors::GraphError, Prop},
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::{
                edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                nodes::node_ref::NodeStorageRef,
            },
            view::{
                internal::{
                    EdgeFilterOps, EdgeTimeSemanticsOps, GraphTimeSemanticsOps, Immutable,
                    InheritCoreOps, InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps,
                    InheritMaterialize, InheritNodeHistoryFilter, InheritStorageOps, NodeFilterOps,
                    NodeTimeSemanticsOps, Static, TimeSemantics,
                },
                Base,
            },
        },
        graph::views::{
            layer_graph::LayeredGraph, property_filter::internal::InternalExplodedEdgeFilterOps,
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::{
    core::{
        entities::{EID, ELID},
        storage::timeindex::{TimeIndexEntry, TimeIndexOps},
    },
    iter::BoxedLDIter,
};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct ExplodedEdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: Option<usize>,
    filter: PropertyFilter,
}

impl<G> Static for ExplodedEdgePropertyFilteredGraph<G> {}
impl<G> Immutable for ExplodedEdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilteredGraph<G> {
    pub(crate) fn new(graph: G, prop_id: Option<usize>, filter: PropertyFilter) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }

    fn filter(&self, e: EID, t: TimeIndexEntry, layer: usize) -> bool {
        self.filter.matches(
            self.prop_id
                .and_then(|prop_id| {
                    let time_semantics = self.graph.edge_time_semantics();
                    let edge = self.graph.core_edge(e);
                    time_semantics.temporal_edge_prop_exploded(
                        edge.as_ref(),
                        &self.graph,
                        prop_id,
                        t,
                        layer,
                    )
                })
                .as_ref(),
        )
    }
}

impl InternalExplodedEdgeFilterOps for PropertyFilter {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>> =
        ExplodedEdgePropertyFilteredGraph<G>;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_ids(graph.edge_meta())?;
        Ok(ExplodedEdgePropertyFilteredGraph::new(
            graph.clone(),
            t_prop_id,
            self,
        ))
    }
}

impl<'graph, G> Base for ExplodedEdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for ExplodedEdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for ExplodedEdgePropertyFilteredGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_history_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        let res = self.graph.filter_edge_history(eid, t, layer_ids) && {
            if eid.is_deletion() {
                self.filter(eid.edge, t.previous(), eid.layer())
            } else {
                self.filter(eid.edge, t, eid.layer())
            }
        };
        res
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && edge
                .filtered_additions_iter(LayeredGraph::new(&self, layer_ids.clone()))
                .any(|(_, additions)| !additions.is_empty())
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for ExplodedEdgePropertyFilteredGraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }

    fn node_list_trusted(&self) -> bool {
        false
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        let res = self.graph.filter_node(node, layer_ids)
            && self
                .node_time_semantics()
                .node_valid(node, LayeredGraph::new(self, layer_ids.clone()));
        res
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph.node_time_semantics()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        self.graph.edge_time_semantics()
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_global()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_global()
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.earliest_time_window(start, end)
    }
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.latest_time_window(start, end)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph.has_temporal_prop(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_iter(prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn temporal_prop_last_at(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_last_at(prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.graph.temporal_prop_last_at_window(prop_id, t, w)
    }
}
