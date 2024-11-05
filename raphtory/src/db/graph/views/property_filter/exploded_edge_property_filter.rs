use crate::{
    core::{
        entities::{properties::props::Meta, LayerIds},
        utils::errors::GraphError,
        Prop, PropType,
    },
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::{
                edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                nodes::node_ref::NodeStorageRef,
            },
            view::{
                internal::{
                    EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps, InheritListOps,
                    InheritMaterialize, InheritNodeFilterOps, InternalLayerOps, Static,
                    TimeSemantics,
                },
                Base, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::views::property_filter::{
            internal::InternalExplodedEdgeFilterOps, PropertyValueFilter,
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::core::{
    entities::{edges::edge_ref::EdgeRef, VID},
    storage::timeindex::TimeIndexEntry,
};
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct ExplodedEdgePropertyFilteredGraph<G> {
    graph: G,
    prop_id: Option<usize>,
    filter: PropertyValueFilter,
}

impl<G> Static for ExplodedEdgePropertyFilteredGraph<G> {}
impl<G> Immutable for ExplodedEdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> ExplodedEdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        prop_id: Option<usize>,
        filter: impl Into<PropertyValueFilter>,
    ) -> Self {
        Self {
            graph,
            prop_id,
            filter: filter.into(),
        }
    }

    fn filter(&self, e: EdgeRef, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.filter.filter(
            self.prop_id
                .and_then(|prop_id| self.graph.temporal_edge_prop_at(e, prop_id, t, layer_ids))
                .as_ref(),
        )
    }
}

fn get_id_and_check_type(
    meta: &Meta,
    property: &str,
    dtype: PropType,
) -> Result<Option<usize>, GraphError> {
    let t_prop_id = meta
        .temporal_prop_meta()
        .get_and_validate(property, dtype)?;
    Ok(t_prop_id)
}

fn get_id(meta: &Meta, property: &str) -> Option<usize> {
    let t_prop_id = meta.temporal_prop_meta().get_id(property);
    t_prop_id
}

impl InternalExplodedEdgeFilterOps for PropertyFilter {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>> =
        ExplodedEdgePropertyFilteredGraph<G>;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        let t_prop_id = match &self.filter {
            PropertyValueFilter::ByValue(filter) => {
                get_id_and_check_type(graph.edge_meta(), &self.name, filter.dtype())?
            }
            _ => get_id(graph.edge_meta(), &self.name),
        };
        Ok(ExplodedEdgePropertyFilteredGraph::new(
            graph.clone(),
            t_prop_id,
            self.filter,
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
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ExplodedEdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for ExplodedEdgePropertyFilteredGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self
                .edge_exploded(edge.out_ref(), layer_ids)
                .next()
                .is_some()
    }
}

impl<'graph, G: GraphViewOps<'graph>> TimeSemantics for ExplodedEdgePropertyFilteredGraph<G> {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph.node_earliest_time(v)
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.graph.node_latest_time(v)
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

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.node_earliest_time_window(v, start, end)
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.node_latest_time_window(v, start, end)
    }

    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.include_node_window(v, w, layer_ids)
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.edge_window_exploded(edge.out_ref(), w, layer_ids)
            .next()
            .is_some()
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.node_history(v)
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        // FIXME: this is potentially wrong but there is no way to fix this right now as nodes don't
        // separate timestamps from node property updates and edge additions currently
        self.graph.node_history_window(v, w)
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_history(e, layer_ids)
            .filter(move |t| self.filter(e, *t, layer_ids))
            .into_dyn_boxed()
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_history_window(e, layer_ids, w)
            .filter(move |t| self.filter(e, *t, layer_ids))
            .into_dyn_boxed()
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.edge_exploded(edge.out_ref(), layer_ids).count()
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        self.edge_window_exploded(edge.out_ref(), w, layer_ids)
            .count()
    }

    fn edge_exploded<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_exploded(e, layer_ids)
            .filter(move |&e| {
                self.filter(
                    e,
                    e.time().expect("exploded edge should have timestamp"),
                    layer_ids,
                )
            })
            .into_dyn_boxed()
    }

    fn edge_layers<'a>(&'a self, e: EdgeRef, layer_ids: &'a LayerIds) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_layers(e, layer_ids)
            .filter(move |&e| self.edge_exploded(e, layer_ids).next().is_some())
            .into_dyn_boxed()
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_window_exploded(e, w, layer_ids)
            .filter(move |&e| {
                self.filter(
                    e,
                    e.time().expect("exploded edge should have timestamp"),
                    layer_ids,
                )
            })
            .into_dyn_boxed()
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_window_layers(e, w.clone(), layer_ids)
            .filter(move |&e| {
                self.edge_window_exploded(
                    e,
                    w.clone(),
                    &LayerIds::One(e.layer().expect("exploded edge should have layer")),
                )
                .next()
                .is_some()
            })
            .into_dyn_boxed()
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.edge_exploded(e, layer_ids)
            .next()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.edge_window_exploded(e, w, layer_ids)
            .next()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        // FIXME: this is inefficient, need exploded to return something more useful
        self.edge_exploded(e, layer_ids)
            .last()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        // FIXME: this is inefficient, need exploded to return something more useful
        self.edge_window_exploded(e, w, layer_ids)
            .last()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_deletion_history(e, layer_ids)
            .filter(move |t| self.filter(e, t.previous(), layer_ids))
            .into_dyn_boxed()
    }

    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &'a LayerIds,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph
            .edge_deletion_history_window(e, w, layer_ids)
            .filter(move |t| self.filter(e, t.previous(), layer_ids))
            .into_dyn_boxed()
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        // FIXME: this is probably not correct
        self.graph.edge_is_valid(e, layer_ids)
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        // FIXME: this is probably not correct
        self.graph.edge_is_valid_at_end(e, layer_ids, t)
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph.has_temporal_prop(prop_id)
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(i64, Prop)> {
        self.graph.temporal_prop_iter(prop_id)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(i64, Prop)> {
        self.graph.temporal_prop_iter_window(prop_id, start, end)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(prop_id, start, end)
    }

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        self.graph.has_temporal_node_prop(v, prop_id)
    }

    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        // FIXME: this is wrong as we should not include filtered-out edges here
        self.graph.temporal_node_prop_hist(v, id)
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_node_prop_window(v, prop_id, w)
    }

    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        // FIXME: this is wrong as we should not include filtered-out edges here
        self.graph.temporal_node_prop_hist_window(v, id, start, end)
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.edge_window_exploded(e, w, layer_ids).any(move |e| {
            self.graph
                .temporal_edge_prop_at(e, prop_id, e.time().unwrap(), layer_ids)
                .is_some()
        })
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph
            .temporal_edge_prop_hist_window(e, id, start, end, layer_ids)
            .filter(move |(ti, _)| self.filter(e, *ti, self.layer_ids()))
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        self.graph
            .temporal_edge_prop_at(e, id, t, layer_ids)
            .filter(move |_| self.filter(e, t, layer_ids))
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        self.temporal_edge_prop_hist(e, prop_id, layer_ids)
            .next()
            .is_some()
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph
            .temporal_edge_prop_hist(e, id, layer_ids)
            .filter(move |(ti, _)| self.filter(e, *ti, self.layer_ids()))
            .into_dyn_boxed()
    }
}
