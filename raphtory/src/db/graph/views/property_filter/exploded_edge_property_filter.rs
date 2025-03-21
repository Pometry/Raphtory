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
                    CoreGraphOps, EdgeFilterOps, GraphTimeSemanticsOps, Immutable, InheritCoreOps,
                    InheritLayerOps, InheritListOps, InheritMaterialize, NodeFilterOps,
                    NodeTimeSemanticsOps, Static, TimeSemantics,
                },
                Base, BoxedLIter, IntoDynBoxed,
            },
        },
        graph::views::{
            layer_graph::LayeredGraph,
            property_filter::{internal::InternalExplodedEdgeFilterOps, PropertyValueFilter},
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, EID, ELID},
        storage::timeindex::{AsTime, TimeIndexEntry},
    },
    iter::BoxedLDIter,
};
use std::{borrow::Cow, ops::Range};

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

    fn filter(&self, e: EID, t: TimeIndexEntry, layer: usize) -> bool {
        self.filter.filter(
            self.prop_id
                .and_then(|prop_id| self.graph.temporal_edge_prop_at(e, prop_id, t, layer))
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
            && self
                .edge_exploded(edge.out_ref(), Cow::Borrowed(layer_ids))
                .next()
                .is_some()
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
                .valid(node, LayeredGraph::new(self, layer_ids.clone()));
        res
    }
}

impl<'graph, G: GraphViewOps<'graph>> GraphTimeSemanticsOps
    for ExplodedEdgePropertyFilteredGraph<G>
{
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph.node_time_semantics()
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

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.edge_window_exploded(edge.out_ref(), w, Cow::Borrowed(layer_ids))
            .next()
            .is_some()
    }

    fn edge_history<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph
            .edge_history(e, layer_ids.clone())
            .filter(move |(t, layer)| self.filter(e, *t, *layer))
            .into_dyn_boxed()
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph
            .edge_history_window(e, layer_ids, w)
            .filter(move |(t, layer)| self.filter(e, *t, *layer))
            .into_dyn_boxed()
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.edge_exploded(edge.out_ref(), Cow::Borrowed(layer_ids))
            .count()
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        self.edge_window_exploded(edge.out_ref(), w, Cow::Borrowed(layer_ids))
            .count()
    }

    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_exploded(e, layer_ids.clone())
            .filter(move |&e| {
                self.filter(
                    e.pid(),
                    e.time().expect("exploded edge should have timestamp"),
                    e.layer().expect("exploded edge should have layer"),
                )
            })
            .into_dyn_boxed()
    }

    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_layers(e, layer_ids.clone())
            .filter(move |&e| {
                self.edge_exploded(e, Cow::Borrowed(&layer_ids))
                    .next()
                    .is_some()
            })
            .into_dyn_boxed()
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_window_exploded(e, w, layer_ids.clone())
            .filter(move |&e| {
                self.filter(
                    e.pid(),
                    e.time().expect("exploded edge should have timestamp"),
                    e.layer().expect("exploded edge should have layer"),
                )
            })
            .into_dyn_boxed()
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph
            .edge_window_layers(e, w.clone(), layer_ids)
            .filter(move |&e| {
                self.edge_window_exploded(
                    e,
                    w.clone(),
                    Cow::Borrowed(&LayerIds::One(
                        e.layer().expect("exploded edge should have layer"),
                    )),
                )
                .next()
                .is_some()
            })
            .into_dyn_boxed()
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.edge_exploded(e, Cow::Borrowed(layer_ids))
            .next()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.edge_window_exploded(e, w, Cow::Borrowed(layer_ids))
            .next()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        // FIXME: this is inefficient, need exploded to return something more useful
        self.edge_exploded(e, Cow::Borrowed(layer_ids))
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
        self.edge_window_exploded(e, w, Cow::Borrowed(layer_ids))
            .last()
            .map(|e| e.time_t().unwrap())
    }

    fn edge_deletion_history<'a>(
        &'a self,
        e: EID,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph
            .edge_deletion_history(e, layer_ids.clone())
            .filter(move |(t, l)| self.filter(e, t.previous(), *l))
            .into_dyn_boxed()
    }

    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EID,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize)> {
        self.graph
            .edge_deletion_history_window(e, w, layer_ids.clone())
            .filter(move |(t, l)| self.filter(e, t.previous(), *l))
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

    fn temporal_edge_prop_at(
        &self,
        e: EID,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        // this is called from exploded edge, filtering should already have been handled
        self.graph.temporal_edge_prop_at(e, prop_id, t, layer_id)
    }

    fn temporal_edge_prop_last_at(
        &self,
        e: EID,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
    ) -> Option<Prop> {
        self.temporal_edge_prop_hist_window_rev(e, id, i64::MIN, t.t().saturating_add(1), layer_ids)
            .filter(|(ti, _, _)| ti <= &t)
            .next()
            .map(|(_, _, p)| p)
    }

    fn temporal_edge_prop_last_at_window(
        &self,
        e: EID,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop> {
        let wi = TimeIndexEntry::range(w.clone());
        if wi.contains(&t) {
            self.temporal_edge_prop_hist_window_rev(
                e,
                prop_id,
                w.start,
                t.t().saturating_add(1),
                layer_ids,
            )
            .filter(|(ti, _, _)| ti <= &t)
            .next()
            .map(|(_, _, p)| p)
        } else {
            None
        }
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EID,
        id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph
            .temporal_edge_prop_hist(e, id, layer_ids.clone())
            .filter(move |(ti, layer, _)| self.filter(e, *ti, *layer))
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_rev<'a>(
        &'a self,
        e: EID,
        id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph
            .temporal_edge_prop_hist_rev(e, id, layer_ids)
            .filter(move |(ti, layer, _)| self.filter(e, *ti, *layer))
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EID,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph
            .temporal_edge_prop_hist_window(e, id, start, end, layer_ids)
            .filter(move |(ti, layer, _)| self.filter(e, *ti, *layer))
            .into_dyn_boxed()
    }

    fn temporal_edge_prop_hist_window_rev<'a>(
        &'a self,
        e: EID,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, usize, Prop)> {
        self.graph
            .temporal_edge_prop_hist_window_rev(e, id, start, end, layer_ids)
            .filter(move |(ti, layer, _)| self.filter(e, *ti, *layer))
            .into_dyn_boxed()
    }

    fn constant_edge_prop(&self, e: EID, id: usize, layer_ids: Cow<LayerIds>) -> Option<Prop> {
        self.graph.constant_edge_prop(e, id, layer_ids)
    }

    fn constant_edge_prop_window(
        &self,
        e: EID,
        id: usize,
        layer_ids: Cow<LayerIds>,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.graph.constant_edge_prop_window(e, id, layer_ids, w)
    }
}
