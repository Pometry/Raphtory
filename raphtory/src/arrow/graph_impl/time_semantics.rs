use itertools::Itertools;
use std::{iter, ops::Range, sync::Arc};

use once_cell::sync::Lazy;

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, properties::tprop::LayeredTProp, LayerIds, VID},
        storage::timeindex::TimeIndexOps,
    },
    db::api::view::{
        internal::{CoreGraphOps, EdgeFilter, EdgeWindowFilter, TimeSemantics},
        BoxedIter,
    },
    prelude::Prop,
};

use crate::{
    arrow::prelude::{ArrayOps, BaseArrayOps},
    core::storage::timeindex::TimeIndexIntoOps,
    prelude::TimeIndexEntry,
};
use rayon::prelude::*;

use super::ArrowGraph;

static WINDOW_FILTER: Lazy<EdgeWindowFilter> =
    Lazy::new(|| Arc::new(move |e, layer_ids, w| e.active(layer_ids, w)));

impl TimeSemantics for ArrowGraph {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.layers
            .par_iter()
            .map(|layer| layer.node(v).timestamps().first_t())
            .flatten()
            .min()
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.layers
            .par_iter()
            .map(|layer| layer.node(v).timestamps().last_t())
            .flatten()
            .max()
    }

    fn view_start(&self) -> Option<i64> {
        None
    }

    fn view_end(&self) -> Option<i64> {
        None
    }

    #[doc = " Returns the timestamp for the earliest activity"]
    fn earliest_time_global(&self) -> Option<i64> {
        Some(self.earliest())
    }

    #[doc = " Returns the timestamp for the latest activity"]
    fn latest_time_global(&self) -> Option<i64> {
        Some(self.latest())
    }

    #[doc = " Returns the timestamp for the earliest activity in the window"]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.layers
            .par_iter()
            .flat_map(|layer| {
                layer
                    .all_nodes_par()
                    .map(|node| node.timestamps().range(start..end).first_t())
                    .flatten()
            })
            .min()
    }

    #[doc = " Returns the timestamp for the latest activity in the window"]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.layers
            .par_iter()
            .flat_map(|layer| {
                layer
                    .all_nodes_par()
                    .map(|node| node.timestamps().range(start..end).last_t())
                    .flatten()
            })
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.layers
            .par_iter()
            .flat_map(|layer| layer.node(v).timestamps().range(start..end).first_t())
            .min()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.layers
            .par_iter()
            .flat_map(|layer| layer.node(v).timestamps().range(start..end).last_t())
            .max()
    }

    /// check if node `v` should be included in window `w`
    fn include_node_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        _edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.filtered_layers_par(layer_ids)
            .any(|layer| layer.node(v).timestamps().active(w.clone()))
    }

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(&self) -> &EdgeWindowFilter {
        &WINDOW_FILTER
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.layers
            .iter()
            .map(|layer| layer.node(v).timestamps().into_iter_t())
            .kmerge()
            .dedup()
            .collect()
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.layers
            .iter()
            .map(|layer| {
                layer
                    .node(v)
                    .timestamps()
                    .into_range(w.clone())
                    .into_iter_t()
            })
            .kmerge()
            .dedup()
            .collect()
    }

    fn edge_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        match e.layer() {
            Some(layer) => {
                if layer_ids.contains(layer) {
                    self.layers[*layer]
                        .edge(e.pid())
                        .timestamp_slice()
                        .into_iter()
                        .copied()
                        .collect()
                } else {
                    vec![]
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_history_window(&self, e: EdgeRef, layer_ids: LayerIds, w: Range<i64>) -> Vec<i64> {
        match e.layer() {
            Some(layer) => {
                if layer_ids.contains(layer) {
                    self.layers[*layer]
                        .edge(e.pid())
                        .timestamps()
                        .range(w)
                        .iter_t()
                        .collect()
                } else {
                    vec![]
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        if e.time().is_some() {
            return if layer_ids.contains(e.layer().unwrap()) {
                Box::new(iter::once(e))
            } else {
                Box::new(iter::empty())
            };
        }
        match e.layer() {
            Some(layer) => {
                if layer_ids.contains(layer) {
                    let layer_times = self.layers[*layer]
                        .edges
                        .time_col
                        .clone()
                        .into_value(e.pid().0);
                    Box::new(
                        layer_times
                            .into_iter()
                            .map(move |t| e.at(TimeIndexEntry::start(t))),
                    )
                } else {
                    Box::new(iter::empty())
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            Box::new(iter::once(e))
        } else {
            Box::new(iter::empty())
        }
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        if let Some(t) = e.time_t() {
            return if layer_ids.contains(e.layer().unwrap()) && w.contains(&t) {
                Box::new(iter::once(e))
            } else {
                Box::new(iter::empty())
            };
        }
        match e.layer() {
            Some(layer) => {
                if layer_ids.contains(layer) {
                    let windowed_times = self.layers[*layer]
                        .edges
                        .time_col
                        .clone()
                        .into_value(e.pid().0);
                    let start = windowed_times.partition_point(|v| v < &w.start);
                    let windowed_times = windowed_times.sliced(start..);
                    let end = windowed_times.partition_point(|v| v < &w.end);
                    let windowed_times = windowed_times.sliced(..end);
                    Box::new(
                        windowed_times
                            .into_iter()
                            .map(move |t| e.at(TimeIndexEntry::start(t))),
                    )
                } else {
                    Box::new(iter::empty())
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) && e.time_t().map(|t| w.contains(&t)).unwrap_or(true) {
            Box::new(iter::once(e))
        } else {
            Box::new(iter::empty())
        }
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            e.time_t()
                .or_else(|| self.layers[*layer].edge(e.pid()).timestamps().first_t())
        } else {
            None
        }
    }

    #[doc = " Get the time of the earliest activity of an edge `e` in window `w`"]
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            match e.time_t() {
                Some(t) => w.contains(&t).then_some(t),
                None => self.layers[*layer]
                    .edge(e.pid())
                    .timestamps()
                    .range(w)
                    .first_t(),
            }
        } else {
            None
        }
    }

    #[doc = " Get the time of the latest activity of an edge"]
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            e.time_t()
                .or_else(|| self.layers[*layer].edge(e.pid()).timestamps().last_t())
        } else {
            None
        }
    }

    #[doc = " Get the time of the latest activity of an edge `e` in window `w`"]
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            match e.time_t() {
                Some(t) => w.contains(&t).then_some(t),
                None => self.layers[*layer]
                    .edge(e.pid())
                    .timestamps()
                    .range(w)
                    .last_t(),
            }
        } else {
            None
        }
    }

    #[doc = " Get the edge deletions for use with materialize"]
    fn edge_deletion_history(&self, _e: EdgeRef, _layer_ids: LayerIds) -> Vec<i64> {
        vec![]
    }

    #[doc = " Get the edge deletions for use with materialize restricted to window `w`"]
    fn edge_deletion_history_window(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: LayerIds,
    ) -> Vec<i64> {
        vec![]
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: LayerIds) -> bool {
        let layer = e
            .layer()
            .expect("arrow edges should always have layer currently");
        layer_ids.contains(layer)
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: LayerIds, t: i64) -> bool {
        let layer = e
            .layer()
            .expect("arrow edges should always have layer currently");
        layer_ids.contains(layer)
    }

    fn has_temporal_prop(&self, _prop_id: usize) -> bool {
        //FIXME: arrow graph does not have properties yet
        false
    }

    fn temporal_prop_vec(&self, _prop_id: usize) -> Vec<(i64, Prop)> {
        todo!("arrow graph does not have properties yet")
    }

    fn has_temporal_prop_window(&self, _prop_id: usize, _w: Range<i64>) -> bool {
        //FIXME: arrow graph does not have properties yet
        false
    }

    fn temporal_prop_vec_window(
        &self,
        _prop_id: usize,
        _start: i64,
        _end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!("arrow graph does not have properties yet")
    }

    fn has_temporal_node_prop(&self, _v: VID, _prop_id: usize) -> bool {
        // FIXME: arrow nodes don't have properties yet
        false
    }

    #[doc = " and the second element is the property value."]
    fn temporal_node_prop_vec(&self, _v: VID, _id: usize) -> Vec<(i64, Prop)> {
        todo!("arrow nodes don't have properties yet")
    }

    fn has_temporal_node_prop_window(&self, _v: VID, _prop_id: usize, _w: Range<i64>) -> bool {
        // FIXME: arrow nodes don't have properties yet
        false
    }

    fn temporal_node_prop_vec_window(
        &self,
        _v: VID,
        _id: usize,
        _start: i64,
        _end: i64,
    ) -> Vec<(i64, Prop)> {
        todo!("arrow nodes don't have properties yet")
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        let layer_id = e.layer().expect("arrow edges always have layer currently");
        if !layer_ids.contains(layer_id) {
            return false;
        }
        self.layers[*layer_id]
            .edge(e.pid())
            .has_temporal_prop_window(prop_id, w)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, id, layer_ids)
            .into_iter()
            .map(|t_props| t_props.iter_window(start..end).collect::<Vec<_>>())
            .next()
            .unwrap_or_default()
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        let layer_id = e.layer().expect("arrow edges always have layer currently");
        if !layer_ids.contains(layer_id) {
            return false;
        }
        self.layers[*layer_id]
            .edge(e.pid())
            .has_temporal_prop(prop_id)
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.temporal_edge_prop(e, id, layer_ids)
            .into_iter()
            .map(|t_props| t_props.iter().collect::<Vec<_>>())
            .next()
            .unwrap_or_default()
    }
}
