use super::DiskGraph;
use crate::{
    arrow::graph_impl::tprops::read_tprop_column,
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexIntoOps, TimeIndexOps},
    },
    db::api::{
        storage::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{internal::TimeSemantics, BoxedIter},
    },
    prelude::*,
};
use itertools::Itertools;
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use rayon::prelude::*;
use std::{iter, ops::Range};

impl TimeSemantics for DiskGraph {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.inner
            .layers()
            .par_iter()
            .map(|layer| layer.node(v).timestamps().first_t())
            .flatten()
            .min()
    }

    fn node_latest_time(&self, v: VID) -> Option<i64> {
        self.inner
            .layers()
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
        let earliest = self.inner.earliest();
        if earliest == i64::MAX {
            None
        } else {
            Some(earliest)
        }
    }

    #[doc = " Returns the timestamp for the latest activity"]
    fn latest_time_global(&self) -> Option<i64> {
        let latest = self.inner.latest();
        if latest == i64::MIN {
            None
        } else {
            Some(latest)
        }
    }

    #[doc = " Returns the timestamp for the earliest activity in the window"]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.inner
            .layers()
            .par_iter()
            .flat_map(|layer| {
                layer
                    .all_nodes_par()
                    .map(|node| node.timestamps().range_t(start..end).first_t())
                    .flatten()
            })
            .min()
    }

    #[doc = " Returns the timestamp for the latest activity in the window"]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.inner
            .layers()
            .par_iter()
            .flat_map(|layer| {
                layer
                    .all_nodes_par()
                    .map(|node| node.timestamps().range_t(start..end).last_t())
                    .flatten()
            })
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner
            .layers()
            .par_iter()
            .flat_map(|layer| layer.node(v).timestamps().range_t(start..end).first_t())
            .min()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.inner
            .layers()
            .par_iter()
            .flat_map(|layer| layer.node(v).timestamps().range_t(start..end).last_t())
            .max()
    }

    /// check if node `v` should be included in window `w`
    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        self.filtered_layers_par(layer_ids).any(|layer| {
            layer.node(v.vid()).timestamps().active_t(w.clone())
                || self
                    .inner
                    .node_properties()
                    .as_ref()
                    .map(|props| {
                        props
                            .temporal_props
                            .timestamps::<TimeIndexEntry>(v.vid())
                            .active_t(w.clone())
                    })
                    .unwrap_or(false)
        })
    }

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        edge.active(layer_ids, w)
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.inner
            .layers()
            .iter()
            .map(|layer| layer.node(v).timestamps().into_iter_t())
            .kmerge()
            .dedup()
            .collect()
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.inner
            .layers()
            .iter()
            .map(|layer| {
                layer
                    .node(v)
                    .timestamps()
                    .into_range_t(w.clone())
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
                    self.inner
                        .layer(*layer)
                        .edge(e.pid())
                        .timestamp_slice()
                        .into_iter()
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
                    self.inner
                        .layer(*layer)
                        .edge(e.pid())
                        .timestamps::<TimeIndexEntry>()
                        .range_t(w)
                        .iter_t()
                        .collect()
                } else {
                    vec![]
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        edge.additions_par_iter(layer_ids)
            .map(|(_, a)| a.len())
            .sum()
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        edge.additions_par_iter(layer_ids)
            .map(|(_, a)| a.range_t(w.clone()).len())
            .sum()
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
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
                    let layer_times = self
                        .inner
                        .layer(*layer)
                        .edge(e.pid())
                        .timestamps::<TimeIndexEntry>()
                        .into_iter();
                    Box::new(layer_times.into_iter().map(move |t| e.at(t)))
                } else {
                    Box::new(iter::empty())
                }
            }
            None => panic!("arrow edges should always have a layer currently"),
        }
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: &LayerIds) -> BoxedIter<EdgeRef> {
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
        layer_ids: &LayerIds,
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
                    let windowed_times = self
                        .inner
                        .layer(*layer)
                        .edge(e.pid())
                        .timestamps::<TimeIndexEntry>()
                        .range_t(w)
                        .into_iter();
                    Box::new(windowed_times.map(move |t| e.at(t)))
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
        layer_ids: &LayerIds,
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

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            e.time_t().or_else(|| {
                self.inner
                    .layer(*layer)
                    .edge(e.pid())
                    .timestamps::<TimeIndexEntry>()
                    .first_t()
            })
        } else {
            None
        }
    }

    #[doc = " Get the time of the earliest activity of an edge `e` in window `w`"]
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            match e.time_t() {
                Some(t) => w.contains(&t).then_some(t),
                None => self
                    .inner
                    .layer(*layer)
                    .edge(e.pid())
                    .timestamps::<TimeIndexEntry>()
                    .range_t(w)
                    .first_t(),
            }
        } else {
            None
        }
    }

    #[doc = " Get the time of the latest activity of an edge"]
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            e.time_t().or_else(|| {
                self.inner
                    .layer(*layer)
                    .edge(e.pid())
                    .timestamps::<TimeIndexEntry>()
                    .last_t()
            })
        } else {
            None
        }
    }

    #[doc = " Get the time of the latest activity of an edge `e` in window `w`"]
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        let layer = e
            .layer()
            .expect("arrow edges should always have a layer currently");
        if layer_ids.contains(layer) {
            match e.time_t() {
                Some(t) => w.contains(&t).then_some(t),
                None => self
                    .inner
                    .layer(*layer)
                    .edge(e.pid())
                    .timestamps::<TimeIndexEntry>()
                    .range_t(w)
                    .last_t(),
            }
        } else {
            None
        }
    }

    #[doc = " Get the edge deletions for use with materialize"]
    fn edge_deletion_history(&self, _e: EdgeRef, _layer_ids: &LayerIds) -> Vec<i64> {
        vec![]
    }

    #[doc = " Get the edge deletions for use with materialize restricted to window `w`"]
    fn edge_deletion_history_window(
        &self,
        _e: EdgeRef,
        _w: Range<i64>,
        _layer_ids: &LayerIds,
    ) -> Vec<i64> {
        vec![]
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        let layer = e
            .layer()
            .expect("arrow edges should always have layer currently");
        layer_ids.contains(layer)
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        let layer = e
            .layer()
            .expect("arrow edges should always have layer currently");
        layer_ids.contains(layer)
            && self
                .inner
                .layer(*layer)
                .edge(e.pid())
                .timestamps::<TimeIndexEntry>()
                .first_t()
                >= Some(t)
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

    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        match &self.inner.node_properties() {
            None => false,
            Some(props) => props.temporal_props.has_prop(v, prop_id),
        }
    }

    #[doc = " and the second element is the property value."]
    fn temporal_node_prop_vec(&self, v: VID, id: usize) -> Vec<(i64, Prop)> {
        match &self.inner.node_properties() {
            None => {
                vec![]
            }
            Some(props) => props.temporal_props.prop(v, id).iter_t().collect(),
        }
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        match &self.inner.node_properties() {
            None => false,
            Some(props) => props.temporal_props.has_prop_window(v, prop_id, w),
        }
    }

    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        match &self.inner.node_properties() {
            None => vec![],
            Some(props) => props
                .temporal_props
                .prop(v, id)
                .iter_window_t(start..end)
                .collect(),
        }
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        let layer_id = e.layer().expect("arrow edges always have layer currently");
        if !layer_ids.contains(layer_id) {
            return false;
        }
        self.inner
            .layer(*layer_id)
            .edge(e.pid())
            .has_temporal_prop_window(prop_id, w)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: &LayerIds,
    ) -> Vec<(i64, Prop)> {
        if let Some(layer_id) = self.layer_from_ids(&layer_ids.constrain_from_edge(e)) {
            let edge = self.inner.layer(layer_id).edge(e.pid());
            if let Some(t_prop) = edge
                .temporal_property_field(id)
                .and_then(|field| read_tprop_column(id, field, edge))
            {
                match e.time() {
                    Some(t) => {
                        debug_assert!((start..end).contains(&t.t()));
                        t_prop.at(&t).map(|v| (t.t(), v)).into_iter().collect()
                    }
                    None => t_prop.iter_window_t(start..end).collect(),
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: &LayerIds) -> bool {
        let layer_id = e.layer().expect("arrow edges always have layer currently");
        if !layer_ids.contains(layer_id) {
            return false;
        }
        self.inner
            .layer(*layer_id)
            .edge(e.pid())
            .has_temporal_prop_inner(prop_id)
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
    ) -> Vec<(i64, Prop)> {
        match layer_ids.constrain_from_edge(e) {
            LayerIds::None => {
                vec![]
            }
            LayerIds::All => {
                todo!("multilayer edge view not supported in arrow yet")
            }
            LayerIds::One(layer_id) => {
                let edge = self.inner.layer(layer_id).edge(e.pid());
                if let Some(t_prop) = edge
                    .temporal_property_field(id)
                    .and_then(|field| read_tprop_column(id, field, edge))
                {
                    match e.time() {
                        Some(t) => t_prop.at(&t).map(|v| (t.t(), v)).into_iter().collect(),
                        None => t_prop.iter_t().collect(),
                    }
                } else {
                    vec![]
                }
            }
            LayerIds::Multiple(_) => {
                todo!("multilayer edge view not supported in arrow yet")
            }
        }
    }
}
