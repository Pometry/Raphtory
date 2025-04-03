use crate::{
    core::Prop,
    db::api::{
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::internal::{
            time_semantics::time_semantics_ops::NodeTimeSemanticsOps, CoreGraphOps,
            EdgeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
    },
    iter::{BoxedLDIter, BoxedLIter, IntoDynBoxed, IntoDynDBoxed},
};
use std::{iter, ops::Range};

pub struct EventSemantics;

impl NodeTimeSemanticsOps for EventSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).first_t()
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).last_t()
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).first_t()
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).last_t()
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).iter_t().into_dyn_boxed()
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).range_t(w).iter_t().into_dyn_boxed()
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows()
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows_window(TimeIndexEntry::range(w))
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        !node.history(view).is_empty()
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        node.history(view).active_t(w)
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.iter().into_dyn_dboxed()
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.iter_window(TimeIndexEntry::range(w)).into_dyn_dboxed()
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let prop = node.tprop(prop_id);
        prop.last_before(t.next())
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        _view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        let w = TimeIndexEntry::range(w);
        if w.contains(&t) {
            let prop = node.tprop(prop_id);
            prop.last_before(t.next()).filter(|(t, _)| w.contains(t))
        } else {
            None
        }
    }
}

impl EdgeTimeSemanticsOps for EventSemantics {
    fn include_edge_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> bool {
        if view.edge_history_filtered() {
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .any(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .range_t(w.clone())
                        .iter()
                        .filter(|t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .next()
                        .is_some()
                })
        } else {
            edge.added(view.layer_ids(), w)
        }
    }

    fn edge_history<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        if view.edge_history_filtered() {
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .map(move |(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    let view = view.clone();
                    additions
                        .iter()
                        .filter(move |t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .map(move |t| (t, layer_id))
                })
                .kmerge()
                .into_dyn_boxed()
        } else {
            edge.additions_iter(view.layer_ids())
                .map(|(layer_id, additions)| additions.iter().map(move |t| (t, layer_id)))
                .kmerge()
                .into_dyn_boxed()
        }
    }

    fn edge_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        edge: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        if view.edge_history_filtered() {
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .map(move |(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    let view = view.clone();
                    additions
                        .range_t(w.clone())
                        .iter()
                        .filter(move |t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .map(move |t| (t, layer_id))
                })
                .kmerge()
                .into_dyn_boxed()
        } else {
            edge.additions_iter(view.layer_ids())
                .map(move |(layer_id, additions)| {
                    additions
                        .range_t(w.clone())
                        .iter()
                        .map(move |t| (t, layer_id))
                })
                .kmerge()
                .into_dyn_boxed()
        }
    }

    fn edge_exploded_count<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
    ) -> usize {
        if view.edge_history_filtered() {
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .iter()
                        .filter(|t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .count()
                })
                .sum()
        } else {
            edge.additions_iter(view.layer_ids())
                .map(|(_, additions)| additions.len())
                .sum()
        }
    }

    fn edge_exploded_count_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        edge: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> usize {
        if view.edge_history_filtered() {
            let eid = edge.eid();
            edge.additions_iter(view.layer_ids())
                .map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .range_t(w.clone())
                        .iter()
                        .filter(|t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .count()
                })
                .sum()
        } else {
            edge.additions_iter(view.layer_ids())
                .map(|(_, additions)| additions.range_t(w.clone()).len())
                .sum()
        }
    }

    fn edge_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let eref = e.out_ref();
        self.edge_history(e, view)
            .map(move |(t, layer_id)| eref.at(t).at_layer(layer_id))
            .into_dyn_boxed()
    }

    fn edge_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let eref = e.out_ref();
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(move |(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    let view = view.clone();
                    additions
                        .iter()
                        .filter(move |t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .next()
                        .map(move |_| eref.at_layer(layer_id))
                })
                .into_dyn_boxed()
        } else {
            e.layer_ids_iter(view.layer_ids())
                .map(move |layer_id| eref.at_layer(layer_id))
                .into_dyn_boxed()
        }
    }

    fn edge_window_exploded<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let eref = e.out_ref();
        self.edge_history_window(e, view, w)
            .map(move |(t, layer_id)| eref.at(t).at_layer(layer_id))
            .into_dyn_boxed()
    }

    fn edge_window_layers<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let eref = e.out_ref();
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(move |(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    let view = view.clone();
                    additions
                        .range_t(w.clone())
                        .iter()
                        .filter(move |t| view.filter_edge_history(elid, *t, view.layer_ids()))
                        .next()
                        .map(move |_| eref.at_layer(layer_id))
                })
                .into_dyn_boxed()
        } else {
            e.additions_iter(view.layer_ids())
                .filter_map(move |(layer_id, additions)| {
                    additions
                        .active_t(w.clone())
                        .then(move || eref.at_layer(layer_id))
                })
                .into_dyn_boxed()
        }
    }

    fn edge_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .iter()
                        .filter_map(|t| {
                            view.filter_edge_history(elid, t, view.layer_ids())
                                .then_some(t.t())
                        })
                        .next()
                })
                .min()
        } else {
            e.additions_iter(view.layer_ids())
                .filter_map(|(_, additions)| additions.first_t())
                .min()
        }
    }

    fn edge_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .range_t(w.clone())
                        .iter()
                        .filter_map(|t| {
                            view.filter_edge_history(elid, t, view.layer_ids())
                                .then_some(t.t())
                        })
                        .next()
                })
                .min()
        } else {
            e.additions_iter(view.layer_ids())
                .filter_map(|(_, additions)| additions.range_t(w.clone()).first_t())
                .min()
        }
    }

    fn edge_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
    ) -> Option<i64> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .iter_rev()
                        .filter_map(|t| {
                            view.filter_edge_history(elid, t, view.layer_ids())
                                .then_some(t.t())
                        })
                        .next()
                })
                .max()
        } else {
            e.additions_iter(view.layer_ids())
                .filter_map(|(_, additions)| additions.last_t())
                .max()
        }
    }

    fn edge_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.additions_iter(view.layer_ids())
                .filter_map(|(layer_id, additions)| {
                    let elid = eid.with_layer(layer_id);
                    additions
                        .range_t(w.clone())
                        .iter_rev()
                        .filter_map(|t| {
                            view.filter_edge_history(elid, t, view.layer_ids())
                                .then_some(t.t())
                        })
                        .next()
                })
                .max()
        } else {
            e.additions_iter(view.layer_ids())
                .filter_map(|(_, additions)| additions.range_t(w.clone()).last_t())
                .max()
        }
    }

    fn edge_deletion_history<'graph, G: GraphViewOps<'graph>>(
        self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        iter::empty().into_dyn_boxed()
    }

    fn edge_deletion_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        _e: EdgeStorageRef<'graph>,
        _view: G,
        _w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize)> {
        iter::empty().into_dyn_boxed()
    }

    fn edge_is_valid<'graph, G: GraphViewOps<'graph>>(&self, _e: EdgeStorageRef, _view: G) -> bool {
        true
    }

    fn edge_is_valid_at_end<'graph, G: GraphViewOps<'graph>>(
        &self,
        _e: EdgeStorageRef,
        _view: G,
        _t: i64,
    ) -> bool {
        true
    }

    fn temporal_edge_prop_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        layer_id: usize,
    ) -> Option<Prop> {
        let eid = e.eid();
        if view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids()) {
            e.temporal_prop_layer(layer_id, prop_id).at(&t)
        } else {
            None
        }
    }

    fn temporal_edge_prop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<Prop> {
        let last = if view.edge_history_filtered() {
            let eid = e.eid();
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .filter_map(|(layer_id, prop)| {
                    prop.iter_window(TimeIndexEntry::MIN..t.next())
                        .rev()
                        .filter(|(t, _)| {
                            view.filter_edge_history(eid.with_layer(layer_id), *t, view.layer_ids())
                        })
                        .next()
                })
                .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
        } else {
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .filter_map(|(_, prop)| prop.last_before(t.next()))
                .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
        };
        last.map(|(_, v)| v)
    }

    fn temporal_edge_prop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<Prop> {
        if w.contains(&t.t()) {
            let last = if view.edge_history_filtered() {
                let eid = e.eid();
                e.temporal_prop_iter(view.layer_ids(), prop_id)
                    .filter_map(|(layer_id, prop)| {
                        prop.iter_window(TimeIndexEntry::start(w.start)..t.next())
                            .rev()
                            .filter(|(t, _)| {
                                view.filter_edge_history(
                                    eid.with_layer(layer_id),
                                    *t,
                                    view.layer_ids(),
                                )
                            })
                            .next()
                    })
                    .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
            } else {
                e.temporal_prop_iter(view.layer_ids(), prop_id)
                    .filter_map(|(_, prop)| {
                        prop.last_before(t.next())
                            .filter(|(t, _)| w.contains(&t.t()))
                    })
                    .max_by(|(t1, _), (t2, _)| t1.cmp(t2))
            };
            last.map(|(_, v)| v)
        } else {
            None
        }
    }

    fn temporal_edge_prop_hist<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(|(layer_id, prop)| {
                    let view = view.clone();
                    prop.iter().filter_map(move |(t, v)| {
                        view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids())
                            .then_some((t, layer_id, v))
                    })
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
                .into_dyn_boxed()
        } else {
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(|(layer_id, prop)| prop.iter().map(move |(t, v)| (t, layer_id, v)))
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
                .into_dyn_boxed()
        }
    }

    fn temporal_edge_prop_hist_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(|(layer_id, prop)| {
                    let view = view.clone();
                    prop.iter().rev().filter_map(move |(t, v)| {
                        view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids())
                            .then_some((t, layer_id, v))
                    })
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
                .into_dyn_boxed()
        } else {
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(|(layer_id, prop)| prop.iter().rev().map(move |(t, v)| (t, layer_id, v)))
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
                .into_dyn_boxed()
        }
    }

    fn temporal_edge_prop_hist_window<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(move |(layer_id, prop)| {
                    let view = view.clone();
                    prop.iter_window(TimeIndexEntry::range(w.clone()))
                        .filter_map(move |(t, v)| {
                            view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids())
                                .then_some((t, layer_id, v))
                        })
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
                .into_dyn_boxed()
        } else {
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(move |(layer_id, prop)| {
                    prop.iter_window(TimeIndexEntry::range(w.clone()))
                        .map(move |(t, v)| (t, layer_id, v))
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 <= t2)
                .into_dyn_boxed()
        }
    }

    fn temporal_edge_prop_hist_window_rev<'graph, G: GraphViewOps<'graph>>(
        self,
        e: EdgeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, usize, Prop)> {
        if view.edge_history_filtered() {
            let eid = e.eid();
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(move |(layer_id, prop)| {
                    let view = view.clone();
                    prop.iter_window(TimeIndexEntry::range(w.clone()))
                        .rev()
                        .filter_map(move |(t, v)| {
                            view.filter_edge_history(eid.with_layer(layer_id), t, view.layer_ids())
                                .then_some((t, layer_id, v))
                        })
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
                .into_dyn_boxed()
        } else {
            e.temporal_prop_iter(view.layer_ids(), prop_id)
                .map(move |(layer_id, prop)| {
                    prop.iter_window(TimeIndexEntry::range(w.clone()))
                        .rev()
                        .map(move |(t, v)| (t, layer_id, v))
                })
                .kmerge_by(|(t1, _, _), (t2, _, _)| t1 >= t2)
                .into_dyn_boxed()
        }
    }

    fn constant_edge_prop<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
    ) -> Option<Prop> {
        let layer_ids = view.layer_ids();
        match layer_ids {
            LayerIds::None => return None,
            LayerIds::All => match view.unfiltered_num_layers() {
                0 => return None,
                1 => return e.constant_prop_layer(0, prop_id),
                _ => {}
            },
            LayerIds::One(layer_id) => return e.constant_prop_layer(*layer_id, prop_id),
            _ => {}
        };
        let mut values = e
            .constant_prop_iter(layer_ids, prop_id)
            .map(|(layer, v)| (view.get_layer_name(layer), v))
            .peekable();
        if values.peek().is_some() {
            Some(Prop::map(values))
        } else {
            None
        }
    }

    fn constant_edge_prop_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        e: EdgeStorageRef,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> Option<Prop> {
        let layer_ids = view.layer_ids();
        match layer_ids {
            LayerIds::None => return None,
            LayerIds::All => match view.unfiltered_num_layers() {
                0 => return None,
                1 => {
                    return if e.additions(0).active_t(w) {
                        e.constant_prop_layer(0, prop_id)
                    } else {
                        None
                    }
                }
                _ => {}
            },
            LayerIds::One(layer_id) => {
                return if e.additions(*layer_id).active_t(w) {
                    e.constant_prop_layer(*layer_id, prop_id)
                } else {
                    None
                }
            }
            _ => {}
        };
        let mut values = e
            .constant_prop_iter(layer_ids, prop_id)
            .filter_map(|(layer, v)| {
                e.additions(layer)
                    .active_t(w.clone())
                    .then(|| (view.get_layer_name(layer), v))
            })
            .peekable();
        if values.peek().is_some() {
            Some(Prop::map(values))
        } else {
            None
        }
    }
}
