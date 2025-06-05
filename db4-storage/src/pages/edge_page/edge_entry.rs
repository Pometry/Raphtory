use std::{
    ops::{Deref, Range},
    sync::Arc,
};

use crate::{avoid_k_merge_with_iterators, LocalPOS};
use arc_swap::Guard;
use parking_lot::RwLockReadGuard;
use raphtory::{
    core::storage::timeindex::{TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps},
    db::api::storage::graph::tprop_storage_ops::TPropOps,
    prelude::Prop,
};
use raphtory_api::core::entities::{EID, VID};
use raphtory_api::iter::BoxedLIter;

use super::{
    edge_page_view::{EdgePageView, TProp, TimeCell},
    frozen_pages::{DiskEdgeSegments, ExactSizeChain},
    mem_edge_page::MemEdgeSegment,
    EdgeSegment,
};

#[derive(Debug, Clone, Copy)]
pub struct EdgeEntry<'a, MP: 'a, DP: 'a> {
    edge_pos: LocalPOS,
    mem_seg: Option<MP>,
    disk_pages: DP,
    seg: &'a EdgeSegment,
}

pub type EdgeStorageRef<'a> = EdgeEntry<'a, &'a MemEdgeSegment, &'a DiskEdgeSegments>;

impl<'a, MP: 'a, DP: 'a> EdgeEntry<'a, MP, DP> {
    pub fn page_id(&self) -> usize {
        self.seg.page_id()
    }
}

impl<'a> EdgeEntry<'a, RwLockReadGuard<'a, MemEdgeSegment>, Guard<Arc<DiskEdgeSegments>>> {
    pub fn read(edge_pos: LocalPOS, page: &'a EdgeSegment) -> Self {
        let mem_page = (!page.head_is_empty()).then(|| page.head.read());
        let disk_pages = page.immut();
        Self {
            edge_pos,
            mem_seg: mem_page,
            disk_pages,
            seg: page,
        }
    }

    pub fn as_ref(&'a self) -> EdgeStorageRef<'a> {
        let disk_pages = self.disk_pages.deref().as_ref();
        EdgeStorageRef {
            edge_pos: self.edge_pos,
            mem_seg: self.mem_seg.as_deref(),
            disk_pages,
            seg: self.seg,
        }
    }
}

impl<'a> EdgeStorageRef<'a> {
    pub fn eid(self) -> EID {
        self.edge_pos
            .as_eid(self.page_id(), self.disk_pages.max_page_len())
    }

    pub fn edge(self) -> (VID, VID) {
        let edge_pos = self.edge_pos;
        self.mem_seg
            .and_then(|mp| mp.get_edge(edge_pos))
            .or_else(|| self.disk_pages.get_edge(edge_pos))
            .expect("Internal error: edge not found")
    }

    fn pages(self) -> impl ExactSizeIterator<Item = &'a dyn EdgePageView> {
        ExactSizeChain::new(
            self.mem_seg.into_iter().map(|p| p as &dyn EdgePageView),
            self.disk_pages.iter(),
        )
    }

    pub fn additions(self) -> EdgeAdditions<'a> {
        EdgeAdditions {
            edge: self,
            range: None,
        }
    }

    pub fn t_prop(self, prop_id: usize) -> EdgeTProps<'a> {
        EdgeTProps {
            edge: self,
            prop_id,
        }
    }

    pub fn c_prop(self, prop_id: usize) -> Option<Prop> {
        self.mem_seg
            .and_then(|mp| mp.as_ref().c_prop(self.edge_pos, prop_id))
            .or_else(|| self.disk_pages.c_prop(self.edge_pos, prop_id))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeAdditions<'a> {
    range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    edge: EdgeStorageRef<'a>,
}

impl<'a> EdgeAdditions<'a> {
    pub fn time_cells(self) -> impl ExactSizeIterator<Item = TimeCell<'a>> + 'a {
        let edge_pos = self.edge.edge_pos;
        self.edge
            .pages()
            .map(move |page| page.additions(edge_pos))
            .map(move |tc| match self.range {
                Some((start, end)) => tc.into_range(start..end),
                None => tc,
            })
    }

    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + 'a {
        let iters = self.time_cells();
        avoid_k_merge_with_iterators(iters, |t_cell| t_cell.into_iter(), |a, b| a < b)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeTProps<'a> {
    edge: EdgeStorageRef<'a>,
    prop_id: usize,
}

impl<'a> EdgeTProps<'a> {
    fn tprops(self, prop_id: usize) -> impl ExactSizeIterator<Item = TProp<'a>> + 'a {
        let edge_pos = self.edge.edge_pos;
        self.edge
            .pages()
            .map(move |page| page.t_prop(edge_pos, prop_id))
    }
}

impl<'a> TPropOps<'a> for EdgeTProps<'a> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        self.tprops(self.prop_id)
            .map(|t_props| t_props.last_before(t))
            .flatten()
            .max_by_key(|(t, _)| *t)
    }

    fn iter_inner(
        self,
        w: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let w = w.map(|w| (w.start, w.end));
        avoid_k_merge_with_iterators(
            self.tprops(self.prop_id),
            move |t_cell| t_cell.iter_inner(w.map(|(start, end)| start..end)),
            |a, b| a < b,
        )
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let w = range.map(|r| (r.start, r.end));
        avoid_k_merge_with_iterators(
            self.tprops(self.prop_id),
            move |t_cell| t_cell.iter_inner_rev(w.map(|(start, end)| start..end)),
            |a, b| a > b,
        )
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        self.tprops(self.prop_id)
            .flat_map(|t_props| t_props.at(ti))
            .next() //TODO: need to figure out how to handle this
    }
}

// FIXME: write some tests with windows, this is wrong in a the current state
impl TimeIndexOps for EdgeAdditions<'_> {
    type IndexType = TimeIndexEntry;

    type RangeType<'b>
        = EdgeAdditions<'b>
    where
        Self: 'b;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.time_cells().any(|tc| tc.active(w.clone()))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        EdgeAdditions {
            edge: self.edge,
            range: Some((w.start, w.end)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        self.time_cells().filter_map(|tc| tc.first()).min()
    }

    fn last(&self) -> Option<Self::IndexType> {
        self.time_cells().filter_map(|tc| tc.last()).max()
    }

    fn iter(&self) -> BoxedLIter<Self::IndexType> {
        Box::new(self.into_iter())
    }

    fn len(&self) -> usize {
        self.time_cells().map(|tc| tc.len()).sum()
    }
}

impl TimeIndexIntoOps for EdgeAdditions<'_> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn into_range(self, w: Range<Self::IndexType>) -> Self::RangeType {
        EdgeAdditions {
            edge: self.edge,
            range: Some((w.start, w.end)),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync {
        self.into_iter()
    }
}
