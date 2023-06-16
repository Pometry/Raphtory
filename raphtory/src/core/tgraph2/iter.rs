use std::rc::Rc;

use itertools::Merge;

use crate::core::Direction;

use super::{
    edge::EdgeView,
    tgraph::TGraph,
    VRef, EID, VID,
};

pub struct Paged<'a, const N: usize, L: lock_api::RawRwLock> {
    guard: Rc<VRef<'a, N, L>>,
    data: Vec<(VID, EID)>,
    i: usize,
    size: usize,
    dir: Direction,
    layer_id: usize,
    src: VID,
    graph: &'a TGraph<N, L>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Paged<'a, N, L> {
    pub(crate) fn new(
        guard: Rc<VRef<'a, N, L>>,
        dir: Direction,
        layer_id: usize,
        src: VID,
        graph: &'a TGraph<N, L>,
    ) -> Self {
        Paged {
            guard,
            data: Vec::new(),
            i: 0,
            size: 16,
            dir,
            layer_id,
            src,
            graph,
        }
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> Iterator for Paged<'a, N, L> {
    type Item = EdgeView<'a, N, L>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(t) = self.data.get(self.i) {
            self.i += 1;
            let e_id = self.guard.edge_ref(t.1);
            let edge = EdgeView::from_edge_ids(self.src, t.0, e_id, self.dir, self.graph);
            return Some(edge);
        }

        if let Some(last) = self.data.last() {
            self.data = self
                .guard
                .edges_from_last(self.layer_id, self.dir, Some(last.0), self.size)
        } else {
            // fetch the first page
            self.data = self
                .guard
                .edges_from_last(self.layer_id, self.dir, None, self.size)
        }

        if self.data.is_empty() {
            return None;
        } else {
            self.i = 1;
            let e_id = self.guard.edge_ref(self.data[0].1);
            return Some(EdgeView::from_edge_ids(
                self.src,
                self.data[0].0,
                e_id,
                self.dir,
                self.graph,
            ));
        }
    }
}

pub enum PagedIter<'a, const N: usize, L: lock_api::RawRwLock + 'static> {
    Page(Paged<'a, N, L>),
    Merged(Merge<Paged<'a, N, L>, Paged<'a, N, L>>),
}

impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> Iterator for PagedIter<'a, N, L> {
    type Item = EdgeView<'a, N, L>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PagedIter::Page(p) => p.next(),
            PagedIter::Merged(c) => c.next(),
        }
    }
}
