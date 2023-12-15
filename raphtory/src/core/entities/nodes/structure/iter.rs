use crate::core::{
    entities::{edges::edge::EdgeView, graph::tgraph::TGraph, VRef, EID, VID},
    Direction,
};
use itertools::Merge;
use std::sync::Arc;

pub struct Paged<'a, const N: usize> {
    guard: Arc<VRef<'a, N>>,
    data: Vec<(VID, EID)>,
    i: usize,
    size: usize,
    dir: Direction,
    layer_id: usize,
    src: VID,
    graph: &'a TGraph<N>,
}

impl<'a, const N: usize> Paged<'a, N> {
    pub(crate) fn new(
        guard: Arc<VRef<'a, N>>,
        dir: Direction,
        layer_id: usize,
        src: VID,
        graph: &'a TGraph<N>,
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

impl<'a, const N: usize> Iterator for Paged<'a, N> {
    type Item = EdgeView<'a, N>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(t) = self.data.get(self.i) {
            self.i += 1;
            let e_id = self.guard.edge_ref(t.1, self.graph);
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
            let e_id = self.guard.edge_ref(self.data[0].1, self.graph);
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

pub enum PagedIter<'a, const N: usize> {
    Page(Paged<'a, N>),
    Merged(Merge<Paged<'a, N>, Paged<'a, N>>),
}

impl<'a, const N: usize> Iterator for PagedIter<'a, N> {
    type Item = EdgeView<'a, N>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PagedIter::Page(p) => p.next(),
            PagedIter::Merged(c) => c.next(),
        }
    }
}
