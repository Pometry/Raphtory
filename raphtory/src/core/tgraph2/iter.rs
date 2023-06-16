use std::rc::Rc;

use itertools::Merge;

use crate::{core::Direction, db::graph::Graph};

use super::{
    edge_store::EdgeStore,
    tgraph::TGraph,
    tgraph_storage::{GraphEntry, LockedGraphStorage},
    GraphItem, VRef, EID, VID,
};

pub struct Paged<'a, const N: usize, L: lock_api::RawRwLock, E> {
    guard: Rc<VRef<'a, N, L>>,
    data: Vec<(VID, EID)>,
    i: usize,
    size: usize,
    dir: Direction,
    layer_id: usize,
    src: VID,
    graph: &'a TGraph<N, L>,
    _e: std::marker::PhantomData<E>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock, E> Paged<'a, N, L, E> {
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
            _e: std::marker::PhantomData,
        }
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock, E: GraphItem<'a, N, L>> Iterator
    for Paged<'a, N, L, E>
{
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(t) = self.data.get(self.i) {
            self.i += 1;
            let edge = GraphItem::from_edge_ids(self.src, t.0, t.1, self.dir, self.graph);
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
            return Some(GraphItem::from_edge_ids(
                self.src,
                self.data[0].0,
                self.data[0].1,
                self.dir,
                self.graph,
            ));
        }
    }
}

pub enum PagedIter<'a, const N: usize, L: lock_api::RawRwLock, E: GraphItem<'a, N, L>> {
    Page(Paged<'a, N, L, E>),
    Merged(Merge<Paged<'a, N, L, E>, Paged<'a, N, L, E>>),
}

impl<'a, const N: usize, L: lock_api::RawRwLock, E: GraphItem<'a, N, L> + PartialOrd> Iterator
    for PagedIter<'a, N, L, E>
{
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PagedIter::Page(p) => p.next(),
            PagedIter::Merged(c) => c.next(),
        }
    }
}

pub struct LockedPaged<'a, T, const N: usize, L: lock_api::RawRwLock> {
    locked_gs: Rc<LockedGraphStorage<'a, N, L>>,
    data: Vec<(VID, EID)>,
    size: usize,
    i: usize,
    dir: Direction,
    layer_id: usize,
    src: VID,
    _t: std::marker::PhantomData<T>,
}

impl<'a, T, const N: usize, L: lock_api::RawRwLock> LockedPaged<'a, T, N, L> {
    pub(crate) fn new(
        locked_gs: Rc<LockedGraphStorage<'a, N, L>>,
        dir: Direction,
        layer_id: usize,
        src: VID,
    ) -> Self {
        LockedPaged {
            locked_gs,
            data: Vec::new(),
            size: 16,
            i: 0,
            dir,
            layer_id,
            src,
            _t: std::marker::PhantomData,
        }
    }
}

// Iterator impl
impl<'a, const N: usize, L: lock_api::RawRwLock> Iterator for LockedPaged<'a, EdgeStore<N>, N, L> {
    type Item = GraphEntry<'a, EdgeStore<N>, L, N>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, e_id)) = self.data.get(self.i) {
            self.i += 1;
            let eid: usize = (*e_id).into();
            let edge = GraphEntry::new(self.locked_gs.clone(), eid);
            return Some(edge);
        }

        if let Some((v_id, _)) = self.data.last() {
            self.data = self.locked_gs.edges_from_last(
                self.i,
                self.layer_id,
                self.dir,
                Some(*v_id),
                self.size,
            )
        } else {
            // fetch the first page
            self.data =
                self.locked_gs
                    .edges_from_last(self.i, self.layer_id, self.dir, None, self.size)
        }

        if self.data.is_empty() {
            return None;
        } else {
            self.i = 1;
            return Some(GraphEntry::new(
                self.locked_gs.clone(),
                self.data[0].1.into(),
            ));
        }
    }
}
