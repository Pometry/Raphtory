use std::ops::Range;

use raphtory_core::{
    entities::{ELID, properties::tcell::TCell},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps, TimeIndexWindow},
};

use crate::utils::Iter4;

#[derive(Clone, Debug)]
pub enum MemAdditions<'a> {
    Edges(&'a TCell<ELID>),
    Props(&'a TCell<Option<usize>>),
    WEdges(TimeIndexWindow<'a, TimeIndexEntry, TCell<ELID>>),
    WProps(TimeIndexWindow<'a, TimeIndexEntry, TCell<Option<usize>>>),
}

impl<'a> From<&'a TCell<ELID>> for MemAdditions<'a> {
    fn from(edges: &'a TCell<ELID>) -> Self {
        MemAdditions::Edges(edges)
    }
}

impl<'a> From<&'a TCell<Option<usize>>> for MemAdditions<'a> {
    fn from(props: &'a TCell<Option<usize>>) -> Self {
        MemAdditions::Props(props)
    }
}

impl<'a> TimeIndexOps<'a> for MemAdditions<'a> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        match self {
            MemAdditions::Props(props) => props.active(w),
            MemAdditions::Edges(edges) => edges.active(w),
            MemAdditions::WProps(window) => window.active(w),
            MemAdditions::WEdges(window) => window.active(w),
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        match self {
            MemAdditions::Props(props) => MemAdditions::WProps(props.range(w)),
            MemAdditions::Edges(edges) => MemAdditions::WEdges(edges.range(w)),
            MemAdditions::WProps(window) => MemAdditions::WProps(window.range(w)),
            MemAdditions::WEdges(window) => MemAdditions::WEdges(window.range(w)),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            MemAdditions::Props(props) => Iter4::I(props.iter().map(|(k, _)| *k)),
            MemAdditions::Edges(edges) => Iter4::J(edges.iter().map(|(k, _)| *k)),
            MemAdditions::WProps(window) => Iter4::K(window.iter()),
            MemAdditions::WEdges(window) => Iter4::L(window.iter()),
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            MemAdditions::Props(props) => Iter4::I(props.iter_rev()),
            MemAdditions::Edges(edges) => Iter4::J(edges.iter_rev()),
            MemAdditions::WProps(window) => Iter4::K(window.iter_rev()),
            MemAdditions::WEdges(window) => Iter4::L(window.iter_rev()),
        }
    }

    fn len(&self) -> usize {
        match self {
            MemAdditions::Props(props) => props.len(),
            MemAdditions::Edges(edges) => edges.len(),
            MemAdditions::WProps(window) => window.len(),
            MemAdditions::WEdges(window) => window.len(),
        }
    }
}
