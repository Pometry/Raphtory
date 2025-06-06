use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use raphtory_api::core::{
    entities::ELID,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use raphtory_core::{
    entities::nodes::node_store::PropTimestamps,
    storage::timeindex::{TimeIndexWindow, TimeIndexWindowVariants},
};
use std::{iter, ops::Range};

#[cfg(feature = "storage")]
use {itertools::Itertools, pometry_storage::timestamps::LayerAdditions};

#[derive(Clone, Debug)]
pub enum NodeAdditions<'a> {
    Mem(&'a PropTimestamps),
    Range(TimeIndexWindow<'a, TimeIndexEntry, PropTimestamps>),
    #[cfg(feature = "storage")]
    Col(LayerAdditions<'a>),
}

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Debug)]
pub enum AdditionVariants<Mem, Range, #[cfg(feature = "storage")] Col> {
    Mem(Mem),
    Range(Range),
    #[cfg(feature = "storage")]
    Col(Col),
}

impl<'a> NodeAdditions<'a> {
    #[inline]
    pub fn prop_events(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.props_ts.iter().map(|(t, _)| *t))
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                AdditionVariants::Col(index.clone().prop_events().map(|t| t.into_iter()).kmerge())
            }
        }
    }

    #[inline]
    pub fn prop_events_rev(&self) -> impl Iterator<Item = TimeIndexEntry> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.props_ts.iter().map(|(t, _)| *t).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .props_ts
                        .iter_window(range.clone())
                        .map(|(t, _)| *t)
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.props_ts.iter().map(|(t, _)| *t).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(
                index
                    .clone()
                    .prop_events()
                    .map(|t| t.into_iter().rev())
                    .kmerge_by(|t1, t2| t1 >= t2),
            ),
        }
    }

    #[inline]
    pub fn edge_events(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e)),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)))
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history()),
        }
    }

    #[inline]
    pub fn edge_events_rev(&self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + use<'a> {
        match self {
            NodeAdditions::Mem(index) => {
                AdditionVariants::Mem(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
            }
            NodeAdditions::Range(index) => AdditionVariants::Range(match index {
                TimeIndexWindow::Empty => TimeIndexWindowVariants::Empty(iter::empty()),
                TimeIndexWindow::Range { timeindex, range } => TimeIndexWindowVariants::Range(
                    timeindex
                        .edge_ts
                        .iter_window(range.clone())
                        .map(|(t, e)| (*t, *e))
                        .rev(),
                ),
                TimeIndexWindow::All(index) => {
                    TimeIndexWindowVariants::All(index.edge_ts.iter().map(|(t, e)| (*t, *e)).rev())
                }
            }),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(index.edge_history_rev()),
        }
    }
}

impl<'b> TimeIndexOps<'b> for NodeAdditions<'b> {
    type IndexType = TimeIndexEntry;
    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<TimeIndexEntry>) -> bool {
        match self {
            NodeAdditions::Mem(index) => index.active(w),
            NodeAdditions::Range(index) => index.active(w),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().any(|index| index.active(w.clone())),
        }
    }

    fn range(&self, w: Range<TimeIndexEntry>) -> Self {
        match self {
            NodeAdditions::Mem(index) => NodeAdditions::Range(index.range(w)),
            NodeAdditions::Range(index) => NodeAdditions::Range(index.range(w)),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => NodeAdditions::Col(index.with_range(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.first(),
            NodeAdditions::Range(index) => index.first(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.first()).min(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            NodeAdditions::Mem(index) => index.last(),
            NodeAdditions::Range(index) => index.last(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => index.iter().flat_map(|index| index.last()).max(),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        match self {
            NodeAdditions::Mem(index) => AdditionVariants::Mem(index.iter()),
            NodeAdditions::Range(index) => AdditionVariants::Range(index.iter()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => {
                AdditionVariants::Col(index.iter().map(|index| index.into_iter()).kmerge())
            }
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'b {
        match self {
            NodeAdditions::Mem(index) => AdditionVariants::Mem(index.iter_rev()),
            NodeAdditions::Range(index) => AdditionVariants::Range(index.iter_rev()),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(index) => AdditionVariants::Col(
                index
                    .iter()
                    .map(|index| index.into_iter().rev())
                    .kmerge_by(|lt, rt| lt >= rt),
            ),
        }
    }

    fn len(&self) -> usize {
        match self {
            NodeAdditions::Mem(index) => index.len(),
            NodeAdditions::Range(range) => range.len(),
            #[cfg(feature = "storage")]
            NodeAdditions::Col(col) => col.len(),
        }
    }
}
