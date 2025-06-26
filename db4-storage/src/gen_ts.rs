use std::{borrow::Borrow, ops::Range};

use either::Either;
use itertools::Itertools;
use raphtory_core::{
    entities::LayerIds,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::utils::Iter4;

// TODO: split the Node time operations into edge additions and property additions
#[derive(Clone, Copy)]
pub struct GenericTimeOps<'a, Ref> {
    range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    layer_id: Either<&'a LayerIds, usize>,
    node: Ref,
}

impl<'a, Ref> GenericTimeOps<'a, Ref> {
    pub fn new(node: Ref, layer_id: &'a LayerIds) -> Self {
        Self {
            range: None,
            layer_id: Either::Left(layer_id),
            node,
        }
    }

    pub fn new_with_layer(node: Ref, layer_id: usize) -> Self {
        Self {
            range: None,
            layer_id: Either::Right(layer_id),
            node,
        }
    }
}

pub trait WithTimeCells<'a>: Copy + Clone + Send + Sync
where
    Self: 'a,
{
    type TimeCell: TimeIndexOps<'a, IndexType = TimeIndexEntry>;

    fn layer_time_cells(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a;
    fn num_layers(&self) -> usize;

    fn time_cells<B: Borrow<LayerIds>>(
        self,
        layer_ids: B,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        match layer_ids.borrow() {
            LayerIds::None => Iter4::I(std::iter::empty()),
            LayerIds::One(layer_id) => Iter4::J(self.layer_time_cells(*layer_id, range)),
            LayerIds::All => Iter4::K(
                (0..self.num_layers())
                    .flat_map(move |layer_id| self.layer_time_cells(layer_id, range)),
            ),
            LayerIds::Multiple(layers) => Iter4::L(
                layers
                    .clone()
                    .into_iter()
                    .flat_map(move |layer_id| self.layer_time_cells(layer_id, range)),
            ),
        }
    }

    fn into_iter<B: Borrow<LayerIds>>(
        self,
        layer_ids: B,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells(layer_ids, range);
        iters.map(|cell| cell.iter()).kmerge()
    }

    fn into_iter_rev<B: Borrow<LayerIds>>(
        self,
        layer_ids: B,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells(layer_ids, range);
        iters.map(|cell| cell.iter_rev()).kmerge_by(|a, b| a > b)
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> GenericTimeOps<'a, Ref> {
    pub fn time_cells(self) -> impl Iterator<Item = Ref::TimeCell> + 'a {
        match self.layer_id {
            Either::Left(layer_ids) => Either::Left(self.node.time_cells(layer_ids, self.range)),
            Either::Right(layer_id) => {
                Either::Right(self.node.layer_time_cells(layer_id, self.range))
            }
        }
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> TimeIndexOps<'a> for GenericTimeOps<'a, Ref> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.time_cells().any(|t_cell| t_cell.active(w.clone()))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        GenericTimeOps {
            range: Some((w.start, w.end)),
            node: self.node,
            layer_id: self.layer_id,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        Iterator::min(self.time_cells().filter_map(|t_cell| t_cell.first()))
    }

    fn last(&self) -> Option<Self::IndexType> {
        Iterator::max(self.time_cells().filter_map(|t_cell| t_cell.last()))
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self.layer_id {
            Either::Left(layer_id) => Either::Left(self.node.into_iter(layer_id, self.range)),
            Either::Right(layer_id) => {
                Either::Right(self.node.into_iter(LayerIds::One(layer_id), self.range))
            }
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self.layer_id {
            Either::Left(layer_id) => Either::Left(self.node.into_iter_rev(layer_id, self.range)),
            Either::Right(layer_id) => {
                Either::Right(self.node.into_iter_rev(LayerIds::One(layer_id), self.range))
            }
        }
    }

    fn len(&self) -> usize {
        self.time_cells().map(|t_cell| t_cell.len()).sum()
    }
}
