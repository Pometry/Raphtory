use std::{borrow::Borrow, ops::Range};

use itertools::Itertools;
use raphtory_core::{
    entities::LayerIds,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::utils::Iter4;

#[derive(Clone, Copy)]
pub struct GenericTimeOps<'a, Ref> {
    range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    layer_id: &'a LayerIds,
    node: Ref,
}

impl<'a, Ref> GenericTimeOps<'a, Ref> {
    pub fn new(node: Ref, layer_id: &'a LayerIds) -> Self {
        Self {
            range: None,
            layer_id,
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

    fn time_cells<B: Borrow<LayerIds> + 'a>(
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

    fn into_iter(
        self,
        layer_ids: &'a LayerIds,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells(layer_ids, range);
        iters.map(|cell| cell.iter()).kmerge()
    }

    fn into_iter_rev(
        self,
        layer_ids: &'a LayerIds,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells(layer_ids, range);
        iters.map(|cell| cell.iter_rev()).kmerge_by(|a, b| a > b)
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> TimeIndexOps<'a> for GenericTimeOps<'a, Ref> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.node
            .time_cells(self.layer_id, self.range)
            .any(|t_cell| t_cell.active(w.clone()))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        GenericTimeOps {
            range: Some((w.start, w.end)),
            node: self.node,
            layer_id: self.layer_id,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        Iterator::min(
            self.node
                .time_cells(self.layer_id, self.range)
                .filter_map(|t_cell| t_cell.first()),
        )
    }

    fn last(&self) -> Option<Self::IndexType> {
        Iterator::max(
            self.node
                .time_cells(self.layer_id, self.range)
                .filter_map(|t_cell| t_cell.last()),
        )
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.node.into_iter(self.layer_id, self.range)
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.node.into_iter_rev(self.layer_id, self.range)
    }

    fn len(&self) -> usize {
        self.node
            .time_cells(self.layer_id, self.range)
            .map(|t_cell| t_cell.len())
            .sum()
    }
}
