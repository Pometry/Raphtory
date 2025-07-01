use std::ops::Range;

use itertools::Itertools;
use raphtory_core::storage::timeindex::{TimeIndexEntry, TimeIndexOps};

// TODO: split the Node time operations into edge additions and property additions
#[derive(Clone, Copy)]
pub struct GenericTimeOps<'a, Ref> {
    range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    layer_id: usize,
    node: Ref,
    _mark: std::marker::PhantomData<&'a ()>,
}

impl<'a, Ref> GenericTimeOps<'a, Ref> {
    pub fn new_with_layer(node: Ref, layer_id: usize) -> Self {
        Self {
            range: None,
            layer_id,
            node,
            _mark: std::marker::PhantomData,
        }
    }

    pub fn new_additions_with_layer(node: Ref, layer_id: usize) -> Self {
        Self {
            range: None,
            layer_id,
            node,
            _mark: std::marker::PhantomData,
        }
    }
}

pub trait WithTimeCells<'a>: Copy + Clone + Send + Sync
where
    Self: 'a,
{
    type TimeCell: TimeIndexOps<'a, IndexType = TimeIndexEntry>;

    fn t_props_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a;

    fn additions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a;

    fn num_layers(&self) -> usize;
}

#[derive(Clone, Copy)]
pub struct EdgeAdditionCellsRef<'a, Ref: WithTimeCells<'a> + 'a> {
    node: Ref,
    _mark: std::marker::PhantomData<&'a ()>,
}

impl<'a, Ref: WithTimeCells<'a> + 'a> EdgeAdditionCellsRef<'a, Ref> {
    pub fn new(node: Ref) -> Self {
        Self {
            node,
            _mark: std::marker::PhantomData,
        }
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> WithTimeCells<'a> for EdgeAdditionCellsRef<'a, Ref> {
    type TimeCell = Ref::TimeCell;

    fn t_props_tc(
        self,
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn additions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        self.node.additions_tc(layer_id, range)
    }

    fn num_layers(&self) -> usize {
        self.node.num_layers()
    }
}

#[derive(Clone, Copy)]
pub struct PropAdditionCellsRef<'a, Ref: WithTimeCells<'a> + 'a> {
    node: Ref,
    _mark: std::marker::PhantomData<&'a ()>,
}

impl<'a, Ref: WithTimeCells<'a> + 'a> PropAdditionCellsRef<'a, Ref> {
    pub fn new(node: Ref) -> Self {
        Self {
            node,
            _mark: std::marker::PhantomData,
        }
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> WithTimeCells<'a> for PropAdditionCellsRef<'a, Ref> {
    type TimeCell = Ref::TimeCell;

    fn t_props_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        self.node.t_props_tc(layer_id, range)
    }

    fn additions_tc(
        self,
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn num_layers(&self) -> usize {
        self.node.num_layers()
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> GenericTimeOps<'a, Ref> {
    pub fn time_cells(self) -> impl Iterator<Item = Ref::TimeCell> + 'a {
        let range = self.range;
        let layer_id = self.layer_id;

        let t_cells = self.node.t_props_tc(layer_id, range);
        let a_cells = self.node.additions_tc(layer_id, range);

        t_cells.chain(a_cells)
    }

    fn into_iter(self) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells();
        iters.map(|cell| cell.iter()).kmerge()
    }

    fn into_iter_rev(self) -> impl Iterator<Item = TimeIndexEntry> + Send + Sync + 'a {
        let iters = self.time_cells();
        iters.map(|cell| cell.iter_rev()).kmerge_by(|a, b| a > b)
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
            _mark: std::marker::PhantomData,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        Iterator::min(self.time_cells().filter_map(|t_cell| t_cell.first()))
    }

    fn last(&self) -> Option<Self::IndexType> {
        Iterator::max(self.time_cells().filter_map(|t_cell| t_cell.last()))
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.into_iter()
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.into_iter_rev()
    }

    fn len(&self) -> usize {
        self.time_cells().map(|t_cell| t_cell.len()).sum()
    }
}
