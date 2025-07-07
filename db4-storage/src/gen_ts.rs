use std::ops::Range;

use itertools::Itertools;
use raphtory_core::{
    entities::{ELID, LayerIds},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};

use crate::{NodeEntryRef, segments::additions::MemAdditions, utils::Iter2};

#[derive(Clone, Copy, Debug)]
pub enum LayerIter<'a> {
    One(usize),
    LRef(&'a LayerIds),
}

pub static ALL_LAYERS: LayerIter<'static> = LayerIter::LRef(&LayerIds::All);

impl<'a> LayerIter<'a> {
    pub fn into_iter(self, num_layers: usize) -> impl Iterator<Item = usize> + 'a {
        match self {
            LayerIter::One(id) => Iter2::I1(std::iter::once(id)),
            LayerIter::LRef(layers) => Iter2::I2(layers.iter(num_layers)),
        }
    }
}

impl From<usize> for LayerIter<'_> {
    fn from(id: usize) -> Self {
        LayerIter::One(id)
    }
}

impl<'a> From<&'a LayerIds> for LayerIter<'a> {
    fn from(layers: &'a LayerIds) -> Self {
        LayerIter::LRef(layers)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GenericTimeOps<'a, Ref> {
    range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    layer_id: LayerIter<'a>,
    node: Ref,
}

impl<'a, Ref> GenericTimeOps<'a, Ref> {
    pub fn new_with_layer(node: Ref, layer_id: impl Into<LayerIter<'a>>) -> Self {
        Self {
            range: None,
            layer_id: layer_id.into(),
            node,
        }
    }

    pub fn new_additions_with_layer(node: Ref, layer_id: impl Into<LayerIter<'a>>) -> Self {
        Self {
            range: None,
            layer_id: layer_id.into(),
            node,
        }
    }
}

pub trait WithTimeCells<'a>: Copy + Clone + Send + Sync + std::fmt::Debug
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

    fn deletions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a;

    fn num_layers(&self) -> usize;
}

pub trait WithEdgeEvents<'a>: WithTimeCells<'a> {
    type TimeCell: EdgeEventOps<'a>;
}

impl<'a> WithEdgeEvents<'a> for NodeEntryRef<'a> {
    type TimeCell = MemAdditions<'a>;
}

pub trait EdgeEventOps<'a>: TimeIndexOps<'a, IndexType = TimeIndexEntry> {
    fn edge_events(self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + Send + Sync + 'a;
    fn edge_events_rev(self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + Send + Sync + 'a;
}

#[derive(Clone, Copy, Debug)]
pub struct AdditionCellsRef<'a, Ref: WithTimeCells<'a> + 'a> {
    node: Ref,
    _mark: std::marker::PhantomData<&'a ()>,
}

impl<'a, Ref: WithTimeCells<'a> + 'a> AdditionCellsRef<'a, Ref> {
    pub fn new(node: Ref) -> Self {
        Self {
            node,
            _mark: std::marker::PhantomData,
        }
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> WithTimeCells<'a> for AdditionCellsRef<'a, Ref> {
    type TimeCell = Ref::TimeCell;

    fn t_props_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        self.node.t_props_tc(layer_id, range) // Assuming t_props_tc is not used for additions
    }

    fn additions_tc(
        self,
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn deletions_tc(
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

#[derive(Clone, Copy, Debug)]
pub struct DeletionCellsRef<'a, Ref: WithTimeCells<'a> + 'a> {
    node: Ref,
    _mark: std::marker::PhantomData<&'a ()>,
}

impl<'a, Ref: WithTimeCells<'a> + 'a> DeletionCellsRef<'a, Ref> {
    pub fn new(node: Ref) -> Self {
        Self {
            node,
            _mark: std::marker::PhantomData,
        }
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> WithTimeCells<'a> for DeletionCellsRef<'a, Ref> {
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
        _layer_id: usize,
        _range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        std::iter::empty()
    }

    fn deletions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        self.node.deletions_tc(layer_id, range)
    }

    fn num_layers(&self) -> usize {
        self.node.num_layers()
    }
}

#[derive(Clone, Copy, Debug)]
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

    fn deletions_tc(
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

#[derive(Clone, Copy, Debug)]
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

    fn deletions_tc(
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

impl<'a, Ref: WithEdgeEvents<'a> + 'a> GenericTimeOps<'a, EdgeAdditionCellsRef<'a, Ref>>
where
    <Ref as WithTimeCells<'a>>::TimeCell: EdgeEventOps<'a>,
{
    pub fn edge_events(self) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + Send + Sync + 'a {
        self.layer_id
            .into_iter(self.node.num_layers())
            .flat_map(|layer_id| {
                self.node
                    .additions_tc(layer_id, self.range)
                    .map(|t_cell| t_cell.edge_events())
            })
            .kmerge_by(|a, b| a < b)
    }

    pub fn edge_events_rev(
        self,
    ) -> impl Iterator<Item = (TimeIndexEntry, ELID)> + Send + Sync + 'a {
        self.layer_id
            .into_iter(self.node.num_layers())
            .flat_map(|layer_id| {
                self.node
                    .additions_tc(layer_id, self.range)
                    .map(|t_cell| t_cell.edge_events_rev())
            })
            .kmerge_by(|a, b| a > b)
    }
}

impl<'a, Ref: WithTimeCells<'a> + 'a> GenericTimeOps<'a, Ref> {
    pub fn time_cells(self) -> impl Iterator<Item = Ref::TimeCell> + 'a {
        let range = self.range;
        self.layer_id
            .into_iter(self.node.num_layers())
            .flat_map(move |layer_id| {
                self.node.t_props_tc(layer_id, range).chain(
                    self.node
                        .additions_tc(layer_id, range)
                        .chain(self.node.deletions_tc(layer_id, range)),
                )
            })
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
