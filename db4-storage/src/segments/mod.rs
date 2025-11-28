use super::properties::{Properties, RowEntry};
use crate::{LocalPOS, error::StorageError};
use raphtory_api::core::{
    entities::properties::{meta::Meta, prop::Prop},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{
        ELID,
        properties::{tcell::TCell, tprop::TPropCell},
    },
    storage::timeindex::TimeIndexEntry,
};
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    iter,
    sync::Arc,
};

pub mod edge;
pub mod graph_prop;
pub mod node;

pub mod additions;

pub type PageIndexT = u32;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PageIndexEntry(PageIndexT);

impl Default for PageIndexEntry {
    fn default() -> Self {
        PageIndexEntry(PageIndexT::MAX)
    }
}

impl PageIndexEntry {
    fn index(self) -> Option<usize> {
        (self.0 != PageIndexT::MAX).then_some(self.0 as usize)
    }

    fn is_filled(self) -> bool {
        self.0 != PageIndexT::MAX
    }
}

#[derive(Default)]
struct PageIndex(Vec<PageIndexEntry>);

impl PageIndex {
    fn get(&self, pos: LocalPOS) -> Option<usize> {
        self.0.get(pos.as_index()).and_then(|index| index.index())
    }

    fn set(&mut self, pos: LocalPOS, index: PageIndexEntry) {
        let pos_index = pos.as_index();
        if pos_index >= self.0.len() {
            self.0.resize(pos_index + 1, PageIndexEntry::default());
        }
        self.0[pos_index] = index;
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = Option<usize>> {
        self.0.iter().map(|i| i.index())
    }

    fn filled_positions(&self) -> impl Iterator<Item = LocalPOS> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.is_filled().then_some(LocalPOS::from(i)))
    }

    fn par_iter(&self) -> impl IndexedParallelIterator<Item = Option<usize>> {
        self.0.par_iter().map(|i| i.index())
    }
}

#[derive(Default)]
struct SparseVec<T> {
    index: PageIndex,
    data: Vec<(LocalPOS, T)>,
    max_local_pos: Option<LocalPOS>,
}

impl<T: Debug> Debug for SparseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter_filled()).finish()
    }
}

impl<T> SparseVec<T> {
    fn get(&self, pos: LocalPOS) -> Option<&T> {
        self.index
            .get(pos)
            .and_then(|i| self.data.get(i).map(|(_, x)| x))
    }

    fn is_filled(&self, pos: LocalPOS) -> bool {
        self.index.get(pos).is_some()
    }

    /// Iterator over filled positions.
    ///
    /// Note that this returns items in insertion order!
    fn iter_filled(&self) -> impl Iterator<Item = (LocalPOS, &T)> {
        self.data.iter().map(|(i, x)| (*i, x))
    }

    fn iter_all(&self) -> impl ExactSizeIterator<Item = Option<&T>> {
        self.index.iter().map(|i| i.map(|i| &self.data[i].1))
    }

    fn max_local_pos(&self) -> Option<LocalPOS> {
        self.max_local_pos
    }

    fn num_filled(&self) -> usize {
        self.data.len()
    }
}

impl<T: Send + Sync> SparseVec<T> {
    /// Parallel iterator over filled positions.
    ///
    /// Note that this returns items in insertion order!
    fn par_iter_filled(&self) -> impl IndexedParallelIterator<Item = (LocalPOS, &T)> {
        self.data.par_iter().map(|(i, x)| (*i, x))
    }
    fn par_iter_all(&self) -> impl IndexedParallelIterator<Item = Option<&T>> {
        self.index.par_iter().map(|i| i.map(|i| &self.data[i].1))
    }
}

impl<T: HasRow> SparseVec<T> {
    fn get_or_new(&mut self, pos: LocalPOS) -> MaybeNew<&mut T> {
        match self.index.get(pos) {
            None => {
                let next_index = self.data.len();
                self.data.push((pos, T::default()));
                let new_entry = &mut self.data[next_index].1;
                *new_entry.row_mut() = next_index;
                self.index.set(pos, PageIndexEntry(next_index as u32));
                self.max_local_pos = self.max_local_pos.max(Some(pos));
                MaybeNew::New(new_entry)
            }
            Some(i) => MaybeNew::Existing(&mut self.data[i].1),
        }
    }
}

#[derive(Debug)]
pub struct SegmentContainer<T> {
    segment_id: usize,
    data: SparseVec<T>,
    max_page_len: u32,
    properties: Properties,
    meta: Arc<Meta>,
    lsn: u64,
}

pub trait HasRow: Default + Send + Sync + Sized {
    fn row(&self) -> usize;

    fn row_mut(&mut self) -> &mut usize;
}

impl<T: HasRow> SegmentContainer<T> {
    pub fn new(segment_id: usize, max_page_len: u32, meta: Arc<Meta>) -> Self {
        assert!(max_page_len > 0, "max_page_len must be greater than 0");

        Self {
            segment_id,
            data: Default::default(),
            max_page_len,
            properties: Default::default(),
            meta,
            lsn: 0,
        }
    }

    /// Replaces this container with an empty instance, returning the
    /// old container with its data.
    pub fn take(&mut self) -> Self {
        std::mem::replace(
            self,
            Self::new(self.segment_id, self.max_page_len, self.meta.clone()),
        )
    }

    #[inline]
    pub fn est_size(&self) -> usize {
        // TODO: this is a rough estimate and should be improved
        let data_size =
            (self.data.num_filled() as f64 * std::mem::size_of::<T>() as f64 * 1.5) as usize; // Estimate size of data
        let timestamp_size = std::mem::size_of::<TimeIndexEntry>();
        (self.properties.additions_count * timestamp_size)
            + data_size
            + self.t_prop_est_size()
            + self.c_prop_est_size()
    }

    pub fn get(&self, item_pos: LocalPOS) -> Option<&T> {
        self.data.get(item_pos)
    }

    pub fn has_item(&self, item_pos: LocalPOS) -> bool {
        self.data.is_filled(item_pos)
    }

    pub fn max_page_len(&self) -> u32 {
        self.max_page_len
    }

    pub fn max_rows(&self) -> usize {
        self.data.max_local_pos().map(|pos| pos.0 + 1).unwrap_or(0) as usize
    }

    pub fn is_full(&self) -> bool {
        self.data.num_filled() == self.max_page_len() as usize
    }

    pub fn t_len(&self) -> usize {
        self.properties.t_len()
    }

    /// Reserves a local row for the given item position.
    /// If the item position already exists, it returns a mutable reference to the existing item.
    /// Left variant indicates that the item was already present,
    /// Right variant indicates that a new item was created.
    pub(crate) fn reserve_local_row(&mut self, item_pos: LocalPOS) -> MaybeNew<&mut T> {
        self.data.get_or_new(item_pos)
    }

    #[inline]
    pub fn t_prop_est_size(&self) -> usize {
        let row_size = self.meta.temporal_est_row_size();
        let row_count = self.properties.t_len();

        row_size * row_count
    }

    pub(crate) fn c_prop_est_size(&self) -> usize {
        self.meta.const_est_row_size() * self.len() as usize
    }

    pub fn properties(&self) -> &Properties {
        &self.properties
    }

    pub fn properties_mut(&mut self) -> &mut Properties {
        &mut self.properties
    }

    pub fn check_metadata(
        &self,
        local_pos: LocalPOS,
        props: &[(usize, Prop)],
    ) -> Result<(), StorageError> {
        if let Some(item) = self.get(local_pos) {
            let local_row = item.row();
            let edge_properties = self.properties().get_entry(local_row);
            for (prop_id, prop_val) in props {
                edge_properties.check_metadata(*prop_id, prop_val)?;
            }
        }
        Ok(())
    }

    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }

    pub fn filled_positions(&self) -> impl Iterator<Item = LocalPOS> {
        self.data.index.filled_positions()
    }

    pub fn filled_positions_par(&self) -> impl ParallelIterator<Item = LocalPOS> {
        self.data.par_iter_filled().map(|(i, _)| i)
    }

    #[inline(always)]
    pub fn segment_id(&self) -> usize {
        self.segment_id
    }

    #[inline(always)]
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    #[inline(always)]
    pub fn set_lsn(&mut self, lsn: u64) {
        self.lsn = lsn;
    }

    pub fn len(&self) -> u32 {
        self.data.data.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.data.data.is_empty()
    }

    /// returns items in insertion order!
    pub fn row_entries(&self) -> impl Iterator<Item = (LocalPOS, &T, RowEntry<'_>)> {
        self.data
            .iter_filled()
            .map(|(l_pos, entry)| (l_pos, entry, self.properties().get_entry(entry.row())))
    }

    /// return filled entries ordered by index
    pub fn row_entries_ordered(&self) -> impl Iterator<Item = (LocalPOS, &T, RowEntry<'_>)> {
        self.all_entries().filter_map(|(pos, entry)| {
            let (v, row) = entry?;
            Some((pos, v, row))
        })
    }

    pub fn all_entries(&self) -> impl Iterator<Item = (LocalPOS, Option<(&T, RowEntry<'_>)>)> {
        let max_local_pos = self.data.max_local_pos().map(|p| p.0 as usize).unwrap_or(0);
        self.data
            .iter_all()
            .chain(iter::repeat(None))
            .take(max_local_pos + 1)
            .enumerate()
            .map(|(i, v)| {
                (
                    LocalPOS::from(i),
                    v.map(|v| (v, self.properties().get_entry(v.row()))),
                )
            })
    }

    pub fn all_entries_par(
        &self,
    ) -> impl ParallelIterator<Item = (LocalPOS, Option<(&T, RowEntry<'_>)>)> + '_ {
        self.data.par_iter_all().enumerate().map(|(i, v)| {
            (
                LocalPOS::from(i),
                v.map(|entry| (entry, self.properties().get_entry(entry.row()))),
            )
        })
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        self.properties.earliest()
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        self.properties.latest()
    }

    pub fn temporal_index(&self) -> Vec<usize> {
        self.row_entries_ordered()
            .flat_map(|(_, mp, _)| {
                let row = mp.row();
                self.properties()
                    .times_from_props(row)
                    .into_iter()
                    .flat_map(|entry| entry.iter())
                    .filter_map(|(_, &v)| v)
            })
            .collect::<Vec<_>>()
    }

    pub fn t_prop(&self, item_id: impl Into<LocalPOS>, prop_id: usize) -> Option<TPropCell<'_>> {
        let item_id = item_id.into();
        self.data.get(item_id).and_then(|entry| {
            let prop_entry = self.properties.get_entry(entry.row());
            prop_entry.prop(prop_id)
        })
    }

    pub fn t_prop_rows(&self, item_id: impl Into<LocalPOS>) -> &TCell<Option<usize>> {
        let item_id = item_id.into();
        self.data
            .get(item_id)
            .map(|entry| {
                let prop_entry = self.properties.get_entry(entry.row());
                prop_entry.t_cell()
            })
            .unwrap_or(&TCell::Empty)
    }

    pub fn c_prop(&self, item_id: impl Into<LocalPOS>, prop_id: usize) -> Option<Prop> {
        let item_id = item_id.into();
        self.data.get(item_id).and_then(|entry| {
            let prop_entry = self.properties.c_column(prop_id)?;
            prop_entry.get(entry.row())
        })
    }

    pub fn c_prop_str(&self, item_id: impl Into<LocalPOS>, prop_id: usize) -> Option<&str> {
        let item_id = item_id.into();
        self.data.get(item_id).and_then(|entry| {
            let prop_entry = self.properties.c_column(prop_id)?;
            prop_entry
                .get_ref(entry.row())
                .and_then(|prop| prop.as_str())
        })
    }

    pub fn additions(&self, item_pos: LocalPOS) -> &TCell<ELID> {
        self.data
            .get(item_pos)
            .and_then(|entry| self.properties.additions(entry.row()))
            .unwrap_or(&TCell::Empty)
    }

    pub fn deletions(&self, item_pos: LocalPOS) -> &TCell<ELID> {
        self.data
            .get(item_pos)
            .and_then(|entry| self.properties.deletions(entry.row()))
            .unwrap_or(&TCell::Empty)
    }

    pub fn times_from_props(&self, item_pos: LocalPOS) -> &TCell<Option<usize>> {
        self.data
            .get(item_pos)
            .and_then(|entry| self.properties.times_from_props(entry.row()))
            .unwrap_or(&TCell::Empty)
    }
}
