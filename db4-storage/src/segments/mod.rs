use std::{collections::hash_map::Entry, fmt::Debug, sync::Arc};

use bitvec::{order::Msb0, vec::BitVec};
use either::Either;
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop};
use raphtory_core::{
    entities::{
        nodes::node_store::PropTimestamps,
        properties::{tcell::TCell, tprop::TPropCell},
    },
    storage::timeindex::TimeIndexEntry,
};
use rustc_hash::FxHashMap;

use crate::LocalPOS;

use super::properties::{PropEntry, Properties};

pub mod edge;
pub mod node;

pub mod additions;
pub mod edge_entry;
pub mod node_entry;

pub struct SegmentContainer<T> {
    segment_id: usize,
    items: BitVec<u8, Msb0>,
    data: FxHashMap<LocalPOS, T>,
    max_page_len: usize,
    properties: Properties,
    meta: Arc<Meta>,
    lsn: u64,
}

impl<T: Debug> Debug for SegmentContainer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let items = self
            .items
            .iter()
            .map(|x| if *x { 1 } else { 0 })
            .collect::<Vec<_>>();
        let mut data = self.data.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        f.debug_struct("SegmentContainer")
            .field("page_id", &self.segment_id)
            .field("items", &items as &dyn Debug)
            .field("data", &data)
            .field("max_page_len", &self.max_page_len)
            .field("properties", &self.properties)
            .finish()
    }
}

pub trait HasRow: Default {
    fn row(&self) -> usize;
    fn row_mut(&mut self) -> &mut usize;
}

impl<T: HasRow> SegmentContainer<T> {
    pub fn new(segment_id: usize, max_page_len: usize, meta: Arc<Meta>) -> Self {
        assert!(max_page_len > 0, "max_page_len must be greater than 0");
        Self {
            segment_id,
            items: BitVec::repeat(false, max_page_len),
            data: Default::default(),
            max_page_len,
            properties: Default::default(),
            meta,
            lsn: 0,
        }
    }

    #[inline]
    pub fn est_size(&self) -> usize {
        //FIXME: this is a rough estimate and should be improved
        let data_size = (self.data.len() as f64 * std::mem::size_of::<T>() as f64 * 1.5) as usize; // Estimate size of data
        data_size + self.t_prop_est_size() + self.c_prop_est_size()
    }

    pub fn get(&self, item_pos: &LocalPOS) -> Option<&T> {
        self.data.get(item_pos)
    }

    pub fn set_item(&mut self, item_pos: LocalPOS) {
        self.items.set(item_pos.0 as usize, true);
    }

    pub fn max_page_len(&self) -> usize {
        self.max_page_len
    }

    pub fn is_full(&self) -> bool {
        self.data.len() == self.max_page_len
    }

    pub fn t_len(&self) -> usize {
        self.properties.t_len()
    }

    pub(crate) fn reserve_local_row(&mut self, item_pos: LocalPOS) -> Either<&mut T, &mut T> {
        let local_row = self.data.len();
        self.set_item(item_pos);
        match self.data.entry(item_pos) {
            Entry::Occupied(occupied_entry) => Either::Left(occupied_entry.into_mut()),
            Entry::Vacant(vacant_entry) => {
                let vacant_entry = vacant_entry.insert(T::default());
                *vacant_entry.row_mut() = local_row;
                Either::Right(vacant_entry)
            }
        }
    }

    #[inline]
    pub fn t_prop_est_size(&self) -> usize {
        let row_size = self.meta.temporal_est_row_size();
        let row_count = self.properties.t_len();

        row_size * row_count
    }

    pub(crate) fn c_prop_est_size(&self) -> usize {
        self.meta.const_est_row_size() * self.len()
    }

    pub fn properties(&self) -> &Properties {
        &self.properties
    }

    pub fn properties_mut(&mut self) -> &mut Properties {
        &mut self.properties
    }

    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }

    pub fn items(&self) -> &BitVec<u8, Msb0> {
        &self.items
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

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn row_entries(&self) -> impl Iterator<Item = (LocalPOS, &T, PropEntry)> {
        self.items.iter_ones().filter_map(move |l_pos| {
            let entry = self.data.get(&LocalPOS(l_pos))?;
            Some((
                LocalPOS(l_pos),
                entry,
                self.properties().get_entry(entry.row()),
            ))
        })
    }

    pub fn all_entries(
        &self,
    ) -> impl ExactSizeIterator<Item = (LocalPOS, Option<(&T, PropEntry)>)> {
        self.items.iter().enumerate().map(move |(l_pos, exists)| {
            let l_pos = LocalPOS(l_pos);
            let entry = (*exists).then(|| {
                let entry = self.data.get(&l_pos).unwrap();
                (entry, self.properties().get_entry(entry.row()))
            });
            (l_pos, entry)
        })
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        self.properties.earliest()
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        self.properties.latest()
    }

    pub fn temporal_index(&self) -> Vec<usize> {
        self.row_entries()
            .flat_map(|(_, mp, _)| {
                let row = mp.row();
                self.properties()
                    .temporal_index(row)
                    .into_iter()
                    .flat_map(|entry| entry.props_ts.iter())
                    .filter_map(|(_, &v)| v)
            })
            .collect::<Vec<_>>()
    }

    pub fn t_prop(&self, item_id: impl Into<LocalPOS>, prop_id: usize) -> Option<TPropCell<'_>> {
        let item_id = item_id.into();
        self.data.get(&item_id).and_then(|entry| {
            let prop_entry = self.properties.get_entry(entry.row());
            prop_entry.prop(prop_id)
        })
    }

    pub fn t_prop_rows(&self, item_id: impl Into<LocalPOS>) -> &TCell<Option<usize>> {
        let item_id = item_id.into();
        self.data
            .get(&item_id)
            .map(|entry| {
                let prop_entry = self.properties.get_entry(entry.row());
                prop_entry.t_cell()
            })
            .unwrap_or(&TCell::Empty)
    }

    pub fn c_prop(&self, item_id: impl Into<LocalPOS>, prop_id: usize) -> Option<Prop> {
        let item_id = item_id.into();
        self.data.get(&item_id).and_then(|entry| {
            let prop_entry = self.properties.c_column(prop_id)?;
            prop_entry.get(entry.row())
        })
    }

    pub fn additions(&self, item_pos: LocalPOS) -> &PropTimestamps {
        self.data
            .get(&item_pos)
            .and_then(|entry| {
                let prop_entry = self.properties.get_entry(entry.row());
                prop_entry.timestamps()
            })
            .unwrap_or(&EMPTY_PROP_TIMESTAMPS)
    }
}

const EMPTY_PROP_TIMESTAMPS: PropTimestamps = PropTimestamps {
    edge_ts: TCell::Empty,
    props_ts: TCell::Empty,
};
