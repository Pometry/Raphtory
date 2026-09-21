use super::properties::{PropEntry, Properties};
use crate::{LocalPOS, error::StorageError};
use raphtory_api::core::{
    entities::properties::{
        meta::Meta,
        prop::{AsPropRef, Prop},
    },
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{
        ELID,
        properties::{tcell::TCell, tprop::TPropCell},
    },
    storage::timeindex::EventTime,
};
use rayon::prelude::*;
use std::{
    fmt::{Debug, Formatter},
    iter,
    sync::Arc,
};

pub mod additions;
pub mod edge;
pub mod graph_prop;
pub mod node;
pub mod node_type_index;

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

struct SparseVec<T> {
    index: PageIndex,
    data: Vec<(LocalPOS, T)>,
    max_local_pos: Option<LocalPOS>,
    min_local_pos: LocalPOS, // don't use to check for empty
}

impl<T> Default for SparseVec<T> {
    fn default() -> Self {
        Self {
            data: Default::default(),
            index: Default::default(),
            max_local_pos: Default::default(),
            min_local_pos: LocalPOS(u32::MAX),
        }
    }
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

    /// all the items between lowest and highest position
    /// this has items if there is any position filled between min_local_pos and max_local_pos
    fn iter_all_range(&self) -> impl ExactSizeIterator<Item = Option<(LocalPOS, &T)>> {
        // `end` is exclusive, so the highest filled position needs the `+ 1`
        let (start, end) = match self.max_local_pos() {
            Some(max) => (self.min_local_pos.as_index(), max.as_index() + 1),
            None => (0, 0),
        };
        (start..end)
            .zip(self.index.0[start..end].iter())
            .map(move |(pos, i)| {
                i.index()
                    .map(move |i| (LocalPOS(pos as u32), &self.data[i].1))
            })
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
                self.min_local_pos = self.min_local_pos.min(pos);
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
    out_count: usize, // used to count num edges
    inb_count: usize, // used to count num edges
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
            out_count: 0,
            inb_count: 0,
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

    pub fn inc_out_count(&mut self, i: usize) {
        self.out_count += i;
    }

    pub fn inc_inb_count(&mut self, i: usize) {
        self.inb_count += i;
    }

    pub fn out_count(&self) -> usize {
        self.out_count
    }

    pub fn inb_count(&self) -> usize {
        self.inb_count
    }

    #[inline]
    pub fn est_size(&self) -> usize {
        // TODO: this is a rough estimate and should be improved
        let data_size =
            (self.data.num_filled() as f64 * std::mem::size_of::<T>() as f64 * 1.5) as usize; // Estimate size of data
        let timestamp_size = std::mem::size_of::<EventTime>();
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

    pub fn deletions_len(&self) -> usize {
        self.properties.deletions_count()
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

    pub fn check_metadata<P: AsPropRef>(
        &self,
        local_pos: LocalPOS,
        props: &[(usize, P)],
    ) -> Result<(), StorageError> {
        if let Some(item) = self.get(local_pos) {
            let local_row = item.row();
            let prop_entry = self.properties().get_entry(local_row);

            for (prop_id, prop_val) in props {
                prop_entry.check_metadata(*prop_id, prop_val.as_prop_ref())?;
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

    pub fn len(&self) -> u32 {
        self.data.data.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.data.data.is_empty()
    }

    /// returns items in insertion order!
    pub fn row_entries(&self) -> impl Iterator<Item = (LocalPOS, &T, PropEntry<'_>)> {
        self.data
            .iter_filled()
            .map(|(l_pos, entry)| (l_pos, entry, self.properties().get_entry(entry.row())))
    }

    /// return filled entries ordered by index
    #[inline(always)]
    pub fn row_entries_ordered(&self) -> impl Iterator<Item = (LocalPOS, &T, PropEntry<'_>)> {
        self.data.iter_all_range().filter_map(|entry| {
            let (pos, v) = entry?;
            let row = self.properties().get_entry(v.row());
            Some((pos, v, row))
        })
    }

    pub fn all_entries(&self) -> impl Iterator<Item = (LocalPOS, Option<(&T, PropEntry<'_>)>)> {
        self.data
            .iter_all()
            .chain(iter::repeat(None))
            .take(
                self.data
                    .max_local_pos()
                    .map(|p| (p.0 + 1) as usize)
                    .unwrap_or(0),
            )
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
    ) -> impl ParallelIterator<Item = (LocalPOS, Option<(&T, PropEntry<'_>)>)> + '_ {
        self.data.par_iter_all().enumerate().map(|(i, v)| {
            (
                LocalPOS::from(i),
                v.map(|entry| (entry, self.properties().get_entry(entry.row()))),
            )
        })
    }

    pub fn earliest(&self) -> Option<EventTime> {
        self.properties.earliest()
    }

    pub fn latest(&self) -> Option<EventTime> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[derive(Debug, Default, PartialEq, Eq)]
    struct TestEntry {
        row: usize,
        value: u64,
    }

    impl HasRow for TestEntry {
        fn row(&self) -> usize {
            self.row
        }

        fn row_mut(&mut self) -> &mut usize {
            &mut self.row
        }
    }

    /// Reference model: the positions inserted, de-duplicated, in first-insertion order.
    /// The value stored at each position is the order in which it was first inserted, so
    /// it doubles as the expected `row`.
    fn model(positions: &[u32]) -> Vec<(LocalPOS, u64)> {
        let mut seen = HashMap::new();
        let mut out: Vec<(LocalPOS, u64)> = Vec::new();
        for &p in positions {
            seen.entry(p).or_insert_with(|| {
                out.push((LocalPOS(p), out.len() as u64));
                ()
            });
        }
        out
    }

    fn build_sparse(positions: &[u32]) -> SparseVec<TestEntry> {
        let mut sparse: SparseVec<TestEntry> = SparseVec::default();
        for &p in positions {
            let next_value = sparse.num_filled() as u64;
            if let MaybeNew::New(entry) = sparse.get_or_new(LocalPOS(p)) {
                entry.value = next_value;
            }
        }
        sparse
    }

    fn build_container(positions: &[u32], max_page_len: u32) -> SegmentContainer<TestEntry> {
        let mut container: SegmentContainer<TestEntry> =
            SegmentContainer::new(0, max_page_len, Arc::new(Meta::default()));
        for &p in positions {
            let next_value = container.len() as u64;
            if let MaybeNew::New(entry) = container.reserve_local_row(LocalPOS(p)) {
                entry.value = next_value;
            }
        }
        container
    }

    // ---------------------------------------------------------------- PageIndex

    #[test]
    fn page_index_default_entry_is_empty() {
        let entry = PageIndexEntry::default();
        assert_eq!(entry.index(), None);
        assert!(!entry.is_filled());

        let entry = PageIndexEntry(0);
        assert_eq!(entry.index(), Some(0));
        assert!(entry.is_filled());
    }

    #[test]
    fn page_index_set_grows_and_leaves_holes_empty() {
        let mut index = PageIndex::default();
        assert_eq!(index.get(LocalPOS(0)), None);

        index.set(LocalPOS(3), PageIndexEntry(7));
        assert_eq!(index.get(LocalPOS(3)), Some(7));
        assert_eq!(index.get(LocalPOS(0)), None);
        assert_eq!(index.get(LocalPOS(2)), None);
        // out of bounds reads are `None`, not a panic
        assert_eq!(index.get(LocalPOS(4)), None);
        assert_eq!(index.get(LocalPOS(u32::MAX - 1)), None);

        assert_eq!(index.iter().len(), 4);
        assert_eq!(
            index.iter().collect::<Vec<_>>(),
            vec![None, None, None, Some(7)]
        );
        assert_eq!(
            index.filled_positions().collect::<Vec<_>>(),
            vec![LocalPOS(3)]
        );
    }

    #[test]
    fn page_index_set_overwrites() {
        let mut index = PageIndex::default();
        index.set(LocalPOS(1), PageIndexEntry(5));
        index.set(LocalPOS(1), PageIndexEntry(9));
        assert_eq!(index.get(LocalPOS(1)), Some(9));
        assert_eq!(index.iter().len(), 2);
    }

    // ---------------------------------------------------------------- SparseVec

    #[test]
    fn sparse_vec_empty() {
        let sparse: SparseVec<TestEntry> = SparseVec::default();

        assert_eq!(sparse.num_filled(), 0);
        assert_eq!(sparse.max_local_pos(), None);
        assert_eq!(sparse.get(LocalPOS(0)), None);
        assert!(!sparse.is_filled(LocalPOS(0)));
        assert_eq!(sparse.iter_filled().count(), 0);
        assert_eq!(sparse.iter_all().len(), 0);
        assert_eq!(sparse.iter_all_range().len(), 0);
        assert_eq!(sparse.iter_all_range().count(), 0);
    }

    #[test]
    fn sparse_vec_single_entry_at_zero() {
        let sparse = build_sparse(&[0]);

        assert_eq!(sparse.num_filled(), 1);
        assert_eq!(sparse.max_local_pos(), Some(LocalPOS(0)));
        assert_eq!(sparse.get(LocalPOS(0)).map(|e| e.value), Some(0));
        assert_eq!(
            sparse.iter_filled().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![LocalPOS(0)]
        );
        assert_eq!(sparse.iter_all().len(), 1);
        // the single filled position is both the min and the max, and must be returned
        assert_eq!(
            sparse
                .iter_all_range()
                .map(|e| e.map(|(p, v)| (p, v.value)))
                .collect::<Vec<_>>(),
            vec![Some((LocalPOS(0), 0))]
        );
    }

    #[test]
    fn sparse_vec_get_or_new_is_idempotent() {
        let mut sparse: SparseVec<TestEntry> = SparseVec::default();

        let MaybeNew::New(entry) = sparse.get_or_new(LocalPOS(2)) else {
            panic!("first insert at a position must be New");
        };
        entry.value = 42;
        assert_eq!(entry.row, 0);

        let MaybeNew::Existing(entry) = sparse.get_or_new(LocalPOS(2)) else {
            panic!("second insert at the same position must be Existing");
        };
        assert_eq!(entry.value, 42);
        assert_eq!(entry.row, 0);

        assert_eq!(sparse.num_filled(), 1);
    }

    #[test]
    fn sparse_vec_rows_follow_insertion_order() {
        let sparse = build_sparse(&[5, 1, 5, 9, 1]);

        assert_eq!(sparse.num_filled(), 3);
        assert_eq!(sparse.get(LocalPOS(5)).map(|e| e.row), Some(0));
        assert_eq!(sparse.get(LocalPOS(1)).map(|e| e.row), Some(1));
        assert_eq!(sparse.get(LocalPOS(9)).map(|e| e.row), Some(2));
    }

    #[test]
    fn sparse_vec_iter_filled_is_insertion_order_not_position_order() {
        let sparse = build_sparse(&[5, 1, 9]);

        assert_eq!(
            sparse
                .iter_filled()
                .map(|(p, e)| (p, e.value))
                .collect::<Vec<_>>(),
            vec![(LocalPOS(5), 0), (LocalPOS(1), 1), (LocalPOS(9), 2)]
        );
    }

    #[test]
    fn sparse_vec_iter_all_is_dense_from_zero() {
        let sparse = build_sparse(&[3, 1]);

        assert_eq!(sparse.iter_all().len(), 4);
        assert_eq!(
            sparse
                .iter_all()
                .map(|e| e.map(|e| e.value))
                .collect::<Vec<_>>(),
            vec![None, Some(1), None, Some(0)]
        );
    }

    #[test]
    fn sparse_vec_iter_all_range_is_min_to_max_inclusive() {
        let sparse = build_sparse(&[4, 2, 6]);

        assert_eq!(
            sparse
                .iter_all_range()
                .map(|e| e.map(|(p, v)| (p, v.value)))
                .collect::<Vec<_>>(),
            vec![
                Some((LocalPOS(2), 1)),
                None,
                Some((LocalPOS(4), 0)),
                None,
                Some((LocalPOS(6), 2)),
            ]
        );
        assert_eq!(sparse.iter_all_range().len(), 5);
    }

    #[test]
    fn sparse_vec_iter_all_range_skips_leading_holes() {
        let sparse = build_sparse(&[7, 9]);

        // positions 0..7 are never visited, unlike `iter_all`
        assert_eq!(sparse.iter_all().len(), 10);
        assert_eq!(
            sparse
                .iter_all_range()
                .map(|e| e.map(|(p, _)| p))
                .collect::<Vec<_>>(),
            vec![Some(LocalPOS(7)), None, Some(LocalPOS(9))]
        );
    }

    #[test]
    fn sparse_vec_par_iters_match_sequential() {
        let sparse = build_sparse(&[4, 0, 9, 4]);

        assert_eq!(
            sparse.par_iter_filled().map(|(p, _)| p).collect::<Vec<_>>(),
            sparse.iter_filled().map(|(p, _)| p).collect::<Vec<_>>()
        );
        assert_eq!(
            sparse
                .par_iter_all()
                .map(|e| e.map(|e| e.value))
                .collect::<Vec<_>>(),
            sparse
                .iter_all()
                .map(|e| e.map(|e| e.value))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn sparse_vec_handles_large_positions() {
        let sparse = build_sparse(&[u32::MAX - 1]);

        assert_eq!(sparse.num_filled(), 1);
        assert_eq!(sparse.max_local_pos(), Some(LocalPOS(u32::MAX - 1)));
        assert!(sparse.is_filled(LocalPOS(u32::MAX - 1)));
        assert_eq!(sparse.iter_all_range().len(), 1);
    }

    // ------------------------------------------------------- SparseVec proptests

    fn positions() -> impl Strategy<Value = Vec<u32>> {
        prop::collection::vec(0u32..24, 0..40)
    }

    proptest! {
        #[test]
        fn prop_sparse_vec_matches_model(positions in positions()) {
            let expected = model(&positions);
            let sparse = build_sparse(&positions);

            prop_assert_eq!(sparse.num_filled(), expected.len());
            prop_assert_eq!(
                sparse.max_local_pos(),
                expected.iter().map(|(p, _)| *p).max()
            );

            for (pos, value) in &expected {
                prop_assert!(sparse.is_filled(*pos));
                let entry = sparse.get(*pos).expect("filled position must be gettable");
                prop_assert_eq!(entry.value, *value);
                // `row` is the index into the backing `data` vec, i.e. insertion order
                prop_assert_eq!(entry.row, *value as usize);
            }
        }

        #[test]
        fn prop_sparse_vec_iter_filled_is_insertion_order(positions in positions()) {
            let expected = model(&positions);
            let sparse = build_sparse(&positions);

            prop_assert_eq!(
                sparse.iter_filled().map(|(p, e)| (p, e.value)).collect::<Vec<_>>(),
                expected
            );
        }

        #[test]
        fn prop_sparse_vec_iter_all_is_dense(positions in positions()) {
            let expected = model(&positions);
            let sparse = build_sparse(&positions);

            let len = expected.iter().map(|(p, _)| p.as_index() + 1).max().unwrap_or(0);
            let mut dense = vec![None; len];
            for (pos, value) in &expected {
                dense[pos.as_index()] = Some(*value);
            }

            prop_assert_eq!(sparse.iter_all().len(), len);
            prop_assert_eq!(
                sparse.iter_all().map(|e| e.map(|e| e.value)).collect::<Vec<_>>(),
                dense
            );
        }

        #[test]
        fn prop_sparse_vec_iter_all_range_covers_min_to_max(positions in positions()) {
            let expected = model(&positions);
            let sparse = build_sparse(&positions);

            let min = expected.iter().map(|(p, _)| p.as_index()).min();
            let max = expected.iter().map(|(p, _)| p.as_index()).max();

            let dense: Vec<Option<(LocalPOS, u64)>> = match (min, max) {
                (Some(min), Some(max)) => {
                    let lookup: HashMap<usize, u64> = expected
                        .iter()
                        .map(|(p, v)| (p.as_index(), *v))
                        .collect();
                    (min..=max)
                        .map(|i| lookup.get(&i).map(|v| (LocalPOS(i as u32), *v)))
                        .collect()
                }
                _ => vec![],
            };

            let actual = sparse
                .iter_all_range()
                .map(|e| e.map(|(p, v)| (p, v.value)))
                .collect::<Vec<_>>();

            // every filled position between min and max is returned, exactly once, in order
            prop_assert_eq!(&actual, &dense);
            // `ExactSizeIterator::len` must agree with what is actually yielded
            prop_assert_eq!(sparse.iter_all_range().len(), actual.len());
            // no entry is lost relative to `iter_filled`
            prop_assert_eq!(actual.iter().flatten().count(), sparse.num_filled());
        }

        #[test]
        fn prop_sparse_vec_unfilled_positions_are_empty(positions in positions()) {
            let expected = model(&positions);
            let sparse = build_sparse(&positions);
            let filled: HashMap<u32, u64> = expected.iter().map(|(p, v)| (p.0, *v)).collect();

            for pos in 0u32..32 {
                if !filled.contains_key(&pos) {
                    prop_assert!(!sparse.is_filled(LocalPOS(pos)));
                    prop_assert!(sparse.get(LocalPOS(pos)).is_none());
                }
            }
        }

        #[test]
        fn prop_sparse_vec_par_iters_match_sequential(positions in positions()) {
            let sparse = build_sparse(&positions);

            prop_assert_eq!(
                sparse.par_iter_filled().map(|(p, e)| (p, e.value)).collect::<Vec<_>>(),
                sparse.iter_filled().map(|(p, e)| (p, e.value)).collect::<Vec<_>>()
            );
            prop_assert_eq!(
                sparse.par_iter_all().map(|e| e.map(|e| e.value)).collect::<Vec<_>>(),
                sparse.iter_all().map(|e| e.map(|e| e.value)).collect::<Vec<_>>()
            );
        }
    }

    // --------------------------------------------------------- SegmentContainer

    #[test]
    #[should_panic(expected = "max_page_len must be greater than 0")]
    fn segment_container_rejects_zero_page_len() {
        SegmentContainer::<TestEntry>::new(0, 0, Arc::new(Meta::default()));
    }

    #[test]
    fn segment_container_empty() {
        let container = build_container(&[], 8);

        assert!(container.is_empty());
        assert_eq!(container.len(), 0);
        assert_eq!(container.max_rows(), 0);
        assert!(!container.is_full());
        assert_eq!(container.segment_id(), 0);
        assert_eq!(container.max_page_len(), 8);
        assert_eq!(container.get(LocalPOS(0)), None);
        assert!(!container.has_item(LocalPOS(0)));
        assert_eq!(container.filled_positions().count(), 0);
        assert_eq!(container.row_entries().count(), 0);
        assert_eq!(container.row_entries_ordered().count(), 0);
        assert_eq!(container.all_entries().count(), 0);
        assert_eq!(container.all_entries_par().count(), 0);
        assert_eq!(container.earliest(), None);
        assert_eq!(container.latest(), None);
    }

    #[test]
    fn segment_container_reserve_local_row() {
        let mut container = build_container(&[], 4);

        let MaybeNew::New(entry) = container.reserve_local_row(LocalPOS(1)) else {
            panic!("first reserve must be New");
        };
        entry.value = 10;

        let MaybeNew::Existing(entry) = container.reserve_local_row(LocalPOS(1)) else {
            panic!("second reserve must be Existing");
        };
        assert_eq!(entry.value, 10);

        assert_eq!(container.len(), 1);
        assert!(!container.is_empty());
        assert!(container.has_item(LocalPOS(1)));
        assert!(!container.has_item(LocalPOS(0)));
        assert_eq!(container.max_rows(), 2);
    }

    #[test]
    fn segment_container_is_full_counts_filled_not_positions() {
        let mut container = build_container(&[0, 1, 9], 4);
        assert!(!container.is_full());

        container.reserve_local_row(LocalPOS(5));
        assert_eq!(container.len(), 4);
        assert!(container.is_full());
        // `max_rows` tracks the highest position, not the number of entries
        assert_eq!(container.max_rows(), 10);
    }

    #[test]
    fn segment_container_filled_positions_is_ascending() {
        let container = build_container(&[5, 1, 9, 1], 16);

        assert_eq!(
            container.filled_positions().collect::<Vec<_>>(),
            vec![LocalPOS(1), LocalPOS(5), LocalPOS(9)]
        );
        // the parallel variant walks the backing data, so it is in insertion order
        assert_eq!(
            container.filled_positions_par().collect::<Vec<_>>(),
            vec![LocalPOS(5), LocalPOS(1), LocalPOS(9)]
        );
    }

    #[test]
    fn segment_container_row_entries_is_insertion_order() {
        let container = build_container(&[5, 1, 9], 16);

        assert_eq!(
            container
                .row_entries()
                .map(|(p, e, _)| (p, e.value))
                .collect::<Vec<_>>(),
            vec![(LocalPOS(5), 0), (LocalPOS(1), 1), (LocalPOS(9), 2)]
        );
    }

    #[test]
    fn segment_container_row_entries_ordered_is_position_order() {
        let container = build_container(&[5, 1, 9], 16);

        assert_eq!(
            container
                .row_entries_ordered()
                .map(|(p, e, _)| (p, e.value))
                .collect::<Vec<_>>(),
            vec![(LocalPOS(1), 1), (LocalPOS(5), 0), (LocalPOS(9), 2)]
        );
    }

    #[test]
    fn segment_container_row_entries_ordered_includes_highest_position() {
        let container = build_container(&[0, 1, 2], 8);

        assert_eq!(
            container
                .row_entries_ordered()
                .map(|(p, _, _)| p)
                .collect::<Vec<_>>(),
            vec![LocalPOS(0), LocalPOS(1), LocalPOS(2)]
        );
    }

    #[test]
    fn segment_container_all_entries_is_dense() {
        let container = build_container(&[3, 1], 8);

        assert_eq!(
            container
                .all_entries()
                .map(|(p, e)| (p, e.map(|(e, _)| e.value)))
                .collect::<Vec<_>>(),
            vec![
                (LocalPOS(0), None),
                (LocalPOS(1), Some(1)),
                (LocalPOS(2), None),
                (LocalPOS(3), Some(0)),
            ]
        );
        assert_eq!(
            container
                .all_entries_par()
                .map(|(p, e)| (p, e.map(|(e, _)| e.value)))
                .collect::<Vec<_>>(),
            container
                .all_entries()
                .map(|(p, e)| (p, e.map(|(e, _)| e.value)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn segment_container_entries_carry_the_matching_prop_row() {
        let mut container = build_container(&[5, 1, 9], 16);

        // tag each entry's property row with its position so mis-paired rows show up
        for pos in [5u32, 1, 9] {
            let row = container.get(LocalPOS(pos)).unwrap().row();
            container
                .properties_mut()
                .get_mut_entry(row)
                .append_const_props([(0usize, Prop::U64(pos as u64))]);
        }

        for (pos, _, props) in container.row_entries() {
            assert_eq!(props.metadata(0), Some(Prop::U64(pos.0 as u64)));
        }
        for (pos, _, props) in container.row_entries_ordered() {
            assert_eq!(props.metadata(0), Some(Prop::U64(pos.0 as u64)));
        }
        for (pos, entry) in container.all_entries() {
            if let Some((_, props)) = entry {
                assert_eq!(props.metadata(0), Some(Prop::U64(pos.0 as u64)));
            }
        }

        assert_eq!(container.c_prop(LocalPOS(9), 0), Some(Prop::U64(9)));
        assert_eq!(container.c_prop(LocalPOS(2), 0), None);
    }

    #[test]
    fn segment_container_all_entries_seq_eq_par_when_empty() {
        let container = build_container(&[], 8);

        // nothing stored means nothing yielded, by either variant
        let seq = container.all_entries().map(|(p, _)| p).collect::<Vec<_>>();
        let par = container
            .all_entries_par()
            .map(|(p, _)| p)
            .collect::<Vec<_>>();
        assert_eq!(seq, vec![]);
        assert_eq!(par, vec![]);
    }

    #[test]
    fn segment_container_counts() {
        let mut container = build_container(&[0], 8);
        assert_eq!(container.out_count(), 0);
        assert_eq!(container.inb_count(), 0);

        container.inc_out_count(3);
        container.inc_out_count(2);
        container.inc_inb_count(7);
        assert_eq!(container.out_count(), 5);
        assert_eq!(container.inb_count(), 7);
    }

    #[test]
    fn segment_container_take_resets_but_keeps_config() {
        let mut container = build_container(&[2, 4], 8);
        container.inc_out_count(3);

        let taken = container.take();

        assert_eq!(taken.len(), 2);
        assert_eq!(taken.out_count(), 3);

        assert!(container.is_empty());
        assert_eq!(container.out_count(), 0);
        assert_eq!(container.max_rows(), 0);
        assert_eq!(container.segment_id(), taken.segment_id());
        assert_eq!(container.max_page_len(), taken.max_page_len());
        assert_eq!(container.row_entries_ordered().count(), 0);
    }

    #[test]
    fn segment_container_missing_positions_have_empty_time_cells() {
        let container = build_container(&[1], 8);

        assert!(container.additions(LocalPOS(0)).is_empty());
        assert!(container.deletions(LocalPOS(0)).is_empty());
        assert!(container.times_from_props(LocalPOS(0)).is_empty());
        assert!(container.t_prop_rows(LocalPOS(0)).is_empty());
        assert_eq!(container.t_prop(LocalPOS(0), 0).is_none(), true);
    }

    // ----------------------------------------------- SegmentContainer proptests

    proptest! {
        #[test]
        fn prop_segment_container_matches_model(positions in positions()) {
            let expected = model(&positions);
            let container = build_container(&positions, 64);

            prop_assert_eq!(container.len() as usize, expected.len());
            prop_assert_eq!(container.is_empty(), expected.is_empty());
            prop_assert_eq!(
                container.max_rows(),
                expected.iter().map(|(p, _)| p.as_index() + 1).max().unwrap_or(0)
            );

            for (pos, value) in &expected {
                prop_assert!(container.has_item(*pos));
                prop_assert_eq!(container.get(*pos).map(|e| e.value), Some(*value));
            }
        }

        #[test]
        fn prop_segment_container_row_entries_agree(positions in positions()) {
            let expected = model(&positions);
            let container = build_container(&positions, 64);

            // insertion order
            prop_assert_eq!(
                container.row_entries().map(|(p, e, _)| (p, e.value)).collect::<Vec<_>>(),
                expected.clone()
            );

            // position order, same set
            let mut by_position = expected;
            by_position.sort_by_key(|(p, _)| *p);
            prop_assert_eq!(
                container.row_entries_ordered().map(|(p, e, _)| (p, e.value)).collect::<Vec<_>>(),
                by_position.clone()
            );

            // `filled_positions` agrees with the ordered entries
            prop_assert_eq!(
                container.filled_positions().collect::<Vec<_>>(),
                by_position.iter().map(|(p, _)| *p).collect::<Vec<_>>()
            );
        }

        #[test]
        fn prop_segment_container_all_entries_is_dense(positions in positions()) {
            let expected = model(&positions);
            let container = build_container(&positions, 64);

            let len = expected
                .iter()
                .map(|(p, _)| p.as_index() + 1)
                .max()
                .unwrap_or(0);
            let mut dense = vec![None; len];
            for (pos, value) in &expected {
                dense[pos.as_index()] = Some(*value);
            }

            let actual = container
                .all_entries()
                .map(|(_, e)| e.map(|(e, _)| e.value))
                .collect::<Vec<_>>();

            prop_assert_eq!(&actual, &dense);
            prop_assert_eq!(
                container
                    .all_entries()
                    .map(|(p, _)| p)
                    .collect::<Vec<_>>(),
                (0..len).map(LocalPOS::from).collect::<Vec<_>>()
            );
            prop_assert_eq!(
                container
                    .all_entries_par()
                    .map(|(p, e)| (p, e.map(|(e, _)| e.value)))
                    .collect::<Vec<_>>(),
                container
                    .all_entries()
                    .map(|(p, e)| (p, e.map(|(e, _)| e.value)))
                    .collect::<Vec<_>>()
            );
        }

        #[test]
        fn prop_segment_container_is_full(positions in positions(), max_page_len in 1u32..24) {
            let expected = model(&positions);
            let container = build_container(&positions, max_page_len);

            prop_assert_eq!(
                container.is_full(),
                expected.len() == max_page_len as usize
            );
        }
    }
}
