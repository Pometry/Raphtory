use crate::core::{storage::timeindex::AsTime, Prop};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_memstorage::core::entities::properties::tprop::TProp;
use std::ops::Range;

#[derive(Copy, Clone, Debug)]
pub enum TPropRef<'a> {
    Mem(&'a TProp),
    #[cfg(feature = "storage")]
    Disk(DiskTProp<'a, TimeIndexEntry>),
}

macro_rules! for_all_tp {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            TPropRef::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_variants_tp {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => StorageVariants::Mem($result),
            TPropRef::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_variants_tp {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TPropRef::Mem($pattern) => $result,
        }
    };
}

pub trait TPropOps<'a>: Sized + 'a + Send {
    fn active(self, w: Range<i64>) -> bool {
        self.iter_window_t(w).next().is_some()
    }
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)>;

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a;
    fn iter_t(self) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter().map(|(t, v)| (t.t(), v))
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a;

    fn iter_window_t(self, r: Range<i64>) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter_window(TimeIndexEntry::range(r))
            .map(|(t, v)| (t.t(), v))
    }
    fn iter_window_te(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (i64, Prop)> + Send + 'a {
        self.iter_window(r).map(|(t, v)| (t.t(), v))
    }
    fn at(self, ti: &TimeIndexEntry) -> Option<Prop>;

    fn len(self) -> usize;

    fn is_empty(self) -> bool {
        self.len() == 0
    }
}

use raphtory_memstorage::db::api::storage::graph::variants::{storage_variants, storage_variants3};

#[cfg(feature = "storage")]
macro_rules! SelfType2 {
    ($Mem:ident, $Disk:ident) => {
        StorageVariants<$Mem, $Disk>
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! SelfType2 {
    ($Mem:ident, $Disk:ident) => {
        storage_variants::StorageVariants<$Mem>
    };
}

macro_rules! for_all2 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants::StorageVariants::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            storage_variants::StorageVariants::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter2 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants::StorageVariants::Mem($pattern) => StorageVariants::Mem($result),
            storage_variants::StorageVariants::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter2 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants::StorageVariants::Mem($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! SelfType3 {
    ($Mem:ident, $Unlocked:ident, $Disk:ident) => {
        StorageVariants<$Mem, $Unlocked, $Disk>
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! SelfType3 {
    ($Mem:ident, $Unlocked:ident, $Disk:ident) => {
        storage_variants3::StorageVariants<$Mem, $Unlocked>
    };
}

macro_rules! for_all3 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants3::StorageVariants::Mem($pattern) => $result,
            storage_variants3::StorageVariants::Unlocked($pattern) => $result,
            #[cfg(feature = "storage")]
            storage_variants3::StorageVariants::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter3 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants3::StorageVariants::Mem($pattern) => StorageVariants::Mem($result),
            storage_variants3::torageVariants::Unlocked($pattern) => {
                StorageVariants::Unlocked($result)
            }
            storage_variants3::torageVariants::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter3 {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            storage_variants3::StorageVariants::Mem($pattern) => {
                storage_variants3::StorageVariants::Mem($result)
            }
            storage_variants3::StorageVariants::Unlocked($pattern) => {
                storage_variants3::StorageVariants::Unlocked($result)
            }
        }
    };
}

impl<
        'a,
        Mem: TPropOps<'a> + 'a,
        Unlocked: TPropOps<'a> + 'a,
        #[cfg(feature = "storage")] Disk: TPropOps<'a> + 'a,
    > TPropOps<'a> for SelfType3!(Mem, Unlocked, Disk)
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all3!(self, props => props.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter3!(self, props => props.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter3!(self, props => props.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all3!(self, props => props.at(ti))
    }

    fn len(self) -> usize {
        for_all3!(self, props=> props.len())
    }

    fn is_empty(self) -> bool {
        for_all3!(self, props => props.is_empty())
    }
}

impl<'a, Mem: TPropOps<'a> + 'a, #[cfg(feature = "storage")] Disk: TPropOps<'a> + 'a> TPropOps<'a>
    for SelfType2!(Mem, Disk)
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all2!(self, props => props.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter2!(self, props => props.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter2!(self, props => props.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all2!(self, props => props.at(ti))
    }

    fn len(self) -> usize {
        for_all2!(self, props=> props.len())
    }

    fn is_empty(self) -> bool {
        for_all2!(self, props => props.is_empty())
    }
}

impl<'a> TPropOps<'a> for &'a TProp {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        match self {
            TProp::Empty => None,
            TProp::Str(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Str(v.clone()))),
            TProp::I32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::I32(*v))),
            TProp::I64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::I64(*v))),
            TProp::U8(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U8(*v))),
            TProp::U16(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U16(*v))),
            TProp::U32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U32(*v))),
            TProp::U64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::U64(*v))),
            TProp::F32(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::F32(*v))),
            TProp::F64(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::F64(*v))),
            TProp::Bool(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Bool(*v))),
            TProp::DTime(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::DTime(*v))),
            TProp::NDTime(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::NDTime(*v))),
            TProp::Graph(cell) => cell
                .last_before(t)
                .map(|(t, v)| (t, Prop::Graph(v.clone()))),
            TProp::PersistentGraph(cell) => cell
                .last_before(t)
                .map(|(t, v)| (t, Prop::PersistentGraph(v.clone()))),
            TProp::Document(cell) => cell
                .last_before(t)
                .map(|(t, v)| (t, Prop::Document(v.clone()))),
            TProp::List(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::List(v.clone()))),
            TProp::Map(cell) => cell.last_before(t).map(|(t, v)| (t, Prop::Map(v.clone()))),
        }
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_inner()
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        self.iter_window_inner(r)
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        match self {
            TProp::Empty => None,
            TProp::Str(cell) => cell.at(ti).map(|v| Prop::Str(v.clone())),
            TProp::I32(cell) => cell.at(ti).map(|v| Prop::I32(*v)),
            TProp::I64(cell) => cell.at(ti).map(|v| Prop::I64(*v)),
            TProp::U32(cell) => cell.at(ti).map(|v| Prop::U32(*v)),
            TProp::U8(cell) => cell.at(ti).map(|v| Prop::U8(*v)),
            TProp::U16(cell) => cell.at(ti).map(|v| Prop::U16(*v)),
            TProp::U64(cell) => cell.at(ti).map(|v| Prop::U64(*v)),
            TProp::F32(cell) => cell.at(ti).map(|v| Prop::F32(*v)),
            TProp::F64(cell) => cell.at(ti).map(|v| Prop::F64(*v)),
            TProp::Bool(cell) => cell.at(ti).map(|v| Prop::Bool(*v)),
            TProp::DTime(cell) => cell.at(ti).map(|v| Prop::DTime(*v)),
            TProp::NDTime(cell) => cell.at(ti).map(|v| Prop::NDTime(*v)),
            TProp::Graph(cell) => cell.at(ti).map(|v| Prop::Graph(v.clone())),
            TProp::PersistentGraph(cell) => cell.at(ti).map(|v| Prop::PersistentGraph(v.clone())),
            TProp::Document(cell) => cell.at(ti).map(|v| Prop::Document(v.clone())),
            TProp::List(cell) => cell.at(ti).map(|v| Prop::List(v.clone())),
            TProp::Map(cell) => cell.at(ti).map(|v| Prop::Map(v.clone())),
        }
    }

    fn len(self) -> usize {
        match self {
            TProp::Empty => 0,
            TProp::Str(v) => v.len(),
            TProp::U8(v) => v.len(),
            TProp::U16(v) => v.len(),
            TProp::I32(v) => v.len(),
            TProp::I64(v) => v.len(),
            TProp::U32(v) => v.len(),
            TProp::U64(v) => v.len(),
            TProp::F32(v) => v.len(),
            TProp::F64(v) => v.len(),
            TProp::Bool(v) => v.len(),
            TProp::DTime(v) => v.len(),
            TProp::NDTime(v) => v.len(),
            TProp::Graph(v) => v.len(),
            TProp::PersistentGraph(v) => v.len(),
            TProp::Document(v) => v.len(),
            TProp::List(v) => v.len(),
            TProp::Map(v) => v.len(),
        }
    }
}

impl<'a> TPropOps<'a> for TPropRef<'a> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all_tp!(self, tprop => tprop.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants_tp!(self, tprop => tprop.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_variants_tp!(self, tprop => tprop.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all_tp!(self, tprop => tprop.at(ti))
    }

    fn len(self) -> usize {
        for_all_tp!(self, tprop => tprop.len())
    }
}

#[cfg(test)]
mod test {
    use raphtory_memstorage::core::entities::{properties::tprop::TProp, Prop};

    #[test]
    fn updates_to_prop_can_be_window_iterated() {
        let tprop = TProp::default();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let mut tprop = TProp::from(3.into(), Prop::Str("Pometry".into()));
        tprop
            .set(1.into(), Prop::Str("Pometry Inc.".into()))
            .unwrap();
        tprop.set(2.into(), Prop::Str("Raphtory".into())).unwrap();

        assert_eq!(
            tprop.iter_window_t(2..3).collect::<Vec<_>>(),
            vec![(2, Prop::Str("Raphtory".into()))]
        );

        assert_eq!(tprop.iter_window_t(4..5).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            // Results are ordered by time
            tprop.iter_window_t(1..i64::MAX).collect::<Vec<_>>(),
            vec![
                (1, Prop::Str("Pometry Inc.".into())),
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(
            tprop.iter_window_t(3..i64::MAX).collect::<Vec<_>>(),
            vec![(3, Prop::Str("Pometry".into()))]
        );

        assert_eq!(
            tprop.iter_window_t(2..i64::MAX).collect::<Vec<_>>(),
            vec![
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window_t(5..i64::MAX).collect::<Vec<_>>(), vec![]);

        assert_eq!(
            tprop.iter_window_t(i64::MIN..4).collect::<Vec<_>>(),
            // Results are ordered by time
            vec![
                (1, Prop::Str("Pometry Inc.".into())),
                (2, Prop::Str("Raphtory".into())),
                (3, Prop::Str("Pometry".into()))
            ]
        );

        assert_eq!(tprop.iter_window_t(i64::MIN..1).collect::<Vec<_>>(), vec![]);

        let mut tprop = TProp::from(1.into(), Prop::I32(2022));
        tprop.set(2.into(), Prop::I32(2023)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::I32(2022)), (2, Prop::I32(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::I64(2022));
        tprop.set(2.into(), Prop::I64(2023)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::I64(2022)), (2, Prop::I64(2023))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F32(10.0));
        tprop.set(2.into(), Prop::F32(11.0)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::F32(10.0)), (2, Prop::F32(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::F64(10.0));
        tprop.set(2.into(), Prop::F64(11.0)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::F64(10.0)), (2, Prop::F64(11.0))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U32(1));
        tprop.set(2.into(), Prop::U32(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U32(1)), (2, Prop::U32(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U64(1));
        tprop.set(2.into(), Prop::U64(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U64(1)), (2, Prop::U64(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U8(1));
        tprop.set(2.into(), Prop::U8(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U8(1)), (2, Prop::U8(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::U16(1));
        tprop.set(2.into(), Prop::U16(2)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::U16(1)), (2, Prop::U16(2))]
        );

        let mut tprop = TProp::from(1.into(), Prop::Bool(true));
        tprop.set(2.into(), Prop::Bool(true)).unwrap();

        assert_eq!(
            tprop.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![(1, Prop::Bool(true)), (2, Prop::Bool(true))]
        );
    }
}
