use crate::{
    entities::properties::{props::TPropError, tprop::IllegalPropType},
    storage::lazy_vec::IllegalSet,
};
use bigdecimal::BigDecimal;
use lazy_vec::LazyVec;
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropRef, PropType},
    storage::arc_str::ArcStr,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use thiserror::Error;

#[cfg(feature = "arrow")]
use raphtory_api::core::entities::properties::prop::PropArray;

pub mod lazy_vec;
pub mod locked_view;
pub mod timeindex;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct TColumns {
    t_props_log: Vec<PropColumn>,
    num_rows: usize,
}

impl TColumns {
    pub fn push(
        &mut self,
        row: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<Option<usize>, TPropError> {
        let id = self.num_rows;
        let mut has_props = false;

        for (prop_id, prop) in row {
            match self.t_props_log.get_mut(prop_id) {
                Some(col) => col.push(prop)?,
                None => {
                    let col: PropColumn = PropColumn::new(self.num_rows, prop);
                    self.t_props_log
                        .resize_with(prop_id + 1, || PropColumn::Empty(id));
                    self.t_props_log[prop_id] = col;
                }
            }
            has_props = true;
        }

        if has_props {
            self.num_rows += 1;
            for col in self.t_props_log.iter_mut() {
                col.grow(self.num_rows);
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub fn push_null(&mut self) -> usize {
        let id = self.num_rows;
        for col in self.t_props_log.iter_mut() {
            col.push_null();
        }
        self.num_rows += 1;
        id
    }

    pub fn get(&self, prop_id: usize) -> Option<&PropColumn> {
        self.t_props_log.get(prop_id)
    }

    pub fn getx(&self, prop_id: usize) -> Option<&PropColumn> {
        self.t_props_log.get(prop_id)
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PropColumn> {
        self.t_props_log.iter()
    }

    pub fn num_columns(&self) -> usize {
        self.t_props_log.len()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum PropColumn {
    Empty(usize),
    Bool(LazyVec<bool>),
    U8(LazyVec<u8>),
    U16(LazyVec<u16>),
    U32(LazyVec<u32>),
    U64(LazyVec<u64>),
    I32(LazyVec<i32>),
    I64(LazyVec<i64>),
    F32(LazyVec<f32>),
    F64(LazyVec<f64>),
    Str(LazyVec<ArcStr>),
    #[cfg(feature = "arrow")]
    Array(LazyVec<PropArray>),
    List(LazyVec<Arc<Vec<Prop>>>),
    Map(LazyVec<Arc<FxHashMap<ArcStr, Prop>>>),
    NDTime(LazyVec<chrono::NaiveDateTime>),
    DTime(LazyVec<chrono::DateTime<chrono::Utc>>),
    Decimal(LazyVec<BigDecimal>),
}

#[derive(Error, Debug)]
pub enum TPropColumnError {
    #[error(transparent)]
    IllegalSetBool(#[from] IllegalSet<bool>),
    #[error(transparent)]
    IllegalSetU8(#[from] IllegalSet<u8>),
    #[error(transparent)]
    IllegalSetU16(#[from] IllegalSet<u16>),
    #[error(transparent)]
    IllegalSetU32(#[from] IllegalSet<u32>),
    #[error(transparent)]
    IllegalSetU64(#[from] IllegalSet<u64>),
    #[error(transparent)]
    IllegalSetI32(#[from] IllegalSet<i32>),
    #[error(transparent)]
    IllegalSetI64(#[from] IllegalSet<i64>),
    #[error(transparent)]
    IllegalSetF32(#[from] IllegalSet<f32>),
    #[error(transparent)]
    IllegalSetF64(#[from] IllegalSet<f64>),
    #[error(transparent)]
    IllegalSetStr(#[from] IllegalSet<ArcStr>),
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    IllegalSetArray(#[from] IllegalSet<PropArray>),
    #[error(transparent)]
    IllegalSetList(#[from] IllegalSet<Arc<Vec<Prop>>>),
    #[error(transparent)]
    IllegalSetMap(#[from] IllegalSet<Arc<FxHashMap<ArcStr, Prop>>>),
    #[error(transparent)]
    IllegalSetNDTime(#[from] IllegalSet<chrono::NaiveDateTime>),
    #[error(transparent)]
    IllegalSetDTime(#[from] IllegalSet<chrono::DateTime<chrono::Utc>>),
    #[error(transparent)]
    Decimal(#[from] IllegalSet<BigDecimal>),
    #[error(transparent)]
    IllegalPropType(#[from] IllegalPropType),
}

impl Default for PropColumn {
    fn default() -> Self {
        PropColumn::Empty(0)
    }
}

impl PropColumn {
    pub(crate) fn new(idx: usize, prop: Prop) -> Self {
        let mut col = PropColumn::default();
        col.set(idx, prop).unwrap();
        col
    }

    pub(crate) fn dtype(&self) -> PropType {
        match self {
            PropColumn::Empty(_) => PropType::Empty,
            PropColumn::Bool(_) => PropType::Bool,
            PropColumn::U8(_) => PropType::U8,
            PropColumn::U16(_) => PropType::U16,
            PropColumn::U32(_) => PropType::U32,
            PropColumn::U64(_) => PropType::U64,
            PropColumn::I32(_) => PropType::I32,
            PropColumn::I64(_) => PropType::I64,
            PropColumn::F32(_) => PropType::F32,
            PropColumn::F64(_) => PropType::F64,
            PropColumn::Str(_) => PropType::Str,
            #[cfg(feature = "arrow")]
            PropColumn::Array(_) => PropType::Array(Box::new(PropType::Empty)),
            PropColumn::List(_) => PropType::List(Box::new(PropType::Empty)),
            PropColumn::Map(_) => PropType::Map(HashMap::new().into()),
            PropColumn::NDTime(_) => PropType::NDTime,
            PropColumn::DTime(_) => PropType::DTime,
            PropColumn::Decimal(_) => PropType::Decimal { scale: 0 },
        }
    }

    pub(crate) fn grow(&mut self, new_len: usize) {
        while self.len() < new_len {
            self.push_null();
        }
    }

    pub fn set(&mut self, index: usize, prop: Prop) -> Result<(), TPropColumnError> {
        self.init_empty_col(&prop);
        match (self, prop) {
            (PropColumn::Bool(col), Prop::Bool(v)) => col.set(index, v)?,
            (PropColumn::I64(col), Prop::I64(v)) => col.set(index, v)?,
            (PropColumn::U32(col), Prop::U32(v)) => col.set(index, v)?,
            (PropColumn::U64(col), Prop::U64(v)) => col.set(index, v)?,
            (PropColumn::F32(col), Prop::F32(v)) => col.set(index, v)?,
            (PropColumn::F64(col), Prop::F64(v)) => col.set(index, v)?,
            (PropColumn::Str(col), Prop::Str(v)) => col.set(index, v)?,
            #[cfg(feature = "arrow")]
            (PropColumn::Array(col), Prop::Array(v)) => col.set(index, v)?,
            (PropColumn::U8(col), Prop::U8(v)) => col.set(index, v)?,
            (PropColumn::U16(col), Prop::U16(v)) => col.set(index, v)?,
            (PropColumn::I32(col), Prop::I32(v)) => col.set(index, v)?,
            (PropColumn::List(col), Prop::List(v)) => col.set(index, v)?,
            (PropColumn::Map(col), Prop::Map(v)) => col.set(index, v)?,
            (PropColumn::NDTime(col), Prop::NDTime(v)) => col.set(index, v)?,
            (PropColumn::DTime(col), Prop::DTime(v)) => col.set(index, v)?,
            (PropColumn::Decimal(col), Prop::Decimal(v)) => col.set(index, v)?,
            (col, prop) => {
                Err(IllegalPropType {
                    expected: col.dtype(),
                    actual: prop.dtype(),
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn push(&mut self, prop: Prop) -> Result<(), IllegalPropType> {
        self.init_empty_col(&prop);
        match (self, prop) {
            (PropColumn::Bool(col), Prop::Bool(v)) => col.push(Some(v)),
            (PropColumn::U8(col), Prop::U8(v)) => col.push(Some(v)),
            (PropColumn::I64(col), Prop::I64(v)) => col.push(Some(v)),
            (PropColumn::U32(col), Prop::U32(v)) => col.push(Some(v)),
            (PropColumn::U64(col), Prop::U64(v)) => col.push(Some(v)),
            (PropColumn::F32(col), Prop::F32(v)) => col.push(Some(v)),
            (PropColumn::F64(col), Prop::F64(v)) => col.push(Some(v)),
            (PropColumn::Str(col), Prop::Str(v)) => col.push(Some(v)),
            #[cfg(feature = "arrow")]
            (PropColumn::Array(col), Prop::Array(v)) => col.push(Some(v)),
            (PropColumn::U16(col), Prop::U16(v)) => col.push(Some(v)),
            (PropColumn::I32(col), Prop::I32(v)) => col.push(Some(v)),
            (PropColumn::List(col), Prop::List(v)) => col.push(Some(v)),
            (PropColumn::Map(col), Prop::Map(v)) => col.push(Some(v)),
            (PropColumn::NDTime(col), Prop::NDTime(v)) => col.push(Some(v)),
            (PropColumn::DTime(col), Prop::DTime(v)) => col.push(Some(v)),
            (PropColumn::Decimal(col), Prop::Decimal(v)) => col.push(Some(v)),
            (col, prop) => {
                return Err(IllegalPropType {
                    expected: col.dtype(),
                    actual: prop.dtype(),
                })
            }
        }
        Ok(())
    }

    fn init_empty_col(&mut self, prop: &Prop) {
        if let PropColumn::Empty(len) = self {
            match prop {
                Prop::Bool(_) => *self = PropColumn::Bool(LazyVec::with_len(*len)),
                Prop::I64(_) => *self = PropColumn::I64(LazyVec::with_len(*len)),
                Prop::U32(_) => *self = PropColumn::U32(LazyVec::with_len(*len)),
                Prop::U64(_) => *self = PropColumn::U64(LazyVec::with_len(*len)),
                Prop::F32(_) => *self = PropColumn::F32(LazyVec::with_len(*len)),
                Prop::F64(_) => *self = PropColumn::F64(LazyVec::with_len(*len)),
                Prop::Str(_) => *self = PropColumn::Str(LazyVec::with_len(*len)),
                #[cfg(feature = "arrow")]
                Prop::Array(_) => *self = PropColumn::Array(LazyVec::with_len(*len)),
                Prop::U8(_) => *self = PropColumn::U8(LazyVec::with_len(*len)),
                Prop::U16(_) => *self = PropColumn::U16(LazyVec::with_len(*len)),
                Prop::I32(_) => *self = PropColumn::I32(LazyVec::with_len(*len)),
                Prop::List(_) => *self = PropColumn::List(LazyVec::with_len(*len)),
                Prop::Map(_) => *self = PropColumn::Map(LazyVec::with_len(*len)),
                Prop::NDTime(_) => *self = PropColumn::NDTime(LazyVec::with_len(*len)),
                Prop::DTime(_) => *self = PropColumn::DTime(LazyVec::with_len(*len)),
                Prop::Decimal(_) => *self = PropColumn::Decimal(LazyVec::with_len(*len)),
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, PropColumn::Empty(_))
    }

    pub(crate) fn push_null(&mut self) {
        match self {
            PropColumn::Bool(col) => col.push(None),
            PropColumn::I64(col) => col.push(None),
            PropColumn::U32(col) => col.push(None),
            PropColumn::U64(col) => col.push(None),
            PropColumn::F32(col) => col.push(None),
            PropColumn::F64(col) => col.push(None),
            PropColumn::Str(col) => col.push(None),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.push(None),
            PropColumn::U8(col) => col.push(None),
            PropColumn::U16(col) => col.push(None),
            PropColumn::I32(col) => col.push(None),
            PropColumn::List(col) => col.push(None),
            PropColumn::Map(col) => col.push(None),
            PropColumn::NDTime(col) => col.push(None),
            PropColumn::DTime(col) => col.push(None),
            PropColumn::Decimal(col) => col.push(None),
            PropColumn::Empty(count) => {
                *count += 1;
            }
        }
    }

    pub fn get(&self, index: usize) -> Option<Prop> {
        match self {
            PropColumn::Bool(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::I64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::F32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::F64(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::Str(col) => col.get_opt(index).map(|prop| prop.into()),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.get_opt(index).map(|prop| Prop::Array(prop.clone())),
            PropColumn::U8(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::U16(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::I32(col) => col.get_opt(index).map(|prop| (*prop).into()),
            PropColumn::List(col) => col.get_opt(index).map(|prop| Prop::List(prop.clone())),
            PropColumn::Map(col) => col.get_opt(index).map(|prop| Prop::Map(prop.clone())),
            PropColumn::NDTime(col) => col.get_opt(index).map(|prop| Prop::NDTime(*prop)),
            PropColumn::DTime(col) => col.get_opt(index).map(|prop| Prop::DTime(*prop)),
            PropColumn::Decimal(col) => col.get_opt(index).map(|prop| Prop::Decimal(prop.clone())),
            PropColumn::Empty(_) => None,
        }
    }

    pub fn get_ref(&self, index: usize) -> Option<PropRef<'_>> {
        match self {
            PropColumn::Bool(col) => col.get_opt(index).map(|prop| PropRef::Bool(*prop)),
            PropColumn::I64(col) => col.get_opt(index).map(|prop| PropRef::I64(*prop)),
            PropColumn::U32(col) => col.get_opt(index).map(|prop| PropRef::U32(*prop)),
            PropColumn::U64(col) => col.get_opt(index).map(|prop| PropRef::U64(*prop)),
            PropColumn::F32(col) => col.get_opt(index).map(|prop| PropRef::F32(*prop)),
            PropColumn::F64(col) => col.get_opt(index).map(|prop| PropRef::F64(*prop)),
            PropColumn::Str(col) => col.get_opt(index).map(|prop| PropRef::Str(prop.as_ref())),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.get_opt(index).map(PropRef::Array),
            PropColumn::U8(col) => col.get_opt(index).map(|prop| PropRef::U8(*prop)),
            PropColumn::U16(col) => col.get_opt(index).map(|prop| PropRef::U16(*prop)),
            PropColumn::I32(col) => col.get_opt(index).map(|prop| PropRef::I32(*prop)),
            PropColumn::List(col) => col.get_opt(index).map(PropRef::List),
            PropColumn::Map(col) => col.get_opt(index).map(PropRef::Map),
            PropColumn::NDTime(col) => col.get_opt(index).map(PropRef::NDTime),
            PropColumn::DTime(col) => col.get_opt(index).map(PropRef::DTime),
            PropColumn::Decimal(col) => col.get_opt(index).map(PropRef::Decimal),
            PropColumn::Empty(_) => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            PropColumn::Bool(col) => col.len(),
            PropColumn::I64(col) => col.len(),
            PropColumn::U32(col) => col.len(),
            PropColumn::U64(col) => col.len(),
            PropColumn::F32(col) => col.len(),
            PropColumn::F64(col) => col.len(),
            PropColumn::Str(col) => col.len(),
            #[cfg(feature = "arrow")]
            PropColumn::Array(col) => col.len(),
            PropColumn::U8(col) => col.len(),
            PropColumn::U16(col) => col.len(),
            PropColumn::I32(col) => col.len(),
            PropColumn::List(col) => col.len(),
            PropColumn::Map(col) => col.len(),
            PropColumn::NDTime(col) => col.len(),
            PropColumn::DTime(col) => col.len(),
            PropColumn::Decimal(col) => col.len(),
            PropColumn::Empty(count) => *count,
        }
    }
}

#[cfg(test)]
mod test {
    use super::TColumns;
    use raphtory_api::core::entities::properties::prop::Prop;

    #[test]
    fn tcolumns_append_1() {
        let mut t_cols = TColumns::default();

        t_cols.push([(1, Prop::U64(1))]).unwrap();

        let col0 = t_cols.get(0).unwrap();
        let col1 = t_cols.get(1).unwrap();

        assert_eq!(col0.len(), 1);
        assert_eq!(col1.len(), 1);
    }

    #[test]
    fn tcolumns_append_3_rows() {
        let mut t_cols = TColumns::default();

        t_cols
            .push([(1, Prop::U64(1)), (0, Prop::Str("a".into()))])
            .unwrap();
        t_cols
            .push([(0, Prop::Str("c".into())), (2, Prop::I64(9))])
            .unwrap();
        t_cols
            .push([(1, Prop::U64(1)), (3, Prop::Str("c".into()))])
            .unwrap();

        assert_eq!(t_cols.len(), 3);

        for col_id in 0..4 {
            let col = t_cols.get(col_id).unwrap();
            assert_eq!(col.len(), 3);
        }

        let col0 = (0..3)
            .map(|row| t_cols.get(0).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col0,
            vec![
                Some(Prop::Str("a".into())),
                Some(Prop::Str("c".into())),
                None
            ]
        );

        let col1 = (0..3)
            .map(|row| t_cols.get(1).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col1, vec![Some(Prop::U64(1)), None, Some(Prop::U64(1))]);

        let col2 = (0..3)
            .map(|row| t_cols.get(2).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col2, vec![None, Some(Prop::I64(9)), None]);

        let col3 = (0..3)
            .map(|row| t_cols.get(3).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(col3, vec![None, None, Some(Prop::Str("c".into()))]);
    }

    #[test]
    fn tcolumns_append_2_columns_12_items() {
        let mut t_cols = TColumns::default();

        for value in 0..12 {
            if value % 2 == 0 {
                t_cols
                    .push([
                        (1, Prop::U64(value)),
                        (0, Prop::Str(value.to_string().into())),
                    ])
                    .unwrap();
            } else {
                t_cols.push([(1, Prop::U64(value))]).unwrap();
            }
        }

        assert_eq!(t_cols.len(), 12);

        let col0 = (0..12)
            .map(|row| t_cols.get(0).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col0,
            vec![
                Some(Prop::Str("0".into())),
                None,
                Some(Prop::Str("2".into())),
                None,
                Some(Prop::Str("4".into())),
                None,
                Some(Prop::Str("6".into())),
                None,
                Some(Prop::Str("8".into())),
                None,
                Some(Prop::Str("10".into())),
                None
            ]
        );

        let col1 = (0..12)
            .map(|row| t_cols.get(1).and_then(|col| col.get(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            col1,
            vec![
                Some(Prop::U64(0)),
                Some(Prop::U64(1)),
                Some(Prop::U64(2)),
                Some(Prop::U64(3)),
                Some(Prop::U64(4)),
                Some(Prop::U64(5)),
                Some(Prop::U64(6)),
                Some(Prop::U64(7)),
                Some(Prop::U64(8)),
                Some(Prop::U64(9)),
                Some(Prop::U64(10)),
                Some(Prop::U64(11))
            ]
        );
    }
}
