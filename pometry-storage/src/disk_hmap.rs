use crate::arrow2::{
    array::{Array, ListArray, PrimitiveArray, Utf8Array},
    datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
    offset::{Offsets, OffsetsBuffer},
    record_batch::RecordBatch as Chunk,
};
use std::{hash::Hash, io::Write, path::Path};

#[derive(Clone, Debug, PartialEq)]
pub struct DiskHashMap {
    offsets: OffsetsBuffer<i64>,
    values: PrimitiveArray<i64>,
    gids: GIDArray,
}

impl DiskHashMap {
    pub fn write(&self, file: impl Write) -> Result<(), super::RAError> {
        let list_vals_dt = DataType::LargeList(Box::new(Field::new(
            "item",
            self.values.data_type().clone(),
            false,
        )));

        let gids = self.gids.to_boxed();

        let schema = Schema::from(vec![
            Field::new("values", list_vals_dt.clone(), false),
            Field::new("gids", gids.data_type().clone(), false),
        ]);

        let values = ListArray::new(
            list_vals_dt,
            self.offsets.clone(),
            self.values.clone().boxed(),
            None,
        )
        .boxed();
        let chunk = Chunk::new(vec![values, gids]);
        write_batches(file, schema, &[chunk])?;
        Ok(())
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, super::RAError> {
        let chunk = unsafe { mmap_batch(path, 0)? };
        let mut arrays = chunk.into_arrays();

        if arrays.len() != 2 {
            return Err(super::RAError::ColumnNotFound(
                "expected 2 columns for arrow hash map".to_string(),
            ));
        }

        let gids = GIDArray::try_from(arrays.pop().unwrap())?;
        let values = arrays.pop().unwrap();

        let list_values = values.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        let offsets = list_values.offsets().clone();
        let values = list_values
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap()
            .clone();

        Ok(Self {
            offsets,
            values,
            gids,
        })
    }

    #[instrument(level = "debug", skip(sorted_gids))]
    pub fn from_sorted_dedup(sorted_gids: Box<dyn Array>) -> Result<Self, super::RAError> {
        let cap = sorted_gids.len();
        // get the positions of each key
        let mut positions = vec![0u64; sorted_gids.len()];
        if let Some(arr) = sorted_gids.as_any().downcast_ref::<PrimitiveArray<u64>>() {
            arr.values()
                .par_iter()
                .map(|t| position(t, cap))
                .collect_into_vec(&mut positions);
        } else if let Some(arr) = sorted_gids.as_any().downcast_ref::<PrimitiveArray<i64>>() {
            arr.values()
                .par_iter()
                .map(|t| position(t, cap))
                .collect_into_vec(&mut positions);
        } else if let Some(arr) = sorted_gids.as_any().downcast_ref::<Utf8Array<i32>>() {
            (0..arr.len())
                .into_par_iter()
                .map(|i| position(arr.value(i), cap))
                .collect_into_vec(&mut positions);
        } else if let Some(arr) = sorted_gids.as_any().downcast_ref::<Utf8Array<i64>>() {
            (0..arr.len())
                .into_par_iter()
                .map(|i| position(arr.value(i), cap))
                .collect_into_vec(&mut positions);
        }

        let mut indices = (0i64..positions.len() as i64).collect_vec();
        indices.par_sort_by_key(|i| positions[*i as usize]);

        let index_col = PrimitiveArray::from_vec(indices);
        let gids = compute::take(sorted_gids.as_ref(), &index_col)?;

        let mut offsets = vec![0usize; cap + 1];

        let atomic_offsets = atomic_usize_from_mut_slice(&mut offsets[1..]);

        positions.par_iter().for_each(|pos| {
            atomic_offsets[*pos as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });

        par_cum_sum(&mut offsets);

        let offsets: Vec<_> = offsets.into_iter().map(|t| t as i64).collect();

        let offsets: OffsetsBuffer<i64> = Offsets::try_from(offsets)?.into();
        let gids = GIDArray::try_from(gids)?;

        Ok(Self {
            offsets,
            values: index_col,
            gids,
        })
    }
}

impl GlobalOrder for DiskHashMap {
    fn len(&self) -> usize {
        self.values.values().len()
    }

    fn id_type(&self) -> GidType {
        self.gids.id_type()
    }

    fn contains<'a, Ref: Into<GidRef<'a>>>(&'a self, gid: Ref) -> bool {
        let gid = gid.into();
        if self.len() == 0 {
            return false;
        }
        match &self.gids {
            GIDArray::I64(arr) => match gid {
                GidRef::U64(gid) => {
                    let pos = position(&gid, self.values.len()) as usize;
                    let (start, end) = self.offsets.start_end(pos);
                    let gid_i64 = gid as i64;
                    arr.values()[start..end].contains(&gid_i64)
                }
                _ => false,
            },
            GIDArray::U64(arr) => match gid {
                GidRef::U64(gid) => {
                    let pos = position(&gid, self.values.len()) as usize;
                    let (start, end) = self.offsets.start_end(pos);
                    arr.values()[start..end].contains(&gid)
                }
                _ => false,
            },
            GIDArray::Str32(arr) => match gid {
                GidRef::Str(gid) => {
                    let pos = position(&gid, self.values.len()) as usize;
                    let (start, end) = self.offsets.start_end(pos);
                    (start..end).any(|i| arr.value(i) == gid)
                }
                _ => false,
            },
            GIDArray::Str64(arr) => match gid {
                GidRef::Str(gid) => {
                    let pos = position(&gid, self.values.len()) as usize;
                    let (start, end) = self.offsets.start_end(pos);
                    (start..end).any(|i| arr.value(i) == gid)
                }
                _ => false,
            },
            GIDArray::StrView(arr) => match gid {
                GidRef::Str(gid) => {
                    let pos = position(&gid, self.values.len()) as usize;
                    let (start, end) = self.offsets.start_end(pos);
                    (start..end).any(|i| arr.value(i) == gid)
                }
                _ => false,
            },
        }
    }

    fn find<'a, Ref: Into<GidRef<'a>>>(&'a self, gid: Ref) -> Option<usize> {
        let gid = gid.into();
        if self.len() == 0 {
            return None;
        }
        match &self.gids {
            GIDArray::I64(arr) => {
                let find_gid = gid.to_i64()?;
                let pos = position(&find_gid, self.values.len()) as usize;
                let (start, end) = self.offsets.start_end(pos);
                (arr.values()[start..end].iter())
                    .zip(&self.values.values()[start..end])
                    .find(|(&gid, _)| gid == find_gid)
                    .map(|(_, i)| *i as usize)
            }
            GIDArray::U64(arr) => {
                let find_gid = gid.to_u64()?;
                let pos = position(&find_gid, self.values.len()) as usize;
                let (start, end) = self.offsets.start_end(pos);
                (arr.values()[start..end].iter())
                    .zip(&self.values.values()[start..end])
                    .find(|(&gid, _)| gid == find_gid)
                    .map(|(_, i)| *i as usize)
            }
            GIDArray::Str32(arr) => {
                let find_gid: &str = &gid.to_str();
                let pos = position(&find_gid, self.values.len()) as usize;
                let (start, end) = self.offsets.start_end(pos);
                (start..end)
                    .zip(&self.values.values()[start..end])
                    .find(|(gid_id, _)| arr.value(*gid_id) == find_gid)
                    .map(|(_, i)| *i as usize)
            }
            GIDArray::Str64(arr) => {
                let find_gid: &str = &gid.to_str();
                let pos = position(&find_gid, self.values.len()) as usize;
                let (start, end) = self.offsets.start_end(pos);
                (start..end)
                    .zip(&self.values.values()[start..end])
                    .find(|(gid_id, _)| arr.value(*gid_id) == find_gid)
                    .map(|(_, i)| *i as usize)
            }
            GIDArray::StrView(arr) => {
                let find_gid: &str = &gid.to_str();
                let pos = position(&find_gid, self.values.len()) as usize;
                let (start, end) = self.offsets.start_end(pos);
                (start..end)
                    .zip(&self.values.values()[start..end])
                    .find(|(gid_id, _)| arr.value(*gid_id) == find_gid)
                    .map(|(_, i)| *i as usize)
            }
        }
    }
}

use crate::{
    global_order::{GIDArray, GidType},
    utils::calculate_hash,
    GidRef,
};
use itertools::Itertools;
use raphtory_api::{atomic_extra::atomic_usize_from_mut_slice, compute::par_cum_sum};
use rayon::prelude::*;
use tracing::instrument;

use super::{
    compute,
    global_order::GlobalOrder,
    load::mmap::{mmap_batch, write_batches},
};

fn position<T: Hash + std::fmt::Debug + ?Sized>(t: &T, cap: usize) -> u64 {
    calculate_hash(t) % (cap as u64)
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use crate::GID;

    use super::*;
    use proptest::{prelude::*, sample::size_range};

    fn check_arrow_map(ahm: DiskHashMap, hm: HashMap<GID, usize>) {
        assert_eq!(ahm.len(), hm.len());
        for (gid, pos) in hm.iter() {
            assert_eq!(ahm.find(gid), Some(*pos));
        }

        let temp_file = tempfile::NamedTempFile::new().unwrap();
        ahm.write(&temp_file).unwrap();

        let ahm = DiskHashMap::read(temp_file.path()).unwrap();

        assert_eq!(ahm.len(), hm.len());
        for (gid, pos) in hm.iter() {
            assert_eq!(ahm.find(gid), Some(*pos));
        }
    }

    #[test]
    fn test_one() {
        let hm: HashMap<GID, usize> = [(GID::U64(1), 0)].into();
        let map = DiskHashMap::from_sorted_dedup(Box::new(PrimitiveArray::from_slice(vec![1u64])))
            .unwrap();
        check_arrow_map(map, hm);
    }

    #[test]
    fn test_two() {
        let hm: HashMap<GID, usize> = [(GID::U64(0), 0), (GID::U64(1), 1)].into();
        let map =
            DiskHashMap::from_sorted_dedup(Box::new(PrimitiveArray::from_slice(vec![0u64, 1])))
                .unwrap();
        check_arrow_map(map, hm);
    }

    proptest! {
        #[test]
        fn test_u64_hm(ts in any_with::<Vec<u64>>(size_range(1..=100).lift()).prop_map(|mut v| {v.sort(); v.dedup(); v})) {
            let hm: HashMap<GID, usize> = ts.iter().copied().map(GID::U64).enumerate().map(|(i, gid)| (gid, i)).collect();
            let map = DiskHashMap::from_sorted_dedup(Box::new(PrimitiveArray::from_slice(ts))).unwrap();
            check_arrow_map(map, hm);
        }

        #[test]
        fn test_utf8_hm(ts in any_with::<Vec<String>>(size_range(1..=100).lift()).prop_map(|mut v| {v.sort(); v.dedup(); v})) {
            let hm: HashMap<GID, usize> = ts.iter().cloned().map(GID::Str).enumerate().map(|(i, gid)| (gid, i)).collect();
            let map = DiskHashMap::from_sorted_dedup(Box::new(Utf8Array::<i32>::from_slice(ts))).unwrap();
            check_arrow_map(map, hm);
        }
    }
}
