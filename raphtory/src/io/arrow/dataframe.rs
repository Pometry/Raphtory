use crate::core::utils::errors::GraphError;

use polars_arrow::{
    array::{Array, PrimitiveArray, Utf8Array},
    compute::cast::{self, CastOptions},
    datatypes::{ArrowDataType as DataType, TimeUnit},
    offset::Offset,
    types::NativeType,
};

use itertools::Itertools;

#[derive(Debug)]
pub(crate) struct DFView {
    pub(crate) names: Vec<String>,
    pub(crate) arrays: Vec<Vec<Box<dyn Array>>>,
}

impl DFView {
    pub fn check_cols_exist(&self, cols: &[&str]) -> Result<(), GraphError> {
        let non_cols: Vec<&&str> = cols
            .iter()
            .filter(|c| !self.names.contains(&c.to_string()))
            .collect();
        if non_cols.len() > 0 {
            return Err(GraphError::ColumnDoesNotExist(non_cols.iter().join(", ")));
        }

        Ok(())
    }

    pub(crate) fn iter_col<T: NativeType>(
        &self,
        name: &str,
    ) -> Option<impl Iterator<Item = Option<&T>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = arr.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            arr.iter()
        });

        Some(iter)
    }

    pub fn utf8<O: Offset>(&self, name: &str) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;
        // test that it's actually a utf8 array
        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<Utf8Array<O>>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = arr.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
            arr.iter()
        });

        Some(iter)
    }

    pub fn time_iter_col(&self, name: &str) -> Option<impl Iterator<Item = Option<i64>> + '_> {
        let idx = self.names.iter().position(|n| n == name)?;

        let _ = (&self.arrays[0])[idx]
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()?;

        let iter = self.arrays.iter().flat_map(move |arr| {
            let arr = &arr[idx];
            let arr = if let DataType::Timestamp(_, _) = arr.data_type() {
                let array = cast::cast(
                    &*arr.clone(),
                    &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
                    CastOptions::default(),
                )
                .unwrap();
                array
            } else {
                arr.clone()
            };

            let arr = arr.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            arr.clone().into_iter()
        });

        Some(iter)
    }
}
