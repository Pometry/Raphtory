use crate::{arrow_loader::dataframe::DFChunk, errors::GraphError};
use arrow::array::{Array, ArrayRef};
use raphtory_api::core::{
    entities::properties::prop::{
        data_type_as_prop_type,
        prop_col::{lift_property_col, PropCol},
        PropRef, PropType,
    },
    storage::dict_mapper::MaybeNew,
};
use rayon::prelude::*;

pub struct PropCols {
    prop_ids: Vec<usize>,
    cols: Vec<Box<dyn PropCol>>,
    len: usize,
}

impl PropCols {
    pub fn iter_row(&self, i: usize) -> impl Iterator<Item = (usize, PropRef<'_>)> + '_ {
        self.prop_ids
            .iter()
            .zip(self.cols.iter())
            .filter_map(move |(id, col)| col.get_ref(i).map(|v| (*id, v)))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn par_rows(
        &self,
    ) -> impl IndexedParallelIterator<Item = impl Iterator<Item = (usize, PropRef<'_>)> + '_> + '_
    {
        (0..self.len()).into_par_iter().map(|i| self.iter_row(i))
    }

    pub fn prop_ids(&self) -> &[usize] {
        &self.prop_ids
    }

    pub fn cols(&self) -> Vec<ArrayRef> {
        self.cols.iter().map(|col| col.as_array()).collect()
    }
}

pub fn combine_properties_arrow<E>(
    props: &[impl AsRef<str>],
    indices: &[usize],
    df: &DFChunk,
    prop_id_resolver: impl Fn(&str, PropType) -> Result<MaybeNew<usize>, E>,
) -> Result<PropCols, GraphError>
where
    GraphError: From<E>,
{
    let dtypes = indices
        .iter()
        .map(|idx| data_type_as_prop_type(df.chunk[*idx].data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let cols = indices
        .iter()
        .map(|idx| lift_property_col(&df.chunk[*idx]))
        .collect::<Vec<_>>();
    let prop_ids = props
        .iter()
        .zip(dtypes.into_iter())
        .map(|(name, dtype)| Ok(prop_id_resolver(name.as_ref(), dtype)?.inner()))
        .collect::<Result<Vec<_>, E>>()?;

    Ok(PropCols {
        prop_ids,
        cols,
        len: df.len(),
    })
}
