use crate::{arrow_loader::dataframe::DFChunk, errors::GraphError};
use arrow::array::{Array, ArrayRef};
use raphtory_api::core::{
    entities::{
        properties::{
            meta::{Meta, WriteLockedLayerPresence},
            prop::{
                data_type_as_prop_type,
                prop_col::{lift_property_col, PropCol},
                PropRef, PropType,
            },
        },
        LayerId,
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

    /// Prop ids whose column holds at least one value in this chunk.
    /// Only an entirely empty column pays a full scan.
    pub fn populated_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.prop_ids
            .iter()
            .zip(self.cols.iter())
            .filter(|(_, col)| !col.is_all_null())
            .map(|(id, _)| *id)
    }

    /// The exact `(layer, prop_id)` pairs this chunk populates. May scan the entire chunk,
    /// but short-circuits when a property is seen in all `distinct_layers`.
    pub fn mark_layer_prop_pairs(
        &self,
        layer_per_row: Option<&[usize]>,
        distinct_layers: &[LayerId],
        mapper: &mut WriteLockedLayerPresence,
    ) {
        // A single layer needs no per-row attribution: every value in a populated
        // column necessarily belongs to that one layer.
        if distinct_layers.len() <= 1 {
            let Some(&layer) = distinct_layers.first() else {
                return;
            };
            for id in self.populated_prop_ids() {
                mapper.mark(layer, id);
            }
            return;
        }
        let Some(layer_ids) = layer_per_row else {
            // `distinct` can only exceed one layer when a layer column exists
            return;
        };

        // Both come from the same chunk, so this holds structurally. It matters:
        // a layer column shorter than the chunk would leave rows unscanned and could under-mark the
        // per-layer property presence bitset, potentially leading to skipped layers which contain data.
        debug_assert_eq!(
            layer_ids.len(),
            self.len,
            "layer column must cover every row in the chunk"
        );

        // check to see which layers each prop belongs to; i.e. collect unique (layer, prop) pairs
        for (&prop_id, col) in self.prop_ids.iter().zip(self.cols.iter()) {
            let mut found = 0;
            for layer in distinct_layers {
                if mapper.layer_has(*layer, prop_id) {
                    found += 1;
                }
            }
            if found < distinct_layers.len() && !col.is_all_null() {
                for (row, &layer) in layer_ids.iter().enumerate() {
                    if col.get_ref(row).is_some() && mapper.mark(LayerId(layer), prop_id) {
                        found += 1;
                    }
                    if found == distinct_layers.len() {
                        break; // present in every layer it could be; no point scanning on
                    }
                }
            }
        }
    }
}

/// Mark every `(layer, prop)` pair this chunk may write, once, before any rows
/// are appended.
pub fn mark_chunk_prop_presence(
    node_or_edge_meta: &Meta,
    layer_per_row: Option<&[usize]>,
    distinct_layers: &[LayerId],
    t_props: &PropCols,
    metadata: &PropCols,
    shared_metadata_ids: &[usize],
) {
    if t_props.prop_ids().is_empty()
        && metadata.prop_ids().is_empty()
        && shared_metadata_ids.is_empty()
    {
        return;
    }

    t_props.mark_layer_prop_pairs(
        layer_per_row,
        distinct_layers,
        &mut node_or_edge_meta
            .temporal_prop_mapper()
            .write_locked_layer_presence(),
    );

    let mut meta_presence = node_or_edge_meta
        .metadata_mapper()
        .write_locked_layer_presence();
    // shared metadata is on every row, so it is present in every layer here
    for &layer in distinct_layers {
        for &id in shared_metadata_ids.iter() {
            meta_presence.mark(layer, id);
        }
    }
    metadata.mark_layer_prop_pairs(layer_per_row, distinct_layers, &mut meta_presence);
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
        .zip(dtypes)
        .map(|(name, dtype)| Ok(prop_id_resolver(name.as_ref(), dtype)?.inner()))
        .collect::<Result<Vec<_>, E>>()?;

    Ok(PropCols {
        prop_ids,
        cols,
        len: df.len(),
    })
}
