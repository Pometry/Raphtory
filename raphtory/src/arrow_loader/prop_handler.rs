use crate::{arrow_loader::dataframe::DFChunk, errors::GraphError};
use arrow::array::{Array, ArrayRef};
use raphtory_api::core::{
    entities::{
        properties::{
            meta::Meta,
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
    pub fn populated_prop_ids(&self) -> Vec<usize> {
        self.prop_ids
            .iter()
            .zip(self.cols.iter())
            .filter(|(_, col)| (0..self.len).any(|row| col.get_ref(row).is_some()))
            .map(|(id, _)| *id)
            .collect()
    }

    /// The exact `(layer, prop_id)` pairs this chunk populates. May scan the entire chunk,
    /// but short-circuits when a property is seen in all `distinct_layers`.
    pub fn populated_layer_prop_pairs(
        &self,
        layer_per_row: Option<&[usize]>,
        distinct_layers: &[LayerId],
    ) -> Vec<(LayerId, usize)> {
        // A single layer needs no per-row attribution: every value in a populated
        // column necessarily belongs to that one layer.
        if distinct_layers.len() <= 1 {
            let Some(&layer) = distinct_layers.first() else {
                return Vec::new();
            };
            return self
                .populated_prop_ids()
                .into_iter()
                .map(|id| (layer, id))
                .collect();
        }
        let Some(rows) = layer_per_row else {
            // `distinct` can only exceed one layer when a layer column exists
            return Vec::new();
        };

        // Both come from the same chunk, so this holds structurally. It matters:
        // a layer column shorter than the chunk would leave rows unscanned and could under-mark the
        // per-layer property presence bitset, potentially leading to skipped layers which contain data.
        debug_assert_eq!(
            rows.len(),
            self.len,
            "layer column must cover every row in the chunk"
        );

        let layer_width = distinct_layers
            .iter()
            .map(|l| l.0)
            .max()
            .map_or(0, |m| m + 1);
        let mut out = Vec::new();
        let mut seen = vec![false; layer_width];
        // check to see which layers each prop belongs to; i.e. collect unique (layer, prop) pairs
        for (&prop_id, col) in self.prop_ids.iter().zip(self.cols.iter()) {
            seen.iter_mut().for_each(|s| *s = false);
            let mut found = 0;
            for (row, &layer) in rows.iter().enumerate().take(self.len) {
                if found == distinct_layers.len() {
                    break; // present in every layer it could be; no point scanning on
                }
                if !seen[layer] && col.get_ref(row).is_some() {
                    seen[layer] = true;
                    found += 1;
                    out.push((LayerId(layer), prop_id));
                }
            }
        }
        out
    }
}

/// Mark every `(layer, prop)` pair this chunk may write, once, before any rows
/// are appended.
pub fn mark_chunk_prop_presence(
    node_or_edge_meta: &Meta,
    layer_per_row: Option<&[usize]>,
    default_layer: LayerId,
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
    let distinct = distinct_layers(layer_per_row, default_layer);

    node_or_edge_meta
        .temporal_prop_mapper()
        .mark_prop_layer_pairs(t_props.populated_layer_prop_pairs(layer_per_row, &distinct));

    let mut metadata_pairs = metadata.populated_layer_prop_pairs(layer_per_row, &distinct);
    // shared metadata is on every row, so it is present in every layer here
    for &layer in &distinct {
        metadata_pairs.extend(shared_metadata_ids.iter().map(|&id| (layer, id)));
    }
    node_or_edge_meta
        .metadata_mapper()
        .mark_prop_layer_pairs(metadata_pairs);
}

/// The distinct layers a chunk writes to, from its per-row layer column.
/// One pass indexed by layer id.
fn distinct_layers(layer_per_row: Option<&[usize]>, default_layer: LayerId) -> Vec<LayerId> {
    let Some(rows) = layer_per_row else {
        return vec![default_layer];
    };
    let Some(&max_layer) = rows.iter().max() else {
        return Vec::new(); // empty chunk writes nothing
    };
    let mut seen = vec![false; max_layer + 1];
    for &l in rows {
        seen[l] = true;
    }
    seen.iter()
        .enumerate()
        .filter(|(_, &s)| s)
        .map(|(i, _)| LayerId(i))
        .collect()
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
