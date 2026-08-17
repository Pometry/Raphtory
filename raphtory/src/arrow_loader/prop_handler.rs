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
    ///
    /// An all-null column says nothing about presence, so it must not be marked.
    /// Short-circuits on the first row with a value, so a populated column costs
    /// O(1); only an entirely empty column pays a full scan.
    ///
    /// Deliberately goes through `get_ref` rather than `as_array`: the latter
    /// materialises an arrow array, and `MapCol::as_array` panics for
    /// empty-map columns that carry a validity buffer.
    pub fn populated_prop_ids(&self) -> Vec<usize> {
        self.prop_ids
            .iter()
            .zip(self.cols.iter())
            .filter(|(_, col)| (0..self.len).any(|row| col.get_ref(row).is_some()))
            .map(|(id, _)| *id)
            .collect()
    }
}

/// Mark every `(layer, prop)` pair this chunk may write, once, before any rows
/// are appended.
///
/// Bulk loading appends one property per column per row, so testing presence per
/// append costs ~14ns x rows x columns -- around 1e9 checks on a wide dataset --
/// to record at most a few thousand distinct bits. The resolved prop ids and the
/// layers involved are both known per chunk, so the whole set is marked here and
/// the append path uses the `_bulk` writers, which skip the check entirely.
///
/// `layers` is the resolved per-row layer column, or `None` when every row goes
/// to `default_layer`.
///
/// When the chunk touches a single layer -- the common case, including every
/// `load_edges(layer=...)` call and all node loading -- the marking is exact: a
/// column is only marked if it holds at least one value (see
/// [`PropCols::populated_prop_ids`]). When a chunk spans several layers, the union of
/// (layers x non-null props) is marked instead, since attributing a column's
/// non-null rows to individual layers would need the per-row scan this function
/// exists to avoid. That over-marks only in the multi-layer case, and a false
/// positive merely costs a missed layer-skip. A false negative would be unsound,
/// and this cannot produce one.
///
/// `shared_metadata_ids` covers constant metadata applied to every row, which is
/// resolved outside the chunk's columns and so is not part of `metadata`. It is
/// marked unconditionally: a shared value is present on every row by definition.
pub fn mark_chunk_prop_presence(
    node_or_edge_meta: &Meta,
    layers: Option<&[usize]>,
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
    let layer_ids: Vec<LayerId> = match layers {
        // one pass, indexed by layer id: layer counts are tiny, row counts are not
        Some(rows) => {
            let mut seen = vec![false; rows.iter().copied().max().map_or(0, |m| m + 1)];
            for &l in rows {
                seen[l] = true;
            }
            seen.iter()
                .enumerate()
                .filter(|(_, &s)| s)
                .map(|(i, _)| LayerId(i))
                .collect()
        }
        None => vec![default_layer],
    };

    node_or_edge_meta
        .temporal_prop_mapper()
        .mark_props_in_layers(layer_ids.iter().copied(), &t_props.populated_prop_ids());

    let mut const_ids = metadata.populated_prop_ids();
    const_ids.extend_from_slice(shared_metadata_ids);
    node_or_edge_meta
        .metadata_mapper()
        .mark_props_in_layers(layer_ids.iter().copied(), &const_ids);
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
