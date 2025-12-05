use crate::{
    LocalPOS,
    error::StorageError,
    segments::{HasRow, SegmentContainer},
};
use raphtory_api::core::entities::properties::{meta::Meta, prop::Prop};
use raphtory_core::{
    entities::properties::tprop::TPropCell,
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::sync::Arc;

/// In-memory segment that contains graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct MemGraphPropSegment {
    /// Layers containing graph properties and metadata.
    layers: Vec<SegmentContainer<UnitEntry>>,
}

/// A unit-like struct for use with `SegmentContainer`.
/// Graph properties and metadata are already stored in `SegmentContainer`,
/// hence this struct is empty.
#[derive(Debug, Default)]
pub struct UnitEntry(usize);

// `UnitEntry` does not store data, but `HasRow has to be implemented
// for SegmentContainer to work.
impl HasRow for UnitEntry {
    fn row(&self) -> usize {
        self.0
    }

    fn row_mut(&mut self) -> &mut usize {
        &mut self.0
    }
}

impl MemGraphPropSegment {
    /// Graph segments only have a single row.
    pub const DEFAULT_ROW: usize = 0;

    /// Graph segments are currently only written to a single layer.
    pub const DEFAULT_LAYER: usize = 0;

    pub fn new_with_meta(meta: Arc<Meta>) -> Self {
        // Technically, these aren't used since there is always only one graph segment.
        let segment_id = 0;
        let max_page_len = 1;

        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
        }
    }

    pub fn lsn(&self) -> u64 {
        self.layers.iter().map(|seg| seg.lsn()).min().unwrap_or(0)
    }

    pub fn get_or_create_layer(&mut self, layer_id: usize) -> &mut SegmentContainer<UnitEntry> {
        if layer_id >= self.layers.len() {
            let max_page_len = self.layers[0].max_page_len();
            let segment_id = self.layers[0].segment_id();
            let meta = self.layers[0].meta().clone();
            self.layers.resize_with(layer_id + 1, || {
                SegmentContainer::new(segment_id, max_page_len, meta.clone())
            });
        }
        &mut self.layers[layer_id]
    }

    pub fn layers(&self) -> &Vec<SegmentContainer<UnitEntry>> {
        &self.layers
    }

    pub fn layers_mut(&mut self) -> &mut Vec<SegmentContainer<UnitEntry>> {
        &mut self.layers
    }

    pub fn is_empty(&self) -> bool {
        self.layers.iter().all(|layer| layer.est_size() == 0)
    }

    /// Replaces this segment with an empty instance, returning the old segment
    /// with its data.
    ///
    /// The new segment will have the same number of layers as the original.
    pub fn take(&mut self) -> Self {
        let layers = self.layers.iter_mut().map(|layer| layer.take()).collect();

        Self { layers }
    }

    pub fn add_properties<T: AsTime>(
        &mut self,
        t: T,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> usize {
        let layer = self.get_or_create_layer(Self::DEFAULT_LAYER);
        let est_size = layer.est_size();
        let ts = TimeIndexEntry::new(t.t(), t.i());

        layer.reserve_local_row(Self::DEFAULT_ROW.into());
        let mut prop_mut_entry = layer.properties_mut().get_mut_entry(Self::DEFAULT_ROW);
        prop_mut_entry.append_t_props(ts, props);

        let layer_est_size = layer.est_size();
        layer_est_size - est_size
    }

    pub fn check_metadata(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), StorageError> {
        if let Some(layer) = self.layers.get(Self::DEFAULT_LAYER) {
            layer.check_metadata(Self::DEFAULT_ROW.into(), props)?;
        }

        Ok(())
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) -> usize {
        let segment_container = self.get_or_create_layer(Self::DEFAULT_LAYER);
        let est_size = segment_container.est_size();

        let row = segment_container
            .reserve_local_row(Self::DEFAULT_ROW.into())
            .map(|a| a.row());
        let row = row.inner();
        let mut prop_mut_entry = segment_container.properties_mut().get_mut_entry(row);
        prop_mut_entry.append_const_props(props);

        let layer_est_size = segment_container.est_size();
        // random estimate for constant properties
        (layer_est_size - est_size) + 8
    }

    pub fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>> {
        let layer = &self.layers[Self::DEFAULT_LAYER];

        layer.t_prop(Self::DEFAULT_ROW, prop_id)
    }

    pub fn get_metadata(&self, prop_id: usize) -> Option<Prop> {
        let layer = &self.layers[Self::DEFAULT_LAYER];

        layer.c_prop(Self::DEFAULT_ROW, prop_id)
    }
}
