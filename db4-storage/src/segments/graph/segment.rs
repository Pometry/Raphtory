use std::sync::Arc;
use crate::segments::{HasRow, SegmentContainer};
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_core::{entities::properties::tprop::TPropCell, storage::timeindex::TimeIndexEntry};
use raphtory_api::core::entities::properties::prop::Prop;

/// In-memory segment that contains graph temporal properties and graph metadata.
#[derive(Debug)]
pub struct MemGraphSegment {
    /// Layers containing graph properties and metadata.
    layers: Vec<SegmentContainer<GraphSegmentEntry>>
}

/// A unit-like struct for use with `SegmentContainer`.
/// Graph properties and metadata are already stored in `SegmentContainer`,
/// hence this struct is empty.
#[derive(Debug, Default)]
pub struct GraphSegmentEntry;

// GraphSegmentEntry does not store data, but HasRow has to be implemented
// for SegmentContainer to work.
impl HasRow for GraphSegmentEntry {
    fn row(&self) -> usize {
        panic!("GraphSegmentEntry does not support row access");
    }

    fn row_mut(&mut self) -> &mut usize {
        panic!("GraphSegmentEntry does not support row access");
    }
}

impl MemGraphSegment {
    /// Graph segments only have a single row.
    pub const ROW: usize = 0;

    /// Graph segments are currently only written to a single layer.
    pub const LAYER: usize = 0;

    pub fn new() -> Self {
        // Technically, these aren't used since there is always only one graph segment.
        let segment_id = 0;
        let max_page_len = 1;
        let meta = Arc::new(Meta::new_for_graph());

        Self {
            layers: vec![SegmentContainer::new(segment_id, max_page_len, meta)],
        }
    }

    pub fn layers(&self) -> &[SegmentContainer<GraphSegmentEntry>] {
        &self.layers
    }

    /// Replaces this segment with an empty instance, returning the old segment
    /// with its data.
    ///
    /// The new segment will have the same number of layers as the original.
    pub fn take(&mut self) -> Self {
        let layers = self.layers.iter_mut().map(|layer| layer.take()).collect();

        Self {
            layers,
        }
    }

    pub fn add_properties(&mut self, t: TimeIndexEntry, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_t_props(t, props);
    }

    pub fn add_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_const_props(props);
    }

    pub fn update_metadata(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        let layer = &mut self.layers[Self::LAYER];

        layer.properties_mut().get_mut_entry(Self::ROW).append_const_props(props);
    }

    pub fn get_temporal_prop(&self, prop_id: usize) -> Option<TPropCell<'_>> {
        let layer = &self.layers[Self::LAYER];

        layer.t_prop(Self::ROW, prop_id)
    }

    pub fn get_metadata(&self, prop_id: usize) -> Option<Prop> {
        let layer = &self.layers[Self::LAYER];

        layer.c_prop(Self::ROW, prop_id)
    }
}
