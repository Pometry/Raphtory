use crate::{
    LocalPOS, api::graph::GraphSegmentOps, error::StorageError, pages::layer_counter::GraphStats,
    segments::graph::segment::MemGraphSegment,
};
use std::ops::DerefMut;

pub struct GraphWriter<'a, MP: DerefMut<Target = MemGraphSegment> + 'a, GS: GraphSegmentOps> {
    pub page: &'a GS,
    pub writer: MP,
}

impl<'a, MP: DerefMut<Target = MemGraphSegment> + 'a, GS: GraphSegmentOps> GraphWriter<'a, MP, GS> {
    pub fn new(page: &'a GS, writer: MP) -> Self {
        Self { page, writer }
    }
}
