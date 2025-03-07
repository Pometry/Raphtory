use std::path::Path;

use polars_arrow::array::PrimitiveArray;
use raphtory_api::core::entities::{EID, VID};

use crate::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        mutable_chunked_array::MutPrimitiveChunkedArray,
    },
    file_prefix::GraphPaths,
    prelude::{ArrayOps, BaseArrayOps},
    RAError,
};

#[derive(Debug, Clone, Default)]
pub struct EdgeList {
    src_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
}

impl EdgeList {
    pub fn from_edges(
        edges: impl IntoIterator<Item = (u64, u64)>,
        chunk_size: usize,
        graph_dir: &Path,
    ) -> Result<Self, RAError> {
        let mut src_ids =
            MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::AdjOutSrcs);

        let mut dst_ids =
            MutPrimitiveChunkedArray::new_persisted(chunk_size, graph_dir, GraphPaths::AdjOutDsts);

        for (src, dst) in edges.into_iter() {
            src_ids.push(src)?;
            dst_ids.push(dst)?;
        }

        Ok(Self::new(src_ids.finish()?, dst_ids.finish()?))
    }

    pub fn new(
        src_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    ) -> Self {
        Self { src_ids, dst_ids }
    }

    pub fn get(&self, eid: EID) -> (VID, VID) {
        (self.get_src(eid), self.get_dst(eid))
    }

    pub fn get_src(&self, eid: EID) -> VID {
        VID(self.src_ids.get(eid.0) as usize)
    }

    pub fn get_dst(&self, eid: EID) -> VID {
        VID(self.dst_ids.get(eid.0) as usize)
    }

    pub(crate) fn all_edges(&self) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.src_ids
            .iter()
            .zip(self.dst_ids.iter())
            .enumerate()
            .map(|(eid, (src_id, dst_id))| (EID(eid), VID(src_id as usize), VID(dst_id as usize)))
    }

    pub(crate) fn len(&self) -> usize {
        self.src_ids.len()
    }

    pub(crate) fn srcs(&self) -> &ChunkedArray<PrimitiveArray<u64>, NonNull> {
        &self.src_ids
    }

    pub(crate) fn dsts(&self) -> &ChunkedArray<PrimitiveArray<u64>, NonNull> {
        &self.dst_ids
    }
}
