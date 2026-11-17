use roaring::RoaringBitmap;

/// A set of node or edge ids, held as one bitmap of local positions per
/// segment. Segment `i` covers ids `i * stride..(i + 1) * stride`.
#[derive(Debug, Clone, Default)]
pub struct SegmentBitmaps {
    pub stride: usize,
    pub segments: Vec<RoaringBitmap>,
}

impl SegmentBitmaps {
    pub fn contains(&self, id: usize) -> bool {
        self.segments
            .get(id / self.stride)
            .is_some_and(|segment| segment.contains((id % self.stride) as u32))
    }

    pub fn len(&self) -> usize {
        self.segments.iter().map(|s| s.len() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.iter().all(|s| s.is_empty())
    }

    /// Ids in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.segments
            .iter()
            .enumerate()
            .flat_map(move |(i, segment)| {
                segment
                    .iter()
                    .map(move |pos| i * self.stride + pos as usize)
            })
    }
}

/// What a time index knows about the nodes and edges of a window over some
/// layers of the whole graph: every id in `sure_*` is in the view, no id outside
/// `sure_* ∪ maybe_*` is, and the two are disjoint. The caller checks `maybe_*`.
#[derive(Debug, Clone, Default)]
pub struct WindowCandidates {
    pub sure_nodes: SegmentBitmaps,
    pub maybe_nodes: SegmentBitmaps,
    pub sure_edges: SegmentBitmaps,
    pub maybe_edges: SegmentBitmaps,
}
