use roaring::RoaringBitmap;

/// Id set as per-segment bitmaps; segment `i` holds ids `i * stride..(i + 1) * stride`.
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

    /// Ascending.
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

/// `sure_*` are in the window, nothing outside `sure_* ∪ maybe_*` is; the two are disjoint.
#[derive(Debug, Clone, Default)]
pub struct WindowCandidates {
    pub sure_nodes: SegmentBitmaps,
    pub maybe_nodes: SegmentBitmaps,
    pub sure_edges: SegmentBitmaps,
    pub maybe_edges: SegmentBitmaps,
}
