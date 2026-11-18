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
    /// Sorted segments with no answer, as are segments past the bitmaps: check every id.
    pub scan_nodes: Vec<usize>,
    pub scan_edges: Vec<usize>,
}

impl WindowCandidates {
    /// Moves every id of the unanswered segments into `maybe_*`; segments are `(id, len)`.
    pub fn scan_unanswered(
        mut self,
        nodes: impl Iterator<Item = (usize, u32)>,
        edges: impl Iterator<Item = (usize, u32)>,
    ) -> Self {
        fn fill(
            maybe: &mut SegmentBitmaps,
            scan: &[usize],
            segments: impl Iterator<Item = (usize, u32)>,
        ) {
            let answered = maybe.segments.len();
            for (id, len) in segments {
                if id >= answered || scan.binary_search(&id).is_ok() {
                    if id >= maybe.segments.len() {
                        maybe.segments.resize(id + 1, RoaringBitmap::new());
                    }
                    maybe.segments[id].insert_range(0..len);
                }
            }
        }
        fill(&mut self.maybe_nodes, &self.scan_nodes, nodes);
        fill(&mut self.maybe_edges, &self.scan_edges, edges);
        self
    }
}
