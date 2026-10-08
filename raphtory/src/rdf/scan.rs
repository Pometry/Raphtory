//! Lock-free edge scans over a graph view.
//!
//! No Raphtory storage iterator is held across [`Iterator::next`], so writers are never stalled
//! between items and nested reads cannot deadlock. [`EdgeScan`] collects the adjacency of one
//! anchor node at a time.
//!
//! A full scan of a view without node or edge filters (a window is fine) reads the edge storage
//! instead ([`EdgeScan::all`]): a chunk of edge ids is collected under one edge segment's read
//! lock, the lock is released, then each edge is checked against the view. Lock order: writers
//! lock an edge's nodes before its edge segment, so no node is read while a segment is locked.
use crate::{
    db::{
        api::view::{
            internal::{CoreGraphOps, FilterOps, FilterState, InternalLayerOps},
            DynamicGraph, IntoDynamic,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
    rdf::limits::Interrupt,
};
use either::Either;
use raphtory_api::core::{
    entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds, EID, VID},
    storage::arc_str::ArcStr,
};
use raphtory_storage::graph::{
    edges::{edge_storage_ops::EdgeStorageOps, edges::EdgesStorageRef},
    graph::GraphStorage,
    nodes::node_storage_ops::NodeStorageOps,
};
use std::{iter, sync::Arc};
use storage::{
    api::edges::{EdgeRefOps, EdgeSegmentOps, LockedESegment},
    pages::edge_store::EdgeStorageInner,
    Extension,
};

/// The size of the first chunk of edge ids; each chunk doubles, up to the limit given by
/// [`MIN_CHUNK`] and [`CHUNKS_PER_SEGMENT`], so a partly read scan (`LIMIT`, `EXISTS`, `ASK`)
/// reads few edges.
const FIRST_CHUNK: usize = 64;

/// Chunks grow up to at least this many edge ids.
const MIN_CHUNK: usize = 1 << 16;

/// Chunks grow up to at least the layer's edges in the segment divided by this (resuming
/// inside a segment re-walks the edges already read).
const CHUNKS_PER_SEGMENT: usize = 8;

/// Which side of the anchor node the scanned edges are on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dir {
    /// Edges whose source is the anchor.
    Out,
    /// Edges whose destination is the anchor.
    In,
}

/// Iterates `(src, layer, dst)` for every `(edge, layer)` pair visible in a view, or (after
/// [`only_valid`](Self::only_valid)) in `view.valid()`.
///
/// Each pair is produced at most once per scan, and the private layer `_static_graph` (layer
/// 0) never. When a `layer` is given the scan restricts the view to it, so the view may already
/// be restricted or not.
pub(crate) struct EdgeScan {
    view: DynamicGraph,
    layer: Option<LayerId>,
    dir: Dir,
    /// Whether only the pairs that are valid in the view are produced.
    valid: bool,
    anchors: Anchors,
    buf: std::vec::IntoIter<(VID, LayerId, VID)>,
    /// Once it has fired, the scan ends (see [`interruptible`](Self::interruptible)).
    interrupt: Option<Arc<Interrupt>>,
}

/// Where the edges of a scan come from. Both are read lazily, in [`Iterator::next`].
enum Anchors {
    /// The edges of these nodes, in the direction of the scan.
    Nodes(Box<dyn Iterator<Item = VID> + Send>),
    /// The edge between two nodes (taken when it is read).
    Between(Option<(VID, VID)>),
    /// The edges of the layer of the scan (or all edges), from the edge storage.
    Stored(StoredEdges),
}

/// Where a scan of the edge storage is: the edges that have one of some layers.
struct StoredEdges {
    /// The layers whose edges are read: the layer of the scan, or the layers of the view.
    layers: LayerIds,
    /// The layer whose edge count in a segment bounds its chunks (layer 0 when reading several).
    counted: LayerId,
    /// The number of edge ids collected so far.
    read: usize,
    /// The number of edge ids the next chunk collects (before its limit).
    chunk: usize,
    /// For each edge segment, the end of its ids when the scan started; later edges are not
    /// read, so the scan always ends.
    ends: Vec<EID>,
    /// The segment read next.
    segment: usize,
    /// The last edge of `segment` already read, if any.
    after: Option<EID>,
}

impl StoredEdges {
    /// A scan of the edges of `layers` (not `LayerIds::None`) in `gs`.
    fn new(gs: &GraphStorage, layers: LayerIds, first_chunk: usize) -> Self {
        let counts = gs.edge_segment_counts();
        let page_len = edge_store(gs).max_page_len() as usize;
        let ends = counts
            .counts()
            .iter()
            .enumerate()
            .map(|(segment, &count)| EID(segment * page_len + count as usize))
            .collect();
        let counted = match layers {
            LayerIds::One(l) => l,
            // every edge has the private layer 0
            _ => STATIC_GRAPH_LAYER_ID,
        };
        Self {
            layers,
            counted,
            read: 0,
            chunk: first_chunk.max(1),
            ends,
            segment: 0,
            after: None,
        }
    }

    /// The ids of the next chunk of edges (in increasing order), or `None` when every segment
    /// was read. Nothing else is read while the segment is locked.
    fn next_chunk(&mut self, gs: &GraphStorage) -> Option<Vec<EID>> {
        let store = edge_store(gs);
        while let Some(&end) = self.ends.get(self.segment) {
            let Some(segment) = store.segments().get(self.segment) else {
                self.segment += 1;
                self.after = None;
                continue;
            };
            let limit =
                MIN_CHUNK.max(segment.layer_count(self.counted) as usize / CHUNKS_PER_SEGMENT);
            let chunk = self.chunk.min(limit);
            self.chunk = chunk.saturating_mul(2);
            let after = self.after;
            // Read-locks the segment until dropped below. The lock is recursive, so a `read_only()`
            // view held by this thread cannot deadlock it with a waiting writer.
            let locked = segment.locked();
            let eids: Vec<EID> = locked
                .edge_iter(&self.layers)
                .map(|edge| edge.edge_id())
                .skip_while(|eid| after.is_some_and(|after| *eid <= after))
                .take_while(|eid| *eid < end)
                .take(chunk)
                .collect();
            drop(locked);
            self.read += eids.len();
            if eids.len() == chunk {
                self.after = eids.last().copied();
            } else {
                self.segment += 1;
                self.after = None;
            }
            if !eids.is_empty() {
                return Some(eids);
            }
        }
        None
    }
}

/// The edge storage of a graph storage, locked or not.
fn edge_store(gs: &GraphStorage) -> &EdgeStorageInner<storage::ES<Extension>, Extension> {
    match gs.edges() {
        EdgesStorageRef::Mem(edges) => edges.storage(),
        EdgesStorageRef::Unlocked(edges) => edges.storage(),
    }
}

impl EdgeScan {
    fn new(view: DynamicGraph, layer: Option<LayerId>, dir: Dir, anchors: Anchors) -> Self {
        // the private layer has no triples
        let anchors = if layer == Some(STATIC_GRAPH_LAYER_ID) {
            Anchors::Between(None)
        } else {
            anchors
        };
        Self {
            view: restrict(view, layer),
            layer,
            dir,
            valid: false,
            anchors,
            buf: Vec::new().into_iter(),
            interrupt: None,
        }
    }

    /// Every visible `(src, layer, dst)` of the view (optionally only in `layer`), in no
    /// particular order.
    ///
    /// Views without node or edge filters are read from the edge storage; others like
    /// [`all_by_nodes`](Self::all_by_nodes).
    pub(crate) fn all(view: DynamicGraph, layer: Option<LayerId>) -> Self {
        Self::all_in_chunks(view, layer, FIRST_CHUNK)
    }

    /// [`all`](Self::all), with a first chunk of `first_chunk` edges.
    pub(crate) fn all_in_chunks(
        view: DynamicGraph,
        layer: Option<LayerId>,
        first_chunk: usize,
    ) -> Self {
        if layer == Some(STATIC_GRAPH_LAYER_ID) {
            // the private layer has no triples
            return Self::all_by_nodes(view, layer);
        }
        let view = restrict(view, layer);
        if reads_edge_storage(&view, layer) {
            // a scan of all layers reads only the edges that have a layer of the view
            let layers = layer.map_or_else(|| view.layer_ids().clone(), LayerIds::One);
            let edges = StoredEdges::new(view.core_graph(), layers, first_chunk);
            Self::new(view, layer, Dir::Out, Anchors::Stored(edges))
        } else {
            Self::all_by_nodes(view, layer)
        }
    }

    /// [`all`](Self::all) read from the out-edges of every node of the storage, in order of
    /// source VID, then destination VID, then layer id, so the pairs of each source are
    /// consecutive.
    pub(crate) fn all_by_nodes(view: DynamicGraph, layer: Option<LayerId>) -> Self {
        // VIDs have gaps, so use the owned list of existing VIDs, not `0..num_nodes`.
        let anchors = view.core_graph().node_segment_counts().into_iter();
        Self::new(view, layer, Dir::Out, Anchors::Nodes(Box::new(anchors)))
    }

    /// Whether the scan reads the edge storage.
    #[cfg(test)]
    pub(crate) fn reads_edge_storage(&self) -> bool {
        matches!(self.anchors, Anchors::Stored(_))
    }

    /// The number of edges read from the edge storage so far (`None` if it does not read it).
    #[cfg(test)]
    pub(crate) fn edges_read(&self) -> Option<usize> {
        match &self.anchors {
            Anchors::Stored(edges) => Some(edges.read),
            _ => None,
        }
    }

    /// Every visible edge of node `v` in direction `dir` (optionally only in `layer`).
    pub(crate) fn around(view: DynamicGraph, v: VID, dir: Dir, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, dir, Anchors::Nodes(Box::new(iter::once(v))))
    }

    /// Every visible layer of the edge `s -> o` (or only `layer`).
    pub(crate) fn between(view: DynamicGraph, s: VID, o: VID, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, Dir::Out, Anchors::Between(Some((s, o))))
    }

    /// Produces only the pairs valid in the view, i.e. what the same scan of `view.valid()` yields,
    /// without checking node validity (a valid pair has visible ends).
    pub(crate) fn only_valid(mut self) -> Self {
        self.valid = true;
        self
    }

    /// The scan, stopping once `interrupt` has fired (checked before each anchor or chunk, with
    /// nothing locked). It unwinds under
    /// [`run_interruptible`](crate::rdf::limits::run_interruptible), otherwise it ends early and
    /// the caller checks `interrupt`.
    pub(crate) fn interruptible(mut self, interrupt: Option<Arc<Interrupt>>) -> Self {
        self.interrupt = interrupt;
        self
    }
}

impl Iterator for EdgeScan {
    type Item = (VID, LayerId, VID);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.buf.next() {
                return Some(item);
            }
            if self.interrupt.as_deref().is_some_and(Interrupt::must_stop) {
                return None;
            }
            let items = match &mut self.anchors {
                Anchors::Nodes(nodes) => {
                    let v = nodes.next()?;
                    adjacency(&self.view, v, self.dir, self.layer, self.valid)
                }
                Anchors::Between(pair) => {
                    let (s, o) = pair.take()?;
                    match self.view.edge(s, o) {
                        None => Vec::new(),
                        Some(e) => pairs(iter::once(e), self.layer, self.valid),
                    }
                }
                Anchors::Stored(edges) => {
                    let eids = edges.next_chunk(self.view.core_graph())?;
                    stored_pairs(&self.view, self.layer, eids, self.valid)
                }
            };
            self.buf = items.into_iter();
        }
    }
}

/// Restricts `view` to `layer` (intersected with the layers of the view).
fn restrict(view: DynamicGraph, layer: Option<LayerId>) -> DynamicGraph {
    match layer {
        Some(l)
            if l != STATIC_GRAPH_LAYER_ID
                && !matches!(view.layer_ids(), LayerIds::One(id) if *id == l) =>
        {
            let name = view.get_layer_name(l);
            view.valid_layers(Layer::One(name)).into_dynamic()
        }
        _ => view,
    }
}

/// Whether a full scan of `view` (restricted to `layer`, if any) can read the edge storage:
/// the view has the layer, no filter other than a window, and edge filters that do not read
/// nodes, so visibility depends on the edge alone.
fn reads_edge_storage(view: &DynamicGraph, layer: Option<LayerId>) -> bool {
    let has_layer = match (layer, view.layer_ids()) {
        (Some(l), LayerIds::One(id)) => *id == l,
        (Some(_), _) | (None, LayerIds::None) => false,
        (None, _) => true,
    };
    has_layer
        && matches!(
            view.filter_state(),
            FilterState::Neither | FilterState::Window
        )
        && view.node_and_edge_filters_independent()
}

/// The visible `(src, layer, dst)` pairs (only the valid ones if `valid`) of the edges `eids`
/// read from the edge storage. No node is read.
fn stored_pairs(
    view: &DynamicGraph,
    layer: Option<LayerId>,
    eids: Vec<EID>,
    valid: bool,
) -> Vec<(VID, LayerId, VID)> {
    // the check Raphtory's own iterators make for each edge of a node in a window
    let window = view.filter_state() == FilterState::Window;
    let edges = eids.into_iter().filter_map(|eid| {
        let entry = view.core_edge(Either::Left(eid));
        let edge = entry.as_ref();
        if window && !view.filter_edge(edge) {
            return None;
        }
        Some(EdgeView::new(view.clone(), edge.out_ref()))
    });
    pairs(edges, layer, valid)
}

/// The `(src, layer, dst)` pairs of `edges` (all of their layers visible in the view, or only
/// `layer` if the view is restricted to it), only the valid ones if `valid`.
fn pairs(
    edges: impl IntoIterator<Item = EdgeView<DynamicGraph>>,
    layer: Option<LayerId>,
    valid: bool,
) -> Vec<(VID, LayerId, VID)> {
    match layer {
        // the view is restricted to `layer`, so the edge is valid in it if it is valid
        Some(l) => edges
            .into_iter()
            .filter(|e| !valid || e.is_valid())
            .map(|e| (e.edge.src(), l, e.edge.dst()))
            .collect(),
        None => edges
            .into_iter()
            .flat_map(|e| e.explode_layers())
            .filter(|e| !valid || e.is_valid())
            .filter_map(|e| Some((e.edge.src(), e.edge.layer()?, e.edge.dst())))
            .filter(|(_, l, _)| *l != STATIC_GRAPH_LAYER_ID)
            .collect(),
    }
}

/// Collects the visible edges of one node. Storage guards are released before returning.
fn adjacency(
    view: &DynamicGraph,
    v: VID,
    dir: Dir,
    layer: Option<LayerId>,
    valid: bool,
) -> Vec<(VID, LayerId, VID)> {
    // Without independent node filters a visible edge has visible ends, so the anchor is only
    // checked for the layers of the view.
    let node = match view.filter_state() {
        FilterState::Neither
        | FilterState::Window
        | FilterState::Edges
        | FilterState::BothIndependent => {
            let layers = view.layer_ids();
            if !matches!(layers, LayerIds::All) && !view.core_node(v).as_ref().has_layers(layers) {
                return Vec::new();
            }
            NodeView::new_internal(view.clone(), v)
        }
        FilterState::Both | FilterState::Nodes => match view.node(v) {
            Some(node) => node,
            None => return Vec::new(),
        },
    };
    let edges = match dir {
        Dir::Out => node.out_edges(),
        Dir::In => node.in_edges(),
    };
    pairs(edges, layer, valid)
}

/// The layers of `view` (never the private `_static_graph`), with their ids.
///
/// Names are collected before any id is looked up: `get_layer_id` while the `unique_layers`
/// iterator holds its lock deadlocks with a writer creating a layer.
pub(crate) fn view_layers(view: &DynamicGraph) -> Vec<(LayerId, ArcStr)> {
    let names: Vec<ArcStr> = view.unique_layers().collect();
    names
        .into_iter()
        .filter_map(|name| {
            let layer = view.get_layer_id(&name)?;
            (layer != STATIC_GRAPH_LAYER_ID).then_some((layer, name))
        })
        .collect()
}
