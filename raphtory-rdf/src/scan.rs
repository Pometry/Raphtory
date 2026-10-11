//! Lock-free edge scans over a graph view.
//!
//! No Raphtory storage iterator is held across [`Iterator::next`], so writers are never stalled
//! between items and nested reads cannot deadlock. [`EdgeScan`] collects the adjacency of one
//! anchor node at a time.
//!
//! A full scan of a view without node or edge filters (a window is fine) reads the edges by id
//! instead ([`EdgeScan::all`]), a chunk of ids at a time, checking each edge against the view.
use either::Either;
use raphtory::{
    db::{
        api::view::{
            internal::{
                CoreGraphOps, EdgeTimeSemanticsOps, FilterOps, FilterState, GraphTimeSemanticsOps,
                InternalLayerOps,
            },
            DynamicGraph, IntoDynamic,
        },
        graph::{node::NodeView, views::layer_graph::LayeredGraph},
    },
    prelude::*,
};
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef, properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds, EID,
        VID,
    },
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeEntryRef, edge_storage_ops::EdgeStorageOps},
    nodes::node_storage_ops::NodeStorageOps,
};
use std::{iter, vec};

/// The size of the first chunk of edge ids; each chunk doubles, up to [`MAX_CHUNK`], so a
/// partly read scan (`LIMIT`, `EXISTS`, `ASK`) reads few edges.
const FIRST_CHUNK: usize = 64;

/// Chunks grow up to this many edge ids, which bounds the buffer of a scan.
const MAX_CHUNK: usize = 1 << 16;

/// Iterates `(src, layer, dst)` for every `(edge, layer)` pair visible in a view.
///
/// Each pair is produced at most once per scan, and the private layer `_static_graph` (layer
/// 0) never. When a `layer` is given the scan restricts the view to it, so the view may already
/// be restricted or not.
pub(crate) struct EdgeScan {
    view: DynamicGraph,
    layer: Option<LayerId>,
    dir: Direction,
    anchors: Anchors,
    buf: vec::IntoIter<(VID, LayerId, VID)>,
}

/// Where the edges of a scan come from. All are read lazily, in [`Iterator::next`].
enum Anchors {
    /// The edges of these nodes, in the direction of the scan.
    Nodes(Box<dyn Iterator<Item = VID> + Send>),
    /// The edge between two nodes (taken when it is read).
    Between(Option<(VID, VID)>),
    /// The edges of the layer of the scan (or all edges), by id.
    Edges(EdgeIds),
}

/// Where a scan of the edges by id is.
struct EdgeIds {
    /// The ids of the edges when the scan started, in increasing order; later edges are not
    /// read, so the scan always ends.
    eids: iter::Peekable<Box<dyn Iterator<Item = EID> + Send>>,
    /// The number of edge ids the next chunk reads.
    chunk: usize,
}

impl EdgeIds {
    /// A scan of the edges of `view`.
    fn new(view: &DynamicGraph) -> Self {
        let eids: Box<dyn Iterator<Item = EID> + Send> =
            Box::new(view.core_graph().edge_segment_counts().into_iter());
        Self {
            eids: eids.peekable(),
            chunk: FIRST_CHUNK,
        }
    }

    /// The visible `(src, layer, dst)` pairs of the next chunk of edges, or `None` when every
    /// edge was read. No node is read.
    fn next_chunk(
        &mut self,
        view: &DynamicGraph,
        layer: Option<LayerId>,
    ) -> Option<Vec<(VID, LayerId, VID)>> {
        self.eids.peek()?;
        let chunk = self.chunk;
        self.chunk = chunk.saturating_mul(2).min(MAX_CHUNK);
        let mut items = Vec::new();
        for eid in self.eids.by_ref().take(chunk) {
            let entry = view.core_edge(Either::Left(eid));
            let edge = entry.as_ref();
            if !view.filter_edge(edge) {
                continue;
            }
            let (src, dst) = (edge.src(), edge.dst());
            match layer {
                Some(l) => items.push((src, l, dst)),
                None => items.extend(visible_layers(view, edge).map(|l| (src, l, dst))),
            }
        }
        Some(items)
    }
}

impl EdgeScan {
    fn new(view: DynamicGraph, layer: Option<LayerId>, dir: Direction, anchors: Anchors) -> Self {
        Self {
            view: restrict(view, layer),
            layer,
            dir,
            anchors,
            buf: Vec::new().into_iter(),
        }
    }

    /// The visible `(src, layer, dst)` of the view matching a pattern, whose subject and
    /// object are nodes or unbound (`None`) and whose predicate is `layer` or unbound.
    pub(crate) fn matching(
        view: DynamicGraph,
        subject: Option<VID>,
        layer: Option<LayerId>,
        object: Option<VID>,
    ) -> Self {
        match (subject, object) {
            (Some(s), Some(o)) => Self::between(view, s, o, layer),
            (Some(s), None) => Self::around(view, s, Direction::OUT, layer),
            (None, Some(o)) => Self::around(view, o, Direction::IN, layer),
            (None, None) => Self::all(view, layer),
        }
    }

    /// Every visible `(src, layer, dst)` of the view (optionally only in `layer`), in no
    /// particular order.
    ///
    /// Views without node or edge filters are read by edge id; others like
    /// [`all_by_nodes`](Self::all_by_nodes).
    pub(crate) fn all(view: DynamicGraph, layer: Option<LayerId>) -> Self {
        let view = restrict(view, layer);
        if reads_by_edge_id(&view, layer) {
            let edges = EdgeIds::new(&view);
            Self::new(view, layer, Direction::OUT, Anchors::Edges(edges))
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
        Self::new(
            view,
            layer,
            Direction::OUT,
            Anchors::Nodes(Box::new(anchors)),
        )
    }

    /// Every visible edge of node `v` in direction `dir` (optionally only in `layer`).
    fn around(view: DynamicGraph, v: VID, dir: Direction, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, dir, Anchors::Nodes(Box::new(iter::once(v))))
    }

    /// Every visible layer of the edge `s -> o` (or only `layer`).
    fn between(view: DynamicGraph, s: VID, o: VID, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, Direction::OUT, Anchors::Between(Some((s, o))))
    }
}

impl Iterator for EdgeScan {
    type Item = (VID, LayerId, VID);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.buf.next() {
                return Some(item);
            }
            let items = match &mut self.anchors {
                Anchors::Nodes(nodes) => {
                    let v = nodes.next()?;
                    adjacency(&self.view, v, self.dir, self.layer)
                }
                Anchors::Between(pair) => {
                    let (s, o) = pair.take()?;
                    match self.view.edge(s, o) {
                        None => Vec::new(),
                        Some(e) => pairs(&self.view, iter::once(e.edge), self.layer),
                    }
                }
                Anchors::Edges(edges) => edges.next_chunk(&self.view, self.layer)?,
            };
            self.buf = items.into_iter();
        }
    }
}

/// Restricts `view` to `layer` (intersected with the layers of the view).
fn restrict(view: DynamicGraph, layer: Option<LayerId>) -> DynamicGraph {
    match layer {
        Some(l) if !matches!(view.layer_ids(), LayerIds::One(id) if *id == l) => in_layer(&view, l),
        _ => view,
    }
}

/// `view` restricted to `layer` (intersected with the layers of the view).
pub(crate) fn in_layer(view: &DynamicGraph, layer: LayerId) -> DynamicGraph {
    let layers = LayerIds::One(layer).intersect(view.layer_ids());
    LayeredGraph::new(view.clone(), layers).into_dynamic()
}

/// Whether a full scan of `view` (restricted to `layer`, if any) can read the edges by id:
/// the view has the layer, no filter other than a window, and edge filters that do not read
/// nodes, so visibility depends on the edge alone.
fn reads_by_edge_id(view: &DynamicGraph, layer: Option<LayerId>) -> bool {
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

/// The `(src, layer, dst)` pairs of the visible `edges` of `view` (all of their layers visible
/// in the view, or only `layer` if the view is restricted to it).
fn pairs(
    view: &DynamicGraph,
    edges: impl IntoIterator<Item = EdgeRef>,
    layer: Option<LayerId>,
) -> Vec<(VID, LayerId, VID)> {
    match layer {
        Some(l) => edges.into_iter().map(|e| (e.src(), l, e.dst())).collect(),
        None => {
            let mut items = Vec::new();
            for e in edges {
                let entry = view.core_edge(Either::Right(e));
                items.extend(visible_layers(view, entry.as_ref()).map(|l| (e.src(), l, e.dst())));
            }
            items
        }
    }
}

/// The layers of `edge` visible in `view`, without the private `_static_graph`.
fn visible_layers<'a>(
    view: &'a DynamicGraph,
    edge: EdgeEntryRef<'a>,
) -> impl Iterator<Item = LayerId> + 'a {
    view.edge_time_semantics()
        .edge_layers(edge, view, view.layer_ids())
        .filter(|l| *l != STATIC_GRAPH_LAYER_ID)
}

/// Collects the visible edges of one node. Storage guards are released before returning.
fn adjacency(
    view: &DynamicGraph,
    v: VID,
    dir: Direction,
    layer: Option<LayerId>,
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
        Direction::OUT => node.out_edges(),
        Direction::IN => node.in_edges(),
        Direction::BOTH => node.edges(),
    };
    pairs(view, edges.into_iter().map(|e| e.edge), layer)
}

/// The layers of `view` (never the private `_static_graph`), with their names.
pub(crate) fn view_layers(view: &DynamicGraph) -> Vec<(LayerId, ArcStr)> {
    let num_layers = view.edge_meta().layer_meta().num_all_fields();
    view.layer_ids()
        .iter(num_layers)
        .filter(|layer| *layer != STATIC_GRAPH_LAYER_ID)
        .map(|layer| (layer, view.get_layer_name(layer)))
        .collect()
}
