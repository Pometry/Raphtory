//! Lock-free edge scans over a graph view.
//!
//! No Raphtory storage iterator is held across [`Iterator::next`], so writers are never stalled
//! between items and nested reads cannot deadlock. [`EdgeScan`] collects the adjacency of one
//! anchor node at a time.
//!
//! A full scan of a view without node or edge filters (a window is fine) reads the edges by id
//! instead ([`EdgeScan::all`]), a chunk of ids at a time, checking each edge against the view.
use crate::{
    db::{
        api::view::{
            internal::{CoreGraphOps, FilterOps, FilterState, InternalLayerOps},
            DynamicGraph, IntoDynamic,
        },
        graph::{edge::EdgeView, node::NodeView, views::layer_graph::LayeredGraph},
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
    edges::edge_storage_ops::EdgeStorageOps, nodes::node_storage_ops::NodeStorageOps,
};
use std::{iter, sync::Arc};

/// The size of the first chunk of edge ids; each chunk doubles, up to [`MAX_CHUNK`], so a
/// partly read scan (`LIMIT`, `EXISTS`, `ASK`) reads few edges.
const FIRST_CHUNK: usize = 64;

/// Chunks grow up to this many edge ids, so an interrupt is noticed soon.
const MAX_CHUNK: usize = 1 << 16;

/// Which side of the anchor node the scanned edges are on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dir {
    /// Edges whose source is the anchor.
    Out,
    /// Edges whose destination is the anchor.
    In,
}

/// Iterates `(src, layer, dst)` for every `(edge, layer)` pair visible in a view.
///
/// Each pair is produced at most once per scan, and the private layer `_static_graph` (layer
/// 0) never. When a `layer` is given the scan restricts the view to it, so the view may already
/// be restricted or not.
pub(crate) struct EdgeScan {
    view: DynamicGraph,
    layer: Option<LayerId>,
    dir: Dir,
    anchors: Anchors,
    buf: std::vec::IntoIter<(VID, LayerId, VID)>,
    /// Once it has fired, the scan ends (see [`interruptible`](Self::interruptible)).
    interrupt: Option<Arc<Interrupt>>,
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
    /// The layers whose edges are read: the layer of the scan, or the layers of the view.
    layers: LayerIds,
    /// The number of edge ids the next chunk reads.
    chunk: usize,
}

impl EdgeIds {
    /// A scan of the edges of `layers` (not `LayerIds::None`) in `view`.
    fn new(view: &DynamicGraph, layers: LayerIds, first_chunk: usize) -> Self {
        let eids: Box<dyn Iterator<Item = EID> + Send> =
            Box::new(view.core_graph().edge_segment_counts().into_iter());
        Self {
            eids: eids.peekable(),
            layers,
            chunk: first_chunk.max(1),
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
        // the check Raphtory's own iterators make for each edge of a node in a window
        let window = view.filter_state() == FilterState::Window;
        let edges = self.eids.by_ref().take(chunk).filter_map(|eid| {
            let entry = view.core_edge(Either::Left(eid));
            let edge = entry.as_ref();
            if !edge.has_layer(&self.layers) {
                return None;
            }
            if window && !view.filter_edge(edge) {
                return None;
            }
            Some(EdgeView::new(view.clone(), edge.out_ref()))
        });
        Some(pairs(edges, layer))
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
            anchors,
            buf: Vec::new().into_iter(),
            interrupt: None,
        }
    }

    /// Every visible `(src, layer, dst)` of the view (optionally only in `layer`), in no
    /// particular order.
    ///
    /// Views without node or edge filters are read by edge id; others like
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
        if reads_by_edge_id(&view, layer) {
            // a scan of all layers reads only the edges that have a layer of the view
            let layers = layer.map_or_else(|| view.layer_ids().clone(), LayerIds::One);
            let edges = EdgeIds::new(&view, layers, first_chunk);
            Self::new(view, layer, Dir::Out, Anchors::Edges(edges))
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

    /// Whether the scan reads the edges by id.
    #[cfg(test)]
    pub(crate) fn reads_by_edge_id(&self) -> bool {
        matches!(self.anchors, Anchors::Edges(_))
    }

    /// Every visible edge of node `v` in direction `dir` (optionally only in `layer`).
    pub(crate) fn around(view: DynamicGraph, v: VID, dir: Dir, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, dir, Anchors::Nodes(Box::new(iter::once(v))))
    }

    /// Every visible layer of the edge `s -> o` (or only `layer`).
    pub(crate) fn between(view: DynamicGraph, s: VID, o: VID, layer: Option<LayerId>) -> Self {
        Self::new(view, layer, Dir::Out, Anchors::Between(Some((s, o))))
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
                    adjacency(&self.view, v, self.dir, self.layer)
                }
                Anchors::Between(pair) => {
                    let (s, o) = pair.take()?;
                    match self.view.edge(s, o) {
                        None => Vec::new(),
                        Some(e) => pairs(iter::once(e), self.layer),
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
        Some(l)
            if l != STATIC_GRAPH_LAYER_ID
                && !matches!(view.layer_ids(), LayerIds::One(id) if *id == l) =>
        {
            in_layer(&view, l)
        }
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

/// The `(src, layer, dst)` pairs of `edges` (all of their layers visible in the view, or only
/// `layer` if the view is restricted to it).
fn pairs(
    edges: impl IntoIterator<Item = EdgeView<DynamicGraph>>,
    layer: Option<LayerId>,
) -> Vec<(VID, LayerId, VID)> {
    match layer {
        Some(l) => edges
            .into_iter()
            .map(|e| (e.edge.src(), l, e.edge.dst()))
            .collect(),
        None => edges
            .into_iter()
            .flat_map(|e| e.explode_layers())
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
    pairs(edges, layer)
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
