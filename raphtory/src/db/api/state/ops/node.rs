use crate::db::api::{state::ops::ArrowNodeOp, view::internal::NodeList};
pub(crate) use crate::db::api::{
    state::{generic_node_state::InputNodeStateValue, ops::IntoDynNodeOp, NodeOp},
    view::internal::{filtered_node::FilteredNodeStorageOps, FilterOps, FilterState, GraphView},
};
use raphtory_api::core::{
    entities::{properties::prop::PropType, LayerIds, GID, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct Name;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NameStruct {
    name: String,
}
impl From<String> for NameStruct {
    fn from(name: String) -> Self {
        NameStruct { name }
    }
}

impl NodeOp for Name {
    type Output = String;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_name(node)
    }
}

impl ArrowNodeOp for Name {
    type ArrowOutput = NameStruct;
}

impl IntoDynNodeOp for Name {}

#[derive(Debug, Copy, Clone)]
pub struct Id;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct IdStruct {
    id: GID,
}
impl From<GID> for IdStruct {
    fn from(id: GID) -> Self {
        IdStruct { id }
    }
}

impl NodeOp for Id {
    type Output = GID;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_id(node)
    }
}

impl ArrowNodeOp for Id {
    type ArrowOutput = IdStruct;
}

impl IntoDynNodeOp for Id {}

#[derive(Debug, Copy, Clone)]
pub struct Type;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TypeStruct {
    node_type: Option<ArcStr>,
}

impl From<Option<ArcStr>> for TypeStruct {
    fn from(node_type: Option<ArcStr>) -> Self {
        TypeStruct { node_type }
    }
}

impl NodeOp for Type {
    type Output = Option<ArcStr>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type(node)
    }
}

impl ArrowNodeOp for Type {
    type ArrowOutput = TypeStruct;
}

impl IntoDynNodeOp for Type {}

#[derive(Debug, Copy, Clone)]
pub struct TypeId;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TypeIdStruct {
    type_id: usize,
}

impl From<usize> for TypeIdStruct {
    fn from(type_id: usize) -> Self {
        TypeIdStruct { type_id }
    }
}

impl NodeOp for TypeId {
    type Output = usize;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        storage.node_type_id(node)
    }
}

impl ArrowNodeOp for TypeId {
    type ArrowOutput = TypeIdStruct;
}

impl IntoDynNodeOp for TypeId {}

#[derive(Debug, Clone)]
pub struct Degree<G> {
    pub(crate) dir: Direction,
    pub(crate) view: G,
}

impl<G> Degree<G> {
    pub fn new(view: G, dir: Direction) -> Self {
        Self { dir, view }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DegreeStruct {
    degree: usize,
}
impl From<usize> for DegreeStruct {
    fn from(degree: usize) -> Self {
        DegreeStruct { degree }
    }
}

impl<G: GraphView> NodeOp for Degree<G> {
    type Output = usize;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.view.node_list()
    }

    fn prop_type(&self) -> PropType {
        PropType::U64
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> usize {
        let node = storage.core_node(node);
        exact_degree(&self.view, &node, self.dir)
    }
}

impl<G: GraphView> ArrowNodeOp for Degree<G> {
    type ArrowOutput = DegreeStruct;
}

impl<G: GraphView + 'static> IntoDynNodeOp for Degree<G> {}

/// The degree of `node` in `view`.
fn exact_degree<'a, G: GraphView + 'a, N: NodeStorageOps<'a>>(
    view: &'a G,
    node: N,
    dir: Direction,
) -> usize {
    if matches!(view.filter_state(), FilterState::Neither) {
        node.degree(view.layer_ids(), dir)
    } else {
        node.filtered_neighbours_iter(view, view.layer_ids(), dir)
            .count()
    }
}

/// Bounds on the degree of a node. `lower == upper` means the degree is exact.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DegreeBounds {
    pub lower: usize,
    pub upper: usize,
}

impl DegreeBounds {
    fn exact(degree: usize) -> Self {
        DegreeBounds {
            lower: degree,
            upper: degree,
        }
    }
}

/// The degree of a node, computed exactly only when that is cheap; otherwise bounds on it.
///
/// Counting the degree exactly walks the node's adjacency entries (and, in a filtered view such as
/// a window, checks each of their edges against the filter). That is only done when there are at
/// most `threshold` entries to walk; otherwise the bounds come from the adjacency sizes alone.
#[derive(Debug, Clone)]
pub struct ApproxDegree<G> {
    pub(crate) dir: Direction,
    pub(crate) view: G,
    pub(crate) threshold: usize,
}

impl<G: GraphView> NodeOp for ApproxDegree<G> {
    type Output = DegreeBounds;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.view.node_list()
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> DegreeBounds {
        let node = storage.core_node(node);
        let bounds = unfiltered_degree_bounds(&node, self.view.layer_ids(), self.dir);
        // `bounds.upper` is also the number of adjacency entries an exact count walks.
        if matches!(self.view.filter_state(), FilterState::Neither) {
            if bounds.lower == bounds.upper || bounds.upper > self.threshold {
                bounds
            } else {
                DegreeBounds::exact(exact_degree(&self.view, &node, self.dir))
            }
        } else if bounds.upper <= self.threshold {
            DegreeBounds::exact(exact_degree(&self.view, &node, self.dir))
        } else {
            // A window or filter only ever drops neighbours, so the unfiltered upper bound still
            // holds, but without checking the edges any of them may have been dropped.
            DegreeBounds {
                lower: 0,
                upper: bounds.upper,
            }
        }
    }
}

impl<G: GraphView> ArrowNodeOp for ApproxDegree<G> {
    type ArrowOutput = DegreeBounds;
}

impl<G: GraphView + 'static> IntoDynNodeOp for ApproxDegree<G> {}

/// Bounds on the degree of `node` across `layer_ids`, ignoring any window or filter. Only reads
/// adjacency sizes, so it costs O(1) per layer.
fn unfiltered_degree_bounds<'a, N: NodeStorageOps<'a>>(
    node: N,
    layer_ids: &LayerIds,
    dir: Direction,
) -> DegreeBounds {
    match layer_ids {
        LayerIds::None => DegreeBounds::exact(0),
        LayerIds::All | LayerIds::One(_) => layer_degree_bounds(node, layer_ids, dir),
        LayerIds::Multiple(ids) => ids.iter().fold(DegreeBounds::exact(0), |bounds, id| {
            let layer = layer_degree_bounds(node, &LayerIds::One(id), dir);
            // A neighbour can be in several layers: the biggest layer is a lower bound on the
            // degree and the sum over layers an upper one.
            DegreeBounds {
                lower: bounds.lower.max(layer.lower),
                upper: bounds.upper + layer.upper,
            }
        }),
    }
}

/// Bounds on the degree of `node` in a single layer (or in all of them), from its O(1) in/out
/// adjacency sizes.
fn layer_degree_bounds<'a, N: NodeStorageOps<'a>>(
    node: N,
    layer: &LayerIds,
    dir: Direction,
) -> DegreeBounds {
    match dir {
        Direction::OUT | Direction::IN => DegreeBounds::exact(node.degree(layer, dir)),
        Direction::BOTH => {
            // Merging the in and out neighbours to dedup them is the expensive part of `degree`,
            // so this only says the overlap is somewhere between none and all of the smaller side.
            let out = node.degree(layer, Direction::OUT);
            let inb = node.degree(layer, Direction::IN);
            DegreeBounds {
                lower: out.max(inb),
                upper: out + inb,
            }
        }
    }
}
