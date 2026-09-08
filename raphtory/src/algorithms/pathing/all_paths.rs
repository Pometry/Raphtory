use crate::{
    algorithms::pathing::dijkstra::dijkstra_single_source_shortest_paths,
    db::{api::view::StaticGraphViewOps, graph::nodes::Nodes},
    prelude::{GraphViewOps, NodeStateOps},
};
use ahash::{HashSet, HashSetExt};
use indexmap::IndexSet;
use raphtory_api::core::{entities::VID, Direction};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(thiserror::Error, Debug)]
pub enum AllPathsError {
    #[error("Source node is not part of the graph view")]
    SrcNodeMissing,
    #[error("Destination node is not part of the graph view")]
    DstNodeMissing,
}

/// Find all simple (loop-free) paths between a pair of nodes.
///
/// Returns an iterator returning simple paths from shortest to longest
///
/// Based on the algorithm in
///     Yen, Jin Y. “Finding the K Shortest Loopless Paths in a Network.”
///     Management Science 17, no. 11 (1971): 712–16. https://www.jstor.org/stable/2629312.
pub fn all_simple_paths<G: StaticGraphViewOps>(
    view: &G,
    src: impl AsNodeRef,
    dst: impl AsNodeRef,
) -> Result<PathIterator<G, usize>, AllPathsError> {
    let src = (&view).node(src).ok_or(AllPathsError::SrcNodeMissing)?;
    let dst = (&view).node(dst).ok_or(AllPathsError::DstNodeMissing)?;
    Ok(PathIterator::new(view.clone(), src.node, dst.node))
}

#[derive(PartialEq, Eq, Debug)]
struct PathHeapValue<V> {
    cost: V,
    i: usize, // unique counter for tie-breaking
    path: Path,
}

impl<V: Ord> PartialOrd for PathHeapValue<V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<V: Ord> Ord for PathHeapValue<V> {
    // important, ordering is flipped as binary heap is a max-heap
    fn cmp(&self, other: &Self) -> Ordering {
        self.cost
            .cmp(&other.cost)
            .then_with(|| self.i.cmp(&other.i))
            .reverse()
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
struct Path(Arc<IndexSet<VID, ahash::RandomState>>);

impl Hash for Path {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in self.0.iter() {
            v.hash(state)
        }
    }
}

impl Path {
    fn same_root(&self, other: &Path, k: usize) -> bool {
        self.0.iter().take(k).eq(other.0.iter().take(k))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn node(&self, i: usize) -> VID {
        *self.0.get_index(i).unwrap()
    }

    fn root_iter(&self, k: usize) -> impl Iterator<Item = VID> + use<'_> {
        self.0.iter().take(k).copied()
    }
}

impl FromIterator<VID> for Path {
    fn from_iter<T: IntoIterator<Item = VID>>(iter: T) -> Self {
        Path(Arc::new(IndexSet::from_iter(iter)))
    }
}

#[derive(Debug, Default)]
struct PathHeap<V> {
    heap: BinaryHeap<PathHeapValue<V>>,
    set: HashSet<Path>,
    i: usize,
}

impl<V: Ord> PathHeap<V> {
    fn push(&mut self, cost: V, path: IndexSet<VID, ahash::RandomState>) {
        let path = Path(Arc::new(path));
        if self.set.insert(path.clone()) {
            self.heap.push(PathHeapValue {
                cost,
                i: self.i,
                path,
            });
            self.i += 1;
        }
    }

    fn pop(&mut self) -> Option<(V, Path)> {
        let PathHeapValue { cost, path, .. } = self.heap.pop()?;
        Some((cost, path))
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

pub struct PathIterator<G, V> {
    graph: G,
    dst: VID,
    list_a: Vec<Path>,
    list_b: PathHeap<V>,
}

fn shortest_path<G: StaticGraphViewOps>(
    graph: &G,
    src: VID,
    dst: VID,
) -> Option<IndexSet<VID, ahash::RandomState>> {
    let sp =
        dijkstra_single_source_shortest_paths(graph, src, vec![dst], None, Direction::OUT, None)
            .ok()?
            .get_by_index(0)?
            .1
            .path;
    Some(sp)
}

impl<G: StaticGraphViewOps> PathIterator<G, usize> {
    fn new(graph: G, src: VID, dst: VID) -> Self {
        let list_a = vec![];
        let mut list_b = PathHeap::default();
        if let Some(path) = shortest_path(&graph, src, dst) {
            list_b.push(path.len(), path);
        }

        Self {
            graph,
            dst,
            list_a,
            list_b,
        }
    }
}

impl<G: StaticGraphViewOps> Iterator for PathIterator<G, usize> {
    type Item = Nodes<'static, G>;

    fn next(&mut self) -> Option<Self::Item> {
        let (_cost, prev_path) = self.list_b.pop()?;
        self.list_a.push(prev_path.clone());
        let mut ignore_nodes = HashSet::new();
        let mut ignore_edges = HashSet::new();
        for k in 1..prev_path.len() {
            let spur_root = prev_path.node(k - 1);
            for path in self.list_a.iter() {
                if prev_path.same_root(path, k) {
                    ignore_edges.insert((path.node(k - 1), path.node(k)));
                }
                if let Some(spur) = shortest_path(
                    &self
                        .graph
                        .exclude_nodes(ignore_nodes.iter().copied())
                        .exclude_edges(ignore_edges.iter().copied()),
                    spur_root,
                    self.dst,
                ) {
                    let len = k - 1 + spur.len();
                    let path = prev_path.root_iter(k - 1).chain(spur).collect();
                    self.list_b.push(len, path)
                }
            }
            ignore_nodes.insert(spur_root);
        }
        Some(Nodes::new_indexed(self.graph.clone(), prev_path.0.into()))
    }
}
