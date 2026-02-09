use crate::{core::entities::VID, prelude::*};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    mem,
};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Hash)]
#[repr(transparent)]
pub struct ComID(pub usize);

impl ComID {
    pub fn index(&self) -> usize {
        self.0
    }
}

impl From<usize> for ComID {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl<'a> From<&'a usize> for ComID {
    fn from(value: &'a usize) -> Self {
        Self(*value)
    }
}

#[derive(Debug, Default)]
pub struct Partition {
    node_to_com: Vec<ComID>,
    com_to_nodes: Vec<HashSet<VID>>,
}

impl<C: Into<ComID>> FromIterator<C> for Partition {
    fn from_iter<T: IntoIterator<Item = C>>(iter: T) -> Self {
        let node_to_com: Vec<_> = iter.into_iter().map(|c| c.into()).collect();
        let num_coms = node_to_com.iter().max().map(|n| n.index() + 1).unwrap_or(0);
        let mut com_to_nodes: Vec<HashSet<VID>> = (0..num_coms).map(|_| HashSet::new()).collect();
        for (i, c) in node_to_com.iter().enumerate() {
            com_to_nodes[c.index()].insert(VID(i));
        }
        Self {
            node_to_com,
            com_to_nodes,
        }
    }
}

impl Partition {
    pub fn from_coms(coms: Vec<Vec<VID>>) -> Self {
        let num_nodes: usize = coms.iter().map(|com| com.len()).sum();
        let mut node_to_com = vec![ComID(0); num_nodes];
        let mut com_to_nodes = Vec::with_capacity(coms.len());
        for (i, com) in coms.into_iter().enumerate() {
            let com: HashSet<VID> = com.into_iter().map(|v| v.into()).collect();
            for v in com.iter() {
                node_to_com[v.index()] = ComID(i);
            }
            com_to_nodes.push(com);
        }
        Self {
            node_to_com,
            com_to_nodes,
        }
    }
    /// Initialise all-singleton partition (i.e., each node in its own community)
    pub fn new_singletons(n: usize) -> Self {
        let node_to_com = (0..n).map(ComID).collect();
        let com_to_nodes = (0..n).map(|index| HashSet::from([VID(index)])).collect();
        Self {
            node_to_com,
            com_to_nodes,
        }
    }

    /// Get community label for node `node`
    pub fn com(&self, node: &VID) -> ComID {
        self.node_to_com[node.index()]
    }

    /// Get nodes for community `com`
    pub fn nodes(&self, com: &ComID) -> impl Iterator<Item = &VID> + '_ {
        self.com_to_nodes[com.index()].iter()
    }

    pub fn coms(&self) -> impl Iterator<Item = (ComID, &HashSet<VID>)> + '_ {
        self.com_to_nodes
            .iter()
            .enumerate()
            .map(|(index, com)| (ComID(index), com))
    }

    pub fn move_node(&mut self, node: &VID, new_com: ComID) {
        let old_com = self.com(node);
        if old_com != new_com {
            self.node_to_com[node.index()] = new_com;
            self.com_to_nodes[old_com.index()].remove(node);
            self.com_to_nodes[new_com.index()].insert(*node);
        }
    }

    pub fn num_coms(&self) -> usize {
        self.com_to_nodes.len()
    }

    pub fn num_nodes(&self) -> usize {
        self.node_to_com.len()
    }

    /// Relabel communities in compact form, eliminating empty labels
    ///
    /// # Returns
    ///
    /// Compact partitions and mappings from new to old and old to new community labels `(partition, new_to_old, old_to_new)`
    pub fn compact(self) -> (Self, Vec<ComID>, HashMap<ComID, ComID>) {
        let mut node_to_com = self.node_to_com;

        let (new_to_old, com_to_nodes): (Vec<_>, Vec<_>) = self
            .com_to_nodes
            .into_iter()
            .enumerate()
            .filter_map(|(index, com)| (!com.is_empty()).then_some((ComID(index), com)))
            .unzip();
        for (com_index, com) in com_to_nodes.iter().enumerate() {
            for node in com {
                node_to_com[node.index()] = ComID(com_index);
            }
        }
        let old_to_new: HashMap<_, _> = new_to_old
            .iter()
            .enumerate()
            .map(|(new, old)| (*old, ComID(new)))
            .collect();
        (
            Self {
                node_to_com,
                com_to_nodes,
            },
            new_to_old,
            old_to_new,
        )
    }

    pub fn entropy(&self) -> f64 {
        let mut value = 0.0;
        let total_count = self.num_nodes() as f64;
        for (_, com) in self.coms() {
            let count = com.len();
            if count > 0 {
                let p = count as f64 / total_count;
                value += p * p.log2();
            }
        }
        -value
    }

    /// Compute normalised mutual information between this partition and other partition in bits
    pub fn nmi(&self, other: &Partition) -> f64 {
        let total_count = self.num_nodes() as f64;
        let mut value = 0.0;
        for (_, com_i) in self.coms() {
            for (j, com_j) in other.coms() {
                let p_ij =
                    (com_i.iter().filter(|&v| other.com(v) == j).count() as f64) / total_count;
                if p_ij > 0.0 {
                    let p_i = (com_i.len() as f64) / total_count;
                    let p_j = (com_j.len() as f64) / total_count;
                    value += p_ij * (p_ij / (p_i * p_j)).log2();
                }
            }
        }
        2.0 * value / (self.entropy() + other.entropy())
    }
}

pub trait ModularityFunction {
    /// Construct new modularity function from graph
    fn new<'graph, G: GraphViewOps<'graph>>(
        graph: G,
        weight_prop: Option<&str>,
        resolution: f64,
        partition: Partition,
        tol: f64,
    ) -> Self;

    /// Compute modularity delta for moving a node to new community
    fn move_delta(&self, node: &VID, new_com: ComID) -> f64;

    /// Move node to new community
    fn move_node(&mut self, node: &VID, new_com: ComID);

    /// List all candidate moves (i.e., moves that may improve modularity) for a node
    fn candidate_moves(&self, node: &VID) -> Box<dyn Iterator<Item = ComID> + '_>;

    /// Aggregate the modularity function (coarse-grained to communities in partition)
    /// and return the partition
    fn aggregate(&mut self) -> Partition;

    /// Return modularity value for partition
    fn value(&self) -> f64;

    /// Get the partition
    fn partition(&self) -> &Partition;

    fn nodes(&self) -> Box<dyn Iterator<Item = VID>>;
}

/// Undirected modularity function (assumes edges are all present in both directions in the graph)
pub struct ModularityUnDir {
    resolution: f64,
    partition: Partition,
    adj: Vec<Vec<(VID, f64)>>,
    self_loops: Vec<f64>,
    k: Vec<f64>,
    adj_com: Vec<HashMap<ComID, f64>>,
    k_com: Vec<f64>,
    m2: f64,
    tol: f64,
}

impl ModularityFunction for ModularityUnDir {
    fn new<'graph, G: GraphViewOps<'graph>>(
        graph: G,
        weight_prop: Option<&str>,
        resolution: f64,
        partition: Partition,
        tol: f64,
    ) -> Self {
        let _n = graph.count_nodes();
        let nodes = graph.nodes();
        let local_id_map: HashMap<_, _> =
            nodes.iter().enumerate().map(|(i, n)| (n, VID(i))).collect();
        let adj: Vec<_> = nodes
            .iter()
            .map(|node| {
                node.edges()
                    .iter()
                    .filter(|e| e.dst() != e.src())
                    .map(|e| {
                        let w = weight_prop
                            .map(|w| e.properties().get(w).unwrap_f64())
                            .unwrap_or(1.0);
                        let dst_id = local_id_map[&e.nbr().cloned()];
                        (dst_id, w)
                    })
                    .filter(|(_, w)| w >= &tol)
                    .collect::<Vec<_>>()
            })
            .collect();
        let self_loops: Vec<_> = graph
            .nodes()
            .iter()
            .map(|node| {
                graph
                    .edge(node.node, node.node)
                    .map(|e| {
                        weight_prop
                            .map(|w| e.properties().get(w).unwrap_f64())
                            .unwrap_or(1.0)
                    })
                    .filter(|w| w >= &tol)
                    .unwrap_or(0.0)
            })
            .collect();
        let k: Vec<f64> = adj
            .iter()
            .map(|neighbours| neighbours.iter().map(|(_, w)| w).sum())
            .collect();
        let adj_com: Vec<_> = adj
            .iter()
            .enumerate()
            .map(|(index, neighbours)| {
                let mut com_neighbours = HashMap::new();
                for (n, w) in neighbours {
                    com_neighbours
                        .entry(partition.com(n))
                        .and_modify(|old_w| *old_w += *w)
                        .or_insert(*w);
                }
                if self_loops[index] != 0.0 {
                    *com_neighbours
                        .entry(partition.com(&VID(index)))
                        .or_insert(0.0) += self_loops[index];
                }
                com_neighbours
            })
            .collect();
        let k_com: Vec<f64> = partition
            .coms()
            .map(|(_, com)| com.iter().map(|node| k[node.index()]).sum())
            .collect();
        let m2: f64 = k_com.iter().sum();
        Self {
            partition,
            adj,
            self_loops,
            k,
            adj_com,
            k_com,
            resolution,
            m2,
            tol,
        }
    }

    fn move_delta(&self, node: &VID, new_com: ComID) -> f64 {
        let old_com = self.partition.com(node);
        if old_com == new_com {
            0.0
        } else {
            let a = 2.0
                * (self.adj_com[node.index()].get(&new_com).unwrap_or(&0.0)
                    - self.adj_com[node.index()].get(&old_com).unwrap_or(&0.0)
                    + self.self_loops[node.index()]);
            let p = 2.0
                * (self.k[node.index()]
                    * (self.k_com[new_com.index()] - self.k_com[old_com.index()])
                    + self.k[node.index()].powi(2));

            (a - self.resolution * p / self.m2) / self.m2
        }
    }

    fn move_node(&mut self, node: &VID, new_com: ComID) {
        let old_com = self.partition.com(node);
        if old_com != new_com {
            let w_self = self.self_loops[node.index()];
            match self.adj_com[node.index()]
                .entry(old_com)
                .and_modify(|v| *v -= w_self)
            {
                Entry::Occupied(v) => {
                    if *v.get() < self.tol {
                        v.remove();
                    }
                }
                _ => {
                    // should only be possible for small values due to tolerance above
                    debug_assert!(w_self < self.tol)
                }
            }
            if w_self != 0.0 {
                *self.adj_com[node.index()].entry(new_com).or_insert(0.0) += w_self;
            }

            for (n, w) in &self.adj[node.index()] {
                match self.adj_com[n.index()]
                    .entry(old_com)
                    .and_modify(|v| *v -= w)
                {
                    Entry::Occupied(v) => {
                        if *v.get() < self.tol {
                            v.remove();
                        }
                    }
                    _ => {
                        // should only be possible for small values due to tolerance above
                        debug_assert!(*w < self.tol)
                    }
                }
                match self.adj_com[node.index()]
                    .entry(self.partition.com(n))
                    .and_modify(|v| *v -= w)
                {
                    Entry::Occupied(v) => {
                        if *v.get() < self.tol {
                            v.remove();
                        }
                    }
                    _ => {
                        // should only be possible for small values due to tolerance above
                        debug_assert!(*w < self.tol)
                    }
                }
                *self.adj_com[n.index()].entry(new_com).or_insert(0.0) += w;
                *self.adj_com[node.index()]
                    .entry(self.partition.com(n))
                    .or_insert(0.0) += w;
            }
            self.k_com[old_com.index()] -= self.k[node.index()];
            self.k_com[new_com.index()] += self.k[node.index()];
        }
        self.partition.move_node(node, new_com);
    }

    fn candidate_moves(&self, node: &VID) -> Box<dyn Iterator<Item = ComID> + '_> {
        Box::new(self.adj_com[node.index()].keys().copied())
    }

    fn aggregate(&mut self) -> Partition {
        let old_partition = mem::take(&mut self.partition);
        let (new_partition, new_to_old, old_to_new) = old_partition.compact();
        let adj_com: Vec<_> = new_partition
            .coms()
            .map(|(_c_new, com)| {
                let mut neighbours = HashMap::new();
                for n in com {
                    for (c_old, w) in &self.adj_com[n.index()] {
                        *neighbours.entry(old_to_new[c_old]).or_insert(0.0) += w;
                    }
                }
                neighbours
            })
            .collect();
        let adj: Vec<_> = adj_com
            .iter()
            .enumerate()
            .map(|(index, neighbours)| {
                neighbours
                    .iter()
                    .filter(|(ComID(c), _)| c != &index)
                    .map(|(ComID(index), w)| (VID(*index), *w))
                    .collect::<Vec<_>>()
            })
            .collect();
        let self_loops: Vec<_> = adj_com
            .iter()
            .enumerate()
            .map(|(index, neighbours)| neighbours.get(&ComID(index)).copied().unwrap_or(0.0))
            .collect();
        let k: Vec<_> = new_to_old
            .into_iter()
            .map(|ComID(index)| self.k_com[index])
            .collect();
        let k_com = k.clone();
        let partition = Partition::new_singletons(new_partition.num_coms());
        self.adj = adj;
        self.adj_com = adj_com;
        self.self_loops = self_loops;
        self.k = k;
        self.k_com = k_com;
        self.partition = partition;
        new_partition
    }

    fn value(&self) -> f64 {
        let e: f64 = self
            .partition
            .coms()
            .map(|(cid, com)| {
                com.iter()
                    .flat_map(|n| self.adj_com[n.index()].get(&cid))
                    .sum::<f64>()
            })
            .sum();
        let k: f64 = self.k_com.iter().map(|k| k.powi(2)).sum();
        e / self.m2 - k / self.m2.powi(2)
    }

    fn partition(&self) -> &Partition {
        &self.partition
    }

    fn nodes(&self) -> Box<dyn Iterator<Item = VID>> {
        Box::new((0..self.partition.num_nodes()).map(VID))
    }
}

/// Constant Potts model modularity from https://arxiv.org/pdf/1104.3083
pub struct ConstModularity {
    resolution: f64,
    partition: Partition,
    adj: Vec<Vec<(VID, f64)>>,
    self_loops: Vec<f64>,
    adj_com: Vec<HashMap<ComID, f64>>,
    n: Vec<i64>,
    n_com: Vec<i64>,
    n_tot: i64,
    m2: f64,
    tol: f64,
}

impl ModularityFunction for ConstModularity {
    fn new<'graph, G: GraphViewOps<'graph>>(
        graph: G,
        weight_prop: Option<&str>,
        resolution: f64,
        partition: Partition,
        tol: f64,
    ) -> Self {
        let num_nodes = graph.count_nodes();
        let n = vec![1; num_nodes];
        let nodes = graph.nodes();
        let local_id_map: HashMap<_, _> =
            nodes.iter().enumerate().map(|(i, n)| (n, VID(i))).collect();
        let adj: Vec<_> = nodes
            .iter()
            .map(|node| {
                node.edges()
                    .iter()
                    .filter(|e| e.dst() != e.src())
                    .map(|e| {
                        let w = weight_prop
                            .map(|w| e.properties().get(w).unwrap_f64())
                            .unwrap_or(1.0);
                        let dst_id = local_id_map[&e.nbr().cloned()];
                        (dst_id, w)
                    })
                    .filter(|(_, w)| w >= &tol)
                    .collect::<Vec<_>>()
            })
            .collect();
        let self_loops: Vec<_> = graph
            .nodes()
            .iter()
            .map(|node| {
                graph
                    .edge(node.node, node.node)
                    .map(|e| {
                        weight_prop
                            .map(|w| e.properties().get(w).unwrap_f64())
                            .unwrap_or(1.0)
                    })
                    .filter(|w| w >= &tol)
                    .unwrap_or(0.0)
            })
            .collect();
        let m2: f64 = adj
            .iter()
            .flat_map(|neighbours| neighbours.iter().map(|(_, w)| w))
            .sum();
        let adj_com: Vec<_> = adj
            .iter()
            .enumerate()
            .map(|(index, neighbours)| {
                let mut com_neighbours = HashMap::new();
                for (n, w) in neighbours {
                    com_neighbours
                        .entry(partition.com(n))
                        .and_modify(|old_w| *old_w += *w)
                        .or_insert(*w);
                }
                if self_loops[index] != 0.0 {
                    *com_neighbours
                        .entry(partition.com(&VID(index)))
                        .or_insert(0.0) += self_loops[index];
                }
                com_neighbours
            })
            .collect();

        let n_com = partition.coms().map(|(_, com)| com.len() as i64).collect();
        Self {
            partition,
            adj,
            self_loops,
            adj_com,
            resolution,
            n,
            n_com,
            n_tot: num_nodes as i64,
            m2,
            tol,
        }
    }

    fn move_delta(&self, node: &VID, new_com: ComID) -> f64 {
        let old_com = self.partition.com(node);
        if old_com == new_com {
            0.0
        } else {
            let a = 2.0
                * (self.adj_com[node.index()].get(&new_com).unwrap_or(&0.0)
                    - self.adj_com[node.index()].get(&old_com).unwrap_or(&0.0)
                    + self.self_loops[node.index()]);
            let p = 2
                * (self.n[node.index()]
                    * (self.n_com[new_com.index()] - self.n_com[old_com.index()])
                    + self.n[node.index()].pow(2));

            (a / self.m2 - self.resolution * p as f64 / self.n_tot.pow(2) as f64)
        }
    }

    fn move_node(&mut self, node: &VID, new_com: ComID) {
        let old_com = self.partition.com(node);
        if old_com != new_com {
            let w_self = self.self_loops[node.index()];
            match self.adj_com[node.index()]
                .entry(old_com)
                .and_modify(|v| *v -= w_self)
            {
                Entry::Occupied(v) => {
                    if *v.get() < self.tol {
                        v.remove();
                    }
                }
                _ => {
                    // should only be possible for small values due to tolerance above
                    debug_assert!(w_self < self.tol)
                }
            }
            if w_self != 0.0 {
                *self.adj_com[node.index()].entry(new_com).or_insert(0.0) += w_self;
            }

            for (n, w) in &self.adj[node.index()] {
                match self.adj_com[n.index()]
                    .entry(old_com)
                    .and_modify(|v| *v -= w)
                {
                    Entry::Occupied(v) => {
                        if *v.get() < self.tol {
                            v.remove();
                        }
                    }
                    _ => {
                        // should only be possible for small values due to tolerance above
                        debug_assert!(*w < self.tol)
                    }
                }
                match self.adj_com[node.index()]
                    .entry(self.partition.com(n))
                    .and_modify(|v| *v -= w)
                {
                    Entry::Occupied(v) => {
                        if *v.get() < self.tol {
                            v.remove();
                        }
                    }
                    _ => {
                        // should only be possible for small values due to tolerance above
                        debug_assert!(*w < self.tol)
                    }
                }
                *self.adj_com[n.index()].entry(new_com).or_insert(0.0) += w;
                *self.adj_com[node.index()]
                    .entry(self.partition.com(n))
                    .or_insert(0.0) += w;
            }
            self.n_com[old_com.index()] -= self.n[node.index()];
            self.n_com[new_com.index()] += self.n[node.index()];
        }
        self.partition.move_node(node, new_com);
    }

    fn candidate_moves(&self, node: &VID) -> Box<dyn Iterator<Item = ComID> + '_> {
        Box::new(self.adj_com[node.index()].keys().copied())
    }

    fn aggregate(&mut self) -> Partition {
        let old_partition = mem::take(&mut self.partition);
        let (new_partition, new_to_old, old_to_new) = old_partition.compact();
        let adj_com: Vec<_> = new_partition
            .coms()
            .map(|(_c_new, com)| {
                let mut neighbours = HashMap::new();
                for n in com {
                    for (c_old, w) in &self.adj_com[n.index()] {
                        *neighbours.entry(old_to_new[c_old]).or_insert(0.0) += w;
                    }
                }
                neighbours
            })
            .collect();
        let adj: Vec<_> = adj_com
            .iter()
            .enumerate()
            .map(|(index, neighbours)| {
                neighbours
                    .iter()
                    .filter(|(ComID(c), _)| c != &index)
                    .map(|(ComID(index), w)| (VID(*index), *w))
                    .collect::<Vec<_>>()
            })
            .collect();
        let self_loops: Vec<_> = adj_com
            .iter()
            .enumerate()
            .map(|(index, neighbours)| neighbours.get(&ComID(index)).copied().unwrap_or(0.0))
            .collect();
        let n: Vec<_> = new_to_old
            .into_iter()
            .map(|ComID(index)| self.n_com[index])
            .collect();
        let n_com = n.clone();
        let partition = Partition::new_singletons(new_partition.num_coms());
        self.adj = adj;
        self.adj_com = adj_com;
        self.self_loops = self_loops;
        self.n = n;
        self.n_com = n_com;
        self.partition = partition;
        new_partition
    }

    fn value(&self) -> f64 {
        let e: f64 = self
            .partition
            .coms()
            .map(|(cid, com)| {
                com.iter()
                    .flat_map(|n| self.adj_com[n.index()].get(&cid))
                    .sum::<f64>()
            })
            .sum();
        let k: i64 = self.n_com.iter().map(|n| n.pow(2)).sum();
        e / self.m2 - k as f64 / self.n_tot.pow(2) as f64
    }

    fn partition(&self) -> &Partition {
        &self.partition
    }

    fn nodes(&self) -> Box<dyn Iterator<Item = VID>> {
        Box::new((0..self.partition.num_nodes()).map(VID))
    }
}
