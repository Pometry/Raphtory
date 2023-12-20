use crate::{
    core::entities::VID,
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, PropUnwrap},
};
use reqwest::multipart::Part;
use std::collections::{hash_map::Entry, HashMap, HashSet};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Hash)]
#[repr(transparent)]
pub struct ComID(pub usize);

impl ComID {
    pub fn index(&self) -> usize {
        self.0
    }
}

#[derive(Debug, Default)]
pub struct Partition {
    node_to_com: Vec<ComID>,
    com_to_nodes: Vec<HashSet<VID>>,
}

impl Partition {
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
}

pub trait ModularityFunction {
    /// Construct new modularity function from graph
    fn new<'graph, G: GraphViewOps<'graph>>(
        graph: G,
        weight_prop: Option<&str>,
        resolution: f64,
    ) -> Self;

    /// Compute modularity delta for moving a node to new community
    fn move_delta(&self, node: &VID, new_com: ComID) -> f64;

    /// Move node to new community
    fn move_node(&mut self, node: &VID, new_com: ComID);

    /// List all candidate moves (i.e., moves that may improve modularity) for a node
    fn candidate_moves(&self, node: &VID) -> Box<dyn Iterator<Item = ComID> + '_>;

    /// Aggregate the modularity function (coarse-grained to communities in partition)
    fn aggregate(self) -> (Self, Partition)
    where
        Self: Sized;

    /// Return modularity value for partition
    fn value(&self) -> f64;

    /// Get the partition
    fn partition(&self) -> &Partition;
}

pub struct ModularityUnDir {
    resolution: f64,
    partition: Partition,
    adj: Vec<Vec<(VID, f64)>>,
    k: Vec<f64>,
    adj_com: Vec<HashMap<ComID, f64>>,
    k_com: Vec<f64>,
    m2: f64,
}

impl ModularityFunction for ModularityUnDir {
    fn new<'graph, G: GraphViewOps<'graph>>(
        graph: G,
        weight_prop: Option<&str>,
        resolution: f64,
    ) -> Self {
        let n = graph.count_nodes();
        let local_id_map: HashMap<_, _> = graph
            .nodes()
            .iter()
            .enumerate()
            .map(|(i, n)| (n, VID(i)))
            .collect();
        let partition = Partition::new_singletons(n);
        let adj: Vec<_> = graph
            .nodes()
            .iter()
            .map(|node| {
                node.out_edges()
                    .map(|e| {
                        let w = weight_prop
                            .map(|w| e.properties().get(w).unwrap_f64())
                            .unwrap_or(1.0);
                        let dst_id = local_id_map[&e.dst()];
                        (dst_id, w)
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let k: Vec<f64> = adj
            .iter()
            .map(|neighbours| neighbours.iter().map(|(_, w)| w).sum())
            .collect();
        let adj_com: Vec<_> = adj
            .iter()
            .map(|neighbours| {
                let mut com_neighbours = HashMap::new();
                for (n, w) in neighbours {
                    com_neighbours
                        .entry(partition.com(n))
                        .and_modify(|old_w| *old_w += *w)
                        .or_insert(*w);
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
            k,
            adj_com,
            k_com,
            resolution,
            m2,
        }
    }

    fn move_delta(&self, node: &VID, new_com: ComID) -> f64 {
        let old_com = self.partition.com(node);
        if old_com == new_com {
            0.0
        } else {
            (self.adj_com[node.index()].get(&new_com).unwrap_or(&0.0)
                - self.adj_com[node.index()].get(&old_com).unwrap_or(&0.0)
                + self.resolution
                    * self.k[node.index()]
                    * (self.k_com[old_com.index()] - self.k_com[new_com.index()])
                    / self.m2)
                / self.m2
        }
    }

    fn move_node(&mut self, node: &VID, new_com: ComID) {
        let old_com = self.partition.com(node);
        if old_com != new_com {
            for (n, w) in &self.adj[node.index()] {
                if self.partition.com(n) == old_com {
                    match self.adj_com[n.index()]
                        .entry(old_com)
                        .and_modify(|v| *v -= w)
                    {
                        Entry::Occupied(v) => {
                            if *v.get() < 1e-8 {
                                v.remove();
                            }
                        }
                        _ => {
                            // should only be possible for small values due to tolerance above
                            debug_assert!(*w < 1e-8)
                        }
                    }
                }
                if self.partition.com(n) == new_com {
                    *self.adj_com[n.index()].entry(new_com).or_insert(0.0) += w;
                }
            }
            self.k_com[old_com.index()] -= self.k[node.index()];
            self.k_com[new_com.index()] += self.k[node.index()];
        }
        self.partition.move_node(node, new_com);
    }

    fn candidate_moves(&self, node: &VID) -> Box<dyn Iterator<Item = ComID> + '_> {
        Box::new(self.adj_com[node.index()].iter().map(|(c, _)| *c))
    }

    fn aggregate(self) -> (Self, Partition) {
        let (new_partition, new_to_old, old_to_new) = self.partition.compact();
        let adj_com: Vec<_> = new_partition
            .coms()
            .map(|(c_new, com)| {
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
            .map(|neighbours| {
                neighbours
                    .iter()
                    .map(|(ComID(index), w)| (VID(*index), *w))
                    .collect::<Vec<_>>()
            })
            .collect();
        let k: Vec<_> = new_to_old
            .into_iter()
            .map(|ComID(index)| self.k_com[index])
            .collect();
        let k_com = k.clone();
        let partition = Partition::new_singletons(new_partition.num_coms());
        (
            Self {
                adj,
                k,
                adj_com,
                k_com,
                partition,
                ..self
            },
            new_partition,
        )
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
}

#[cfg(test)]
mod test {
    use crate::{
        algorithms::community_detection::modularity::{
            ComID, ModularityFunction, ModularityUnDir, Partition,
        },
        core::entities::VID,
        prelude::*,
    };

    #[test]
    fn test() {
        let p = Partition::default();
    }

    #[test]
    fn test_delta() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

        let mut m = ModularityUnDir::new(&g, None, 1.0);
        let old_value = m.value();
        assert_eq!(old_value, -0.5);
        let delta = m.move_delta(&VID(0), ComID(1));
        println!("delta: {delta}");
        m.move_node(&VID(0), ComID(1));
        assert_eq!(m.value(), old_value + delta)
    }
}
