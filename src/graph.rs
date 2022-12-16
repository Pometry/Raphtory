use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use itertools::Itertools;

use crate::bitset::BitSet;
use crate::tvec::DefaultTVec;
use crate::TemporalGraphStorage;

#[derive(Debug)]
enum Adj {
    Empty(u64),
    List {
        logical: u64,
        out: DefaultTVec<usize>,
        into: DefaultTVec<usize>,
    },
}

#[derive(Default, Debug)]
pub struct TemporalGraph {
    logical_to_physical: HashMap<u64, usize>,
    index: Vec<Adj>,
    t_index: BTreeMap<u64, BitSet>,
}

impl TemporalGraph {
    fn iter(&self) -> Box<dyn Iterator<Item = &u64> + '_> {
        let keys = self.logical_to_physical.keys();
        Box::new(keys)
    }
}

impl TemporalGraphStorage for TemporalGraph {
    fn len(&self) -> usize {
        self.logical_to_physical.len()
    }

    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self {
        match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.index.len();
                self.index.push(Adj::Empty(v));

                self.logical_to_physical.insert(v, physical_id);
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id); //FIXME: not happy here with unwrap
                    })
                    .or_insert_with(|| BitSet::one(physical_id));
            }
            Some(pid) => {
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(*pid);
                    })
                    .or_insert_with(|| BitSet::one(*pid));
            }
        }

        self
    }

    fn iter_vs(&self) -> Box<dyn Iterator<Item = &u64> + '_> {
        self.iter()
    }

    fn iter_vs_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self
            .t_index
            .range(r)
            .flat_map(|(_, vs)| vs.iter())
            .unique()
            .map(|pid| match self.index[pid] {
                Adj::Empty(lid) => lid,
                Adj::List { logical, .. } => logical,
            });
        Box::new(iter)
    }

    fn add_edge(&mut self, src: u64, dst: u64, t: u64) -> &mut Self {
        // mark the times of the vertices at t
        self.add_vertex(src, t).add_vertex(dst, t);

        let scr_pid = self.logical_to_physical[&src];
        let dst_pid = self.logical_to_physical[&dst];

        if let entry @ Adj::Empty(_) = &mut self.index[scr_pid] {
            *entry = Adj::List {
                logical: src,
                out: DefaultTVec::new(t, dst_pid),
                into: DefaultTVec::default(),
            };
        } else if let Adj::List { out, .. } = &mut self.index[scr_pid] {
            out.push(t, dst_pid)
        }

        if let entry @ Adj::Empty(_) = &mut self.index[dst_pid] {
            *entry = Adj::List {
                logical: dst,
                out: DefaultTVec::default(),
                into: DefaultTVec::new(t, scr_pid),
            };
        } else if let Adj::List { into, .. } = &mut self.index[dst_pid] {
            into.push(t, scr_pid)
        }
        self
    }

    fn outbound_window(&self, src: u64, r: Range<u64>) -> Box<dyn Iterator<Item = &u64> + '_> {
        let src_pid = self.logical_to_physical[&src];
        if let Adj::List { out, .. } = &self.index[src_pid] {
            let iter = out.iter_window(r).unique().flat_map(|pid| {
                // unique() is again problematic
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some(logical)
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn outbound(&self, src: u64) -> Box<dyn Iterator<Item = &u64> + '_> {
        let src_pid = self.logical_to_physical[&src];
        if let Adj::List { out, .. } = &self.index[src_pid] {
            let iter = out.iter().flat_map(|pid| {
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some(logical)
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn inbound_window(&self, dst: u64, r: Range<u64>) -> Box<dyn Iterator<Item = &u64> + '_> {
        let dst_pid = self.logical_to_physical[&dst];
        if let Adj::List { into, .. } = &self.index[dst_pid] {
            let iter = into.iter_window(r).flat_map(|pid| {
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some(logical)
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn inbound(&self, dst: u64) -> Box<dyn Iterator<Item = &u64> + '_> {
        let dst_pid = self.logical_to_physical[&dst];
        if let Adj::List { into, .. } = &self.index[dst_pid] {
            let iter = into.iter().flat_map(|pid| {
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some(logical)
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn outbound_degree(&self, src: u64) -> usize {
        self.outbound(src).count() // FIXME use .len() from tvec then sum
    }

    fn inbound_degree(&self, dst: u64) -> usize {
        self.inbound(dst).count() // FIXME use .len() from tvec then sum
    }

    fn outbound_degree_t(&self, src: u64, r: Range<u64>) -> usize {
        self.outbound_window(src, r).count() // FIXME use .len() from tvec then sum
    }

    fn inbound_degree_t(&self, dst: u64, r: Range<u64>) -> usize {
        self.inbound_window(dst, r).count() // FIXME use .len() from tvec then sum
    }

    fn outbound_window_t(
        &self,
        src: u64,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&u64, &u64)> + '_> {
        let src_pid = self.logical_to_physical[&src];
        if let Adj::List { out, .. } = &self.index[src_pid] {
            let iter = out.iter_window_t(r).flat_map(|(t, pid)| {
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some((t, logical))
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn inbound_window_t(
        &self,
        dst: u64,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&u64, &u64)> + '_> {
        let dst_pid = self.logical_to_physical[&dst];
        if let Adj::List { into, .. } = &self.index[dst_pid] {
            let iter = into.iter_window_t(r).flat_map(|(t, pid)| {
                if let Adj::List { logical, .. } = &self.index[*pid] {
                    Some((t, logical))
                } else {
                    None
                }
            });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }
}

#[cfg(test)]
mod graph_test {
    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert_eq!(g.iter_vs().collect::<Vec<&u64>>(), vec![&9])
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        let actual: Vec<u64> = g.iter_vs_window(0..2).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.iter_vs_window(2..10).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.iter_vs_window(0..10).collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![1, 9]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<&u64> = g.outbound_window(9, 0..2).collect();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.outbound_window(9, 0..4).collect();
        assert_eq!(actual, vec![&1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.inbound_window(1, 0..4).collect();
        assert_eq!(actual, vec![&9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![1, 9]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<&u64> = g.outbound_window(9, 0..2).collect();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.outbound_window(9, 0..4).collect();
        assert_eq!(actual, vec![&1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.inbound_window(1, 0..4).collect();
        assert_eq!(actual, vec![&9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3_overwrite() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);
        g.add_edge(9, 1, 12); // add the same edge again at different time

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).collect();
        assert_eq!(actual, vec![1, 9]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<&u64> = g.outbound_window(9, 0..2).collect();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected);

        println!("GRAPH {:?}", g);
        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.outbound_window(9, 0..4).collect();
        assert_eq!(actual, vec![&1]);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.inbound_window(1, 0..4).collect();
        assert_eq!(actual, vec![&9]);

        let actual: Vec<&u64> = g.outbound_window(9, 0..13).collect();
        assert_eq!(actual, vec![&1]);

        // when we look for time we see both variants
        let actual: Vec<&u64> = g.outbound_window(9, 0..13).collect();
        assert_eq!(actual, vec![&1]);
    }

    #[test]
    fn add_edges_at_t1t2t3_check_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);
        g.add_vertex(33, 3);
        g.add_vertex(44, 4);

        g.add_edge(11, 22, 4);
        g.add_edge(22, 33, 5);
        g.add_edge(11, 44, 6);

        let actual = g.iter_vs_window(1..4).collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33]);

        let actual = g.iter_vs_window(1..6).collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33, 44]);

        let actual = g.outbound_window(11, 1..5).collect::<Vec<_>>();
        assert_eq!(actual, vec![&22]);

        let actual = g.outbound_window_t(11, 1..5).collect::<Vec<_>>();
        assert_eq!(actual, vec![(&4, &22)]);

        let actual = g.inbound_window(44, 1..6).collect::<Vec<_>>();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected);


        let actual = g.inbound_window(44, 1..7).collect::<Vec<_>>();
        let expected: Vec<&u64> = vec![&11];
        assert_eq!(actual, expected);


        let actual = g.inbound_window(44, 9 .. 100).collect::<Vec<_>>();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected)
    }
}
