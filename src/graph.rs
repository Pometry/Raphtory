use std::{
    collections::{BTreeMap, HashMap},
    ops::{Range, RangeBounds},
};

use roaring::RoaringTreemap;

use crate::tvec::{DefaultTVec, TVec};
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
    t_index: BTreeMap<u64, RoaringTreemap>,
}

impl TemporalGraph {
    fn iter(&self) -> Box<dyn Iterator<Item = &u64> + '_> {
        let keys = self.logical_to_physical.keys();
        Box::new(keys)
    }
}

impl TemporalGraphStorage for TemporalGraph {
    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self {
        match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.index.len();
                self.index.push(Adj::Empty(v));

                self.logical_to_physical.insert(v, physical_id);
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id.try_into().unwrap()); //FIXME: not happy here with unwrap
                    })
                    .or_insert_with(|| {
                        let mut bs = RoaringTreemap::default();
                        bs.push(physical_id.try_into().unwrap()); //FIXME: not happy here with unwrap
                        bs
                    });
            }
            Some(pid) => {
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        let pid_u64: u64 = (*pid).try_into().unwrap();
                        set.push(pid_u64);
                    })
                    .or_insert_with(|| {
                        let mut bs = RoaringTreemap::default();
                        bs.push((*pid).try_into().unwrap()); //FIXME: not happy here with unwrap
                        bs
                    });
            }
        }

        self
    }

    fn iter_vertices(&self) -> Box<dyn Iterator<Item = &u64> + '_> {
        self.iter()
    }

    fn enumerate_vs_at(&self, r: Range<u64>) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self
            .t_index
            .range(r)
            .flat_map(|(_, vs)| vs.iter())
            .map(|pid| {
                let pid_usize: usize = pid.try_into().unwrap();
                match self.index[pid_usize] {
                    Adj::Empty(lid) => lid,
                    Adj::List { logical, .. } => logical,
                }
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

    fn outbound(&self, src: u64, r: Range<u64>) -> Box<dyn Iterator<Item = &u64> + '_> {
        let src_pid = self.logical_to_physical[&src];
        if let Adj::List { out, .. } = &self.index[src_pid] {
            let iter = out.iter_window(r).flat_map(|pid| {
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

    fn inbound(&self, dst: u64, r: Range<u64>) -> Box<dyn Iterator<Item = &u64> + '_> {
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
}

#[cfg(test)]
mod graph_test {
    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert_eq!(g.iter_vertices().collect::<Vec<&u64>>(), vec![&9])
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);


        let actual: Vec<u64> = g.enumerate_vs_at(0..2).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.enumerate_vs_at(2..10).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.enumerate_vs_at(0..10).collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.enumerate_vs_at(3..10).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.enumerate_vs_at(3..10).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<&u64> = g.outbound(9, 0..2).collect();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected);

        println!("GRAPH {:?}", g);
        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.outbound(9, 0..4).collect();
        assert_eq!(actual, vec![&1]);


        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<&u64> = g.inbound(1, 0..4).collect();
        assert_eq!(actual, vec![&9]);

    }
}
