use std::{
    collections::{BTreeMap, HashMap},
    ops::Range, rc::Rc,
};

use itertools::Itertools;

use crate::tvec::DefaultTVec;
use crate::tset::TSet;
use crate::TemporalGraphStorage;
use crate::{bitset::BitSet, Direction};

#[derive(Debug)]
enum Adj {
    Empty(u64),
    List {
        logical: u64,
        out: TSet<usize>,
        into: TSet<usize>,
    },
}

impl Adj {
    fn logical(&self) -> &u64 {
        match self {
            Adj::Empty(logical) => logical,
            Adj::List { logical, .. } => logical,
        }
    }
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

    fn neighbours_iter(&self, v: u64, d: Direction) -> Box<dyn Iterator<Item = &usize> + '_> {
        let vid = self.logical_to_physical[&v];

        match &self.index[vid] {
            Adj::List { out, into, .. } => {
                match d {
                    Direction::OUT => out.iter(),
                    Direction::IN => into.iter(),
                    _ => {
                        Box::new(itertools::chain!(out.iter(), into.iter())) // probably awful but will have to do for now
                    }
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn neighbours_iter_window(
        &self,
        v: u64,
        d: Direction,
        window: Range<u64>,
    ) -> Box<dyn Iterator<Item = &usize> + '_> {
        let vid = self.logical_to_physical[&v];

        match &self.index[vid] {
            Adj::List { out, into, .. } => {
                match d {
                    Direction::OUT => out.iter_window(window),
                    Direction::IN => into.iter_window(window),
                    _ => {
                        Box::new(itertools::chain!(out.iter_window(window.clone()), into.iter_window(window.clone()))) // probably awful but will have to do for now
                    }
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }
}

impl TemporalGraphStorage for TemporalGraph {
    fn len(&self) -> usize {
        self.logical_to_physical.len()
    }

    fn add_vertex_props(&mut self, v: u64, t: u64, props: Vec<Prop>) -> &mut Self {
        match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.index.len();
                self.index.push(Adj::Empty(v));

                self.logical_to_physical.insert(v, physical_id);
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id);
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

    fn add_edge_props(&mut self, src: u64, dst: u64, t: u64, props: Vec<Prop>) -> &mut Self {
        // mark the times of the vertices at t
        self.add_vertex(src, t).add_vertex(dst, t);

        let scr_pid = self.logical_to_physical[&src];
        let dst_pid = self.logical_to_physical[&dst];

        if let entry @ Adj::Empty(_) = &mut self.index[scr_pid] {
            *entry = Adj::List {
                logical: src,
                out: TSet::new(t, dst_pid),
                into: TSet::default(),
            };
        } else if let Adj::List { out, .. } = &mut self.index[scr_pid] {
            out.push(t, dst_pid)
        }

        if let entry @ Adj::Empty(_) = &mut self.index[dst_pid] {
            *entry = Adj::List {
                logical: dst,
                out: TSet::default(),
                into:TSet::new(t, scr_pid),
            };
        } else if let Adj::List { into, .. } = &mut self.index[dst_pid] {
            into.push(t, scr_pid)
        }
        self
    }

    fn neighbours(&self, v: u64, d: crate::Direction) -> Box<dyn Iterator<Item = &u64> + '_> {
        Box::new(
            self.neighbours_iter(v, d)
                .map(|nid| self.index[*nid].logical()),
        )
    }

    fn neighbours_window(
        &self,
        w: Range<u64>,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = &u64> + '_> {
        Box::new(
            self.neighbours_iter_window(v, d, w)
                .map(|nid| self.index[*nid].logical()),
        )
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

    fn neighbours_window_t(
        &self,
        r: Range<u64>,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&u64, &u64)> + '_> {
        //TODO: this could use some improving but I'm bored now
        match d {
            Direction::OUT => {
                let src_pid = self.logical_to_physical[&v];
                if let Adj::List { out, .. } = &self.index[src_pid] {
                    Box::new(
                        out.iter_window_t(r)
                            .map(|(t, pid)| (t, self.index[*pid].logical())),
                    )
                } else {
                    Box::new(std::iter::empty())
                }
            }
            Direction::IN => {
                let dst_pid = self.logical_to_physical[&v];
                if let Adj::List { into, .. } = &self.index[dst_pid] {
                    Box::new(
                        into.iter_window_t(r)
                            .map(|(t, pid)| (t, self.index[*pid].logical())),
                    )
                } else {
                    Box::new(std::iter::empty())
                }
            }
            Direction::BOTH => {
                panic!()
            }
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
        let actual: Vec<(&u64, &u64)> = g.outbound_window_t(9, 0..13).collect();
        assert_eq!(actual, vec![(&3, &1), (&12, &1)]);
        let actual: Vec<(&u64, &u64)> = g.inbound_window_t(1, 0..13).collect();
        assert_eq!(actual, vec![(&3, &9), (&12, &9)]);
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

        let actual = g.inbound_window(44, 9..100).collect::<Vec<_>>();
        let expected: Vec<&u64> = vec![];
        assert_eq!(actual, expected)
    }

    #[test]
    fn add_the_same_edge_multiple_times(){

        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge(11, 22, 4);
        g.add_edge(11, 22, 4);


        let actual = g.outbound_window(11, 1..5).collect::<Vec<_>>();
        assert_eq!(actual, vec![&22]);
    }
}
