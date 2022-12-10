use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
};

use roaring::RoaringTreemap;

use crate::TemporalGraphStorage;

#[derive(Debug)]
enum Adj {
    Empty(u64),
    List {
        sorted: bool,
        logical: u64,
        out: Vec<usize>,
        into: Vec<usize>,
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

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r: R) -> Box<dyn Iterator<Item = u64> + '_> {
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


        if let entry@Adj::Empty(_) = &mut self.index[scr_pid] {
            *entry = Adj::List { logical: src, out: vec![dst_pid], into: vec![] , sorted: false};
        } else if let Adj::List {out, ..} = &mut self.index[scr_pid]  {
           out.push(dst_pid) 
        }

        if let entry@Adj::Empty(_) = &mut self.index[dst_pid] {
            *entry = Adj::List { logical: dst, out: vec![], into: vec![scr_pid] , sorted: false};
        } else if let Adj::List {into, ..} = &mut self.index[dst_pid]  {
           into.push(scr_pid) 
        }
        self


    }

    fn outbound<R: RangeBounds<u64>>(&self, src: u64, r: R) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(std::iter::empty())
    }

    fn inbound<R: RangeBounds<u64>>(&self, dst: u64, r: R) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(std::iter::empty())
    }
}
