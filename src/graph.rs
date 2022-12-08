use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
};

use roaring::RoaringTreemap;

use crate::TemporalGraphStorage;

#[derive(Debug)]
enum Adj {
    Empty,
    List { out: Vec<u64>, into: Vec<u64> },
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
                self.index.push(Adj::Empty);

                self.logical_to_physical.insert(v, physical_id);
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(v);
                    })
                    .or_insert_with(|| {
                        let mut bs = RoaringTreemap::default();
                        bs.push(v);
                        bs
                    });
            }
            _ => {
                self.t_index.entry(t).and_modify(|set| {
                    set.push(v);
                });
            }
        }

        self
    }

    fn iter_vertices(&self) -> Box<dyn Iterator<Item = &u64> + '_> {
        self.iter()
    }

    fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r: R) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self.t_index.range(r).flat_map(|(_, vs)| vs.iter());
        Box::new(iter)
    }
}
