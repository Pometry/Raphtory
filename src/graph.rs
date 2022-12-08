use std::{
    collections::{BTreeMap, HashMap},
    io::Empty,
    ops::RangeBounds,
};

use roaring::RoaringTreemap;

use crate::{TVertexView, TemporalGraphStorage};

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
// fn update_adj_list_and_ts_index(_adj: &mut Adj, t: u64, v: u64, ts_index: TsIndex) {
//     if let Ok(mut index) = ts_index.lock() {
//         index
//             .entry(t)
//             .and_modify(|set| {
//                 set.push(v);
//             })
//             .or_insert_with(|| {
//                 let mut bs = RoaringTreemap::default();
//                 bs.push(v);
//                 bs
//             });
//     }
// }

impl TemporalGraphStorage for TemporalGraph {
    fn add_vertex(&mut self, v: u64, t: u64) -> &mut Self {
        match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.index.len();
                self.index.reserve(physical_id);
                self.index[physical_id] = Adj::Empty;
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

    fn enumerate_vertices_at<R: RangeBounds<u64>>(&self, r:R) -> TVertexView<R> {
        self.t_index.range(r);
        todo!()
    }
    //     fn add_vertex(&mut self, v: u64, t: u64) -> &Self {
    //         let idx = self.t_index.index.clone();
    //         self.gs
    //             .entry(v)
    //             .and_modify(|adj| update_adj_list_and_ts_index(adj, t, v, idx.clone()))
    //             .or_insert_with(|| {
    //                 update_adj_list_and_ts_index(&mut Adj::Empty, t, v, idx.clone());
    //                 Adj::Empty
    //             });
    //         self
    //     }

    //     fn enumerate_vertices(&self) -> Vec<u64> {
    //         self.gs.iter().map(|entry| *entry.key()).collect()
    //     }

    //     fn enumerate_vs_at<R: RangeBounds<u64>>(&self, r: R) -> TVertexView<R>
    //     where
    //         R: RangeBounds<u64>,
    //     {
    //         self.t_index.iterable_lock(r)
    //     }
}
