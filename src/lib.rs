//! Docbrow is an in memory graph database
//!
//! dockbrown temporal graph tracks vertices, edges and properties over time, all changes are visible at any time window.
//!
//! # Add vertices and edges at different times and iterate over various windows
//!
//! ```
//! use docbrown::graph::TemporalGraph;
//! use docbrown::Prop;
//!
//!
//! let mut g = TemporalGraph::default();
//!
//! g.add_vertex(11, 1);
//! g.add_vertex(22, 2);
//! g.add_vertex(33, 3);
//! g.add_vertex(44, 4);
//!
//! g.add_edge_props(
//!     11,
//!     22,
//!     2,
//!     &vec![
//!         ("amount".into(), Prop::F64(12.34)),
//!         ("label".into(), Prop::Str("blerg".into())),
//!     ],
//! );
//!
//! g.add_edge_props(
//!     22,
//!     33,
//!     3,
//!     &vec![
//!         ("weight".into(), Prop::U32(12)),
//!         ("label".into(), Prop::Str("blerg".into())),
//!     ],
//! );
//!
//! g.add_edge_props(33, 44, 4, &vec![("label".into(), Prop::Str("blerg".into()))]);
//!
//! g.add_edge_props(
//!     44,
//!     11,
//!     5,
//!     &vec![
//!         ("weight".into(), Prop::U32(12)),
//!         ("amount".into(), Prop::F64(12.34)),
//!     ],
//! );
//!
//! // betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
//! let vs = g
//!     .iter_vs_window(2..4)
//!     .map(|v| v.global_id())
//!     .collect::<Vec<_>>();
//! assert_eq!(vs, vec![11, 22, 33]);
//!
//!
//! // between t: 3 and t:6 (excluded) show the visible outbound edges
//! let vs = g
//!     .iter_vs_window(3..6)
//!     .flat_map(|v| {
//!         v.outbound().map(|e| e.global_dst()).collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
//!     }).collect::<Vec<_>>();
//!
//! assert_eq!(vs, vec![33, 44, 11]);
//!
//! let edge_weights = g
//!     .outbound(11)
//!     .flat_map(|e| {
//!         let mut weight = e.props("weight").collect::<Vec<_>>();
//!
//!         let mut amount = e.props("amount").collect::<Vec<_>>();
//!
//!         let mut label = e.props("label").collect::<Vec<_>>();
//!
//!         weight.append(&mut amount);
//!         weight.append(&mut label);
//!         weight
//!     })
//!     .collect::<Vec<_>>();
//!
//! assert_eq!(
//!     edge_weights,
//!     vec![
//!         (&2, Prop::F64(12.34)),
//!         (&2, Prop::Str("blerg".into()))
//!     ]
//! )
//! ```
pub mod bitset;
pub mod db;
pub mod graph;
pub mod lsm;
mod misc;
mod props;
pub mod sortedvec;
mod tadjset;
mod tcell;
mod tset;
mod tvec;

use graph::TemporalGraph;
use std::ops::Range;
use tadjset::AdjEdge;

/// Specify the direction of the neighbours
#[derive(Clone, Copy)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

#[derive(Debug, PartialEq)]
pub enum Prop {
    Str(String),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

pub struct VertexView<'a, G> {
    g_id: u64,
    pid: usize,
    g: &'a G,
    w: Option<Range<u64>>,
}

impl<'a> VertexView<'a, TemporalGraph> {
    pub fn global_id(&self) -> u64 {
        self.g_id
    }

    pub fn outbound_degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::OUT, w)
        } else {
            self.g._degree(self.pid, Direction::OUT)
        }
    }

    pub fn inbound_degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::IN, w)
        } else {
            self.g._degree(self.pid, Direction::IN)
        }
    }

    pub fn degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::BOTH, w)
        } else {
            self.g._degree(self.pid, Direction::BOTH)
        }
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub fn outbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        if let Some(r) = &self.w {
            self.g.outbound_window(self.g_id, r.clone())
        } else {
            self.g.outbound(self.g_id)
        }
    }

    pub fn inbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        if let Some(r) = &self.w {
            self.g.inbound_window(self.g_id, r.clone())
        } else {
            self.g.inbound(self.g_id)
        }
    }
}

// FIXME: this is a bit silly, we might not need the reference lifetime at all
pub struct EdgeView<'a, G: Sized> {
    src_id: usize,
    dst_id: usize,
    g: &'a G,
    t: Option<u64>,
    e_meta: AdjEdge,
}

impl<'a> EdgeView<'a, TemporalGraph> {
    pub fn global_src(&self) -> u64 {
        if self.e_meta.is_local() {
            *self.g.index[self.src_id].logical()
        } else {
            self.src_id.try_into().unwrap()
        }
    }

    pub fn global_dst(&self) -> u64 {
        if self.e_meta.is_local() {
            *self.g.index[self.dst_id].logical()
        } else {
            self.dst_id.try_into().unwrap()
        }
    }

    pub fn time(&self) -> Option<u64> {
        self.t
    }

    pub fn props(&self, name: &'a str) -> Box<dyn Iterator<Item = (&'a u64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break
        self.g.edge_meta[self.e_meta.edge_meta_id()].iter(prop_id)
    }

    pub fn props_window(
        &self,
        name: &'a str,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&'a u64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break

        self.g.edge_meta[self.e_meta.edge_meta_id()].iter_window(prop_id, r)
    }
}
