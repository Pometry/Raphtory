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
pub mod edge;
pub mod graph;
pub mod lsm;
mod misc;
mod props;
pub mod sortedvec;
mod tcell;
mod tset;
mod tvec;

use edge::OtherV;
use graph::TemporalGraph;
use std::ops::Range;

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
        // self.g.index[self.pid].out_degree()
        self.outbound().count()
    }

    pub fn inbound_degree(&self) -> usize {
        // self.g.index[self.pid].in_degree()
        self.inbound().count()
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
    src_id: OtherV,
    dst_id: OtherV,
    g: &'a G,
    t: Option<&'a u64>,
    e_meta: Option<&'a usize>,
}

impl<'a> EdgeView<'a, TemporalGraph> {
    pub fn global_src(&self) -> u64 {
        match self.src_id {
            OtherV::Local(src_id) => *self.g.index[src_id].logical(),
            OtherV::Remote(src_id) => src_id,
        }
    }

    pub fn global_dst(&self) -> u64 {
        match self.dst_id {
            OtherV::Local(dst_id) => *self.g.index[dst_id].logical(),
            OtherV::Remote(dst_id) => dst_id,
        }
    }

    pub fn time(&self) -> Option<&'a u64> {
        self.t
    }

    pub fn props(&self, name: &'a str) -> Box<dyn Iterator<Item = (&'a u64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break

        if let Some(edge_meta_id) = self.e_meta {
            self.g.edge_meta[*edge_meta_id].iter(prop_id)
        } else {
            Box::new(std::iter::empty())
        }
    }

    pub fn props_window(
        &self,
        name: &'a str,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&'a u64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break

        if let Some(edge_meta_id) = self.e_meta {
            self.g.edge_meta[*edge_meta_id].iter_window(prop_id, r)
        } else {
            Box::new(std::iter::empty())
        }
    }
}
