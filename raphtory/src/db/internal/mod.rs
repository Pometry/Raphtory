use crate::{core::tgraph2::tgraph::InnerTemporalGraph, prelude::GraphViewOps};

pub(crate) mod core_ops;
pub(crate) mod addition;
pub(crate) mod prop_add;
pub(crate) mod time_semantics;
pub(crate) mod deletion;
pub(crate) mod graph_ops;
pub(crate) mod materialize;

// impl<const N:usize, G: GraphViewOps> PartialEq<G> for InnerTemporalGraph<N>{
//     fn eq(&self, other: &G) -> bool {
//         if self.num_vertices() == other.num_vertices() && self.num_edges() == other.num_edges() {
//             self.vertices().id().all(|v| other.has_vertex(v)) && // all vertices exist in other 
//             self.edges().explode().count() == other.edges().explode().count() && // same number of exploded edges
//             self.edges().explode().all(|e| { // all exploded edges exist in other
//                 other
//                     .edge(e.src().id(), e.dst().id(), None)
//                     .filter(|ee| ee.active(e.time().expect("exploded")))
//                     .is_some()
//             })
//         } else {
//             false
//         }
//     }
// }