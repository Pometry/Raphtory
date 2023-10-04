//! Implementations of various graph algorithms that can be run on the graph.
//!
//! The algorithms are grouped into modules based on the type of graph they can be run on.
//!
//! To run an algorithm simply import the module and call the function.
//!
//! # Examples
//!
//! ```rust
//! use raphtory::algorithms::metrics::degree::average_degree;
//! use raphtory::prelude::*;
//!  
//!  let g = Graph::new();
//!  let vs = vec![
//!      (1, 1, 2),
//!      (2, 1, 3),
//!      (3, 2, 1),
//!      (4, 3, 2),
//!      (5, 1, 4),
//!      (6, 4, 5),
//!   ];
//!
//!  for (t, src, dst) in &vs {
//!    g.add_edge(*t, *src, *dst, NO_PROPS, None);
//!  };
//! println!("average_degree: {:?}", average_degree(&g));
//! ```

pub mod algorithm_result;
pub mod centrality;
pub mod community_detection;
pub mod cores;
pub mod metrics;
pub mod motifs;
pub mod netflow_one_path_vertex;
pub mod pagerank;
pub mod reciprocity;
pub mod temporal_reachability;
pub mod triangle_count;
pub mod triplet_count;
pub mod pathing;
