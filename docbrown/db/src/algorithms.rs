//! Implementations of various graph algorithms that can be run on the graph.
//!
//! The algorithms are grouped into modules based on the type of graph they can be run on.
//!
//! To run an algorithm simply import the module and call the function.
//!
//! # Examples
//!
//! ```rust
//! use docbrown_db::algorithms::degree::{average_degree};
//! use docbrown_db::graph::Graph;
//!  
//!  let g = Graph::new(1);
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
//!    g.add_edge(*t, *src, *dst, &vec![]);
//!  };
//! println!("average_degree: {:?}", average_degree(&g));
//! ```

pub mod degree;
pub mod directed_graph_density;
pub mod local_clustering_coefficient;
pub mod local_triangle_count;
pub mod reciprocity;
