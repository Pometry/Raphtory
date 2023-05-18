//! # raphtory
//!
//! `raphtory-io` is a module for loading graphs into raphtory from various sources, like csv, neo4j, etc.
//!
//! ## Examples
//!
//! Load a pre-built graph
//! ```rust
//! use raphtory::algorithms::degree::average_degree;
//! use raphtory::db::graph::Graph;
//! use raphtory::db::view_api::*;
//! use raphtory_io::graph_loader::example::lotr_graph::lotr_graph;
//!
//! let graph = lotr_graph(3);
//!
//! // Get the in-degree, out-degree of Gandalf
//! // The graph.vertex option returns a result of an option,
//! // so we need to unwrap the result and the option or
//! // we can use this if let instead
//! if let Some(gandalf) = graph.vertex("Gandalf") {
//!    println!("Gandalf in degree: {:?}", gandalf.in_degree());
//!   println!("Gandalf out degree: {:?}", gandalf.out_degree());
//! }
//!
//! // Run an average degree algorithm on the graph
//! println!("Average degree: {:?}", average_degree(&graph));
//! ```
//!
//! Load a graph from csv
//!
//! ```no_run
//! use raphtory::db::graph::Graph;
//! use raphtory::core::Prop;
//! use std::time::Instant;
//! use raphtory_io::graph_loader::source::csv_loader::CsvLoader;
//! use serde::Deserialize;
//! 
//! let data_dir = "/tmp/lotr.csv";
//! 
//! #[derive(Deserialize, std::fmt::Debug)]
//! pub struct Lotr {
//!    src_id: String,
//!    dst_id: String,
//!    time: i64,
//! }
//!
//! let g = Graph::new(2);
//! let now = Instant::now();
//!
//! CsvLoader::new(data_dir)
//! .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
//!     g.add_vertex(
//!         lotr.time,
//!         lotr.src_id.clone(),
//!         &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//!     )
//!     .expect("Failed to add vertex");
//!
//!     g.add_vertex(
//!         lotr.time,
//!         lotr.dst_id.clone(),
//!         &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//!     )
//!     .expect("Failed to add vertex");
//!
//!     g.add_edge(
//!         lotr.time,
//!         lotr.src_id.clone(),
//!         lotr.dst_id.clone(),
//!         &vec![(
//!             "type".to_string(),
//!             Prop::Str("Character Co-occurrence".to_string()),
//!         )],
//!         None,
//!     )
//!     .expect("Failed to add edge");
//! })
//! .expect("Failed to load graph from CSV data files");
//! ```
//!
pub mod graph_loader;
