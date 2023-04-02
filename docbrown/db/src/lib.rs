//! # DocBrown DB
//!
//! `docbrown_db` is a Rust library for analysing time-based graph data.
//! It is designed to be horizontally scalable,and can be used for a variety of applications
//! such as social network, cyber security, fraud analysis and more.
//!
//! The core feature of Docbrown is the ability to analyse time-based graph data.
//!
//! You can run periodic graph analytics on your graph, and see how the graph changes over time.
//!
//! For example:
//!
//! - Run a PageRank algorithm on your graph every 5 minutes, and see how the PageRank scores change.
//! - View the graph a previous point in time, to see how the graph looked.
//!
//!
//! ## Features
//!
//! - **Time-based Graphs** - DocBrown allows you to create and analyse time-based graphs.
//! - **Graph Analytics** - DocBrown provides a variety of graph analytics algorithms.
//! - **Horizontal Scalability** - DocBrown is designed to be horizontally scalable.
//! - **Distributed** - DocBrown can be distributed across multiple machines.
//! - **Fast** - DocBrown is fast, and can process large amounts of data in a short amount of time.
//! - **Open Source** - DocBrown is open source, and is available on Github under a AGPL-3.0 license.
//!
//! ### Shards
//! DB is the overarching manager for the graph. A GraphDB instance can have N number of shards.
//! These shards (also called TemporalGraphParts) store fragments of a graph.
//! Each shard contains a part of a graph, similar to how data is partitioned.
//!
//! When an edge or node is added to the graph, GraphDB will search for an appropriate
//! place inside a shard to place these.
//!
//! For example, if your graph has 4 shards, altogether they make up the entire temporal graph.
//! Vertices and Edges will be spread across the varying shards.
//!
//! Shards are used for performance and distribution reasons. Having multiple shards running in
//! parallel increases the overall speed. In a matter of seconds, you are able to see your
//! results from your temporal graph analysis. Furthermore, you can run your analysis across
//! multiple machines (e.g. one shard per machine).
//!
//! ## Example
//!
//! Create your own graph below
//! ```
//! use docbrown_db::graph::Graph;
//! use docbrown_core::Direction;
//! use docbrown_core::Prop;
//! use docbrown_db::view_api::*;
//!
//! // Create your GraphDB object and state the number of shards you would like, here we have 2
//! let graph = Graph::new(2);
//!
//! // Add vertex and edges to your graph with the respective properties
//! graph.add_vertex(
//!   1,
//!   "Gandalf",
//!   &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//! );
//!
//! graph.add_vertex(
//!   2,
//!   "Frodo",
//!   &vec![("type".to_string(), Prop::Str("Character".to_string()))],
//! );
//!
//! graph.add_edge(
//!   3,
//!   "Gandalf",
//!   "Frodo",
//!   &vec![(
//!       "meeting".to_string(),
//!       Prop::Str("Character Co-occurrence".to_string()),
//!   )],
//! );
//!
//! // Get the in-degree, out-degree and degree of Gandalf
//! println!("Number of vertices {:?}", graph.num_vertices());
//! println!("Number of Edges {:?}", graph.num_edges());
//! ```
//!
//! Load a pre-built graph
//! ```rust
//! use docbrown_db::algorithms::degree::average_degree;
//! use docbrown_db::graph::Graph;
//! use docbrown_db::view_api::*;
//! use docbrown_db::graph_loader::lotr_graph::lotr_graph;
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
//! ## Supported Operating Systems
//! This library requires Rust 1.54 or later.
//!
//! The following operating systems are supported:
//!
//! - `Linux`
//! - `macOS`
//! - `Windows`
//!
//! ## License
//!
//! This project is licensed under the terms of the AGPL-3.0 license.
//! Please see the Github repository for more information.
//!
//! ## Contributing
//!
//! DocBrown is created by [Pometry](https://pometry.com).
//! We are always looking for contributors to help us improve the library.
//! If you are interested in contributing, please see
//! our [Github repository](https://github.com/Raphtory/docbrown)

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub mod algorithms;
pub mod csv_loader;
pub mod edge;
pub mod graph;
pub mod graph_immutable;
pub mod graph_loader;
pub mod graph_window;
pub mod graphgen;
pub mod path;
pub mod perspective;
pub mod polars_loader;
pub mod program;
pub mod vertex;
pub mod vertices;
pub mod view_api;
