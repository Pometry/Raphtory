//! # raphtory
//!
//! `raphtory` is a Rust library for analysing time-based graph data.
//! It is designed to be horizontally scalable,and can be used for a variety of applications
//! such as social network, cyber security, fraud analysis and more.
//!
//! The core feature of raphtory is the ability to analyse time-based graph data.
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
//! - **Time-based Graphs** - raphtory allows you to create and analyse time-based graphs.
//! - **Graph Analytics** - raphtory provides a variety of graph analytics algorithms.
//! - **Horizontal Scalability** - raphtory is designed to be horizontally scalable.
//! - **Distributed** - raphtory can be distributed across multiple machines.
//! - **Fast** - raphtory is fast, and can process large amounts of data in a short amount of time.
//! - **Open Source** - raphtory is open source, and is available on Github under a GPL-3.0 license.
//!
//! ## Example
//!
//! Create your own graph below
//! ```
//! use raphtory::prelude::*;
//!
//! // Create your GraphDB object and state the number of shards you would like, here we have 2
//! let graph = Graph::new();
//!
//! // Add vertex and edges to your graph with the respective properties
//! graph.add_vertex(
//!   1,
//!   "Gandalf",
//!   [("type".to_string(), Prop::Str("Character".to_string()))],
//! );
//!
//! graph.add_vertex(
//!   2,
//!   "Frodo",
//!   [("type".to_string(), Prop::Str("Character".to_string()))],
//! );
//!
//! graph.add_edge(
//!   3,
//!   "Gandalf",
//!   "Frodo",
//!   [(
//!       "meeting".to_string(),
//!       Prop::Str("Character Co-occurrence".to_string()),
//!   )],
//!   None,
//! );
//!
//! // Get the in-degree, out-degree and degree of Gandalf
//! println!("Number of vertices {:?}", graph.num_vertices());
//! println!("Number of Edges {:?}", graph.num_edges());
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
//! This project is licensed under the terms of the GPL-3.0 license.
//! Please see the Github repository for more information.
//!
//! ## Contributing
//!
//! raphtory is created by [Pometry](https://pometry.com).
//! We are always looking for contributors to help us improve the library.
//! If you are interested in contributing, please see
//! our [Github repository](https://github.com/Raphtory/raphtory)
#[allow(unused_imports)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub mod algorithms;
pub mod core;
pub mod db;
pub mod graphgen;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "io")]
pub mod graph_loader;

#[cfg(feature = "search")]
pub mod search;

pub mod prelude {
    pub const NO_PROPS: [(&str, Prop); 0] = [];
    pub use crate::{
        core::{AsProp, Prop, PropUnwrap},
        db::{
            api::{
                mutation::{AdditionOps, DeletionOps, PropertyAdditionOps},
                view::{
                    EdgeListOps, EdgeViewOps, GraphViewOps, Layer, TimeOps, VertexListOps, LayerOps,
                    VertexViewOps,
                },
            },
            graph::graph::Graph,
        },
    };
}
