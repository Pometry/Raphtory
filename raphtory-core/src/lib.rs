//! # raphtory
//!
//! `raphtory` is the core module for the raphtory library.
//!
//! The raphtory library is a temporal graph analytics tool, which allows users to create
//! and analyze graph data with time.
//!
//! This crate provides the core data structures and functions for working with temporal graphs,
//! as well as building and evaluating algorithms.
//!
//! **Note** this module is not meant to be used as a standalone crate, but in conjunction with the
//! raphtory_db crate.
//!
//! For example code, please see the raphtory_db crate.
//!
//! ## Supported Platforms
//!
//! `raphtory` supports  support for the following platforms:
//!
//! **Note** they must have Rust 1.83 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!
pub mod entities;
pub mod storage;
pub mod utils;

// #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Hash, Default)]
// pub enum Lifespan {
//     Interval {
//         start: i64,
//         end: i64,
//     },
//     Event {
//         time: i64,
//     },
//     #[default]
//     Inherited,
// }
//
// /// struct containing all the necessary information to allow Raphtory creating a document and
// /// storing it
// #[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash, Default)]
// pub struct DocumentInput {
//     pub content: String,
//     pub life: Lifespan,
// }
//
// impl Display for DocumentInput {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.write_str(&self.content)
//     }
// }
//
// impl From<Lifespan> for Value {
//     fn from(lifespan: Lifespan) -> Self {
//         match lifespan {
//             Lifespan::Interval { start, end } => json!({ "start": start, "end": end }),
//             Lifespan::Event { time } => json!({ "time": time }),
//             Lifespan::Inherited => Value::String("inherited".to_string()),
//         }
//     }
// }
