//! Push-based, streaming GraphQL execution engine.
//!
//! This sits *alongside* the `async-graphql` / `dynamic-graphql` resolver stack
//! (see [`crate::model`]) and is verified against it. See
//! `graphql-interpreter-impl.md` for the full design.
//!
//! Pipeline (only the [`sink`] half is implemented so far):
//! request → parse → validate → plan → **execute → [`Sink`] → chunked HTTP body**.
//!
//! The [`Sink`] batches emitted bytes into ~4Kb chunks and ships them over a
//! bounded channel; the HTTP layer flushes each chunk to the response without
//! ever concatenating the whole document.

pub mod exec;
pub mod http;
pub mod plan;
pub mod planner;
pub mod schema;
pub mod sink;
pub mod tokens;
pub mod value;

pub use exec::execute;
pub use http::InterpreterEndpoint;
pub use plan::{IterKind, LeafKind, Nav, Op, Plan};
pub use planner::{plan_request, PlanError, PlannedRequest};
pub use sink::{streaming_body, Sink};
pub use value::Value;
