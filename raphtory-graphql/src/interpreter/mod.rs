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

pub mod sink;

pub use sink::{streaming_body, Sink};
