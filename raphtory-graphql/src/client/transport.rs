//! Wire abstraction for talking to a remote graph server.
//!
//! V1 implementation is `GraphqlTransport`, which renders ops as GraphQL
//! queries against the existing GraphQL server. Future transports (e.g. a
//! gRPC-based one) can be swapped in by implementing this trait — client
//! wrappers won't change.

use crate::client::{op::Op, ClientError};
use async_graphql::async_trait;
use raphtory_api::core::entities::properties::prop::Prop;

/// Executes a graph operation against a remote server.
///
/// Return semantics:
/// - `Ok(None)` — write succeeded with no return value.
/// - `Ok(Some(prop))` — read returned a scalar (`Prop::I64` for `degree`,
///   `Prop::Str` for `name`, etc.).
/// - `Err(_)` — RPC or protocol failure.
///
/// If richer return shapes are needed later (Arrow columns, node handles),
/// this signature grows to a purpose-fit `Value` enum. For now, `Option<Prop>`
/// covers everything we ship.
///
/// Implementations are expected to be `Send + Sync` and cheaply cloneable behind
/// an `Arc` — client wrappers hold `Arc<dyn Transport>` and clone the handle when
/// constructing child references (`RemoteGraph::node`, etc.).
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn execute(&self, op: &Op) -> Result<Option<Prop>, ClientError>;
}
