//! Remote handle types: the client-side mirrors of the server's graph, entity
//! and collection views.
//!
//! Each handle records the view/read operations applied to it and defers to the
//! transport plumbing in [`crate::client`] ([`Transport`](crate::client::Transport),
//! [`Op`](crate::client::op::Op), [`GraphqlTransport`](crate::client::GraphqlTransport))
//! to turn them into a request. The modules here hold no transport logic of their
//! own; they only build and interpret operations.
//!
//! Every type is re-exported from [`crate::client`], so `crate::client::RemoteGraph`
//! and `crate::client::remote::RemoteGraph` name the same type.

pub mod remote_collection_metadata;
pub mod remote_edge;
pub mod remote_edges;
pub mod remote_graph;
pub mod remote_history;
pub mod remote_metadata;
pub mod remote_nested_edges;
pub mod remote_node;
pub mod remote_nodes;
pub mod remote_path_from_graph;
pub mod remote_path_from_node;
pub mod remote_schema;
pub(crate) mod view_ops;

pub use remote_collection_metadata::{Column, RemoteMetadataView, RemotePropertiesView};
pub use remote_edge::RemoteEdge;
pub use remote_edges::RemoteEdges;
pub use remote_graph::RemoteGraph;
pub use remote_history::{
    RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds, RemoteHistoryTimestamps,
    RemoteIntervals,
};
pub use remote_metadata::{
    RemoteMetadata, RemoteProperties, RemotePropertyTuple, RemoteTemporalProperties,
    RemoteTemporalProperty,
};
pub use remote_nested_edges::RemoteNestedEdges;
pub use remote_node::RemoteNode;
pub use remote_nodes::RemoteNodes;
pub use remote_path_from_graph::RemotePathFromGraph;
pub use remote_path_from_node::RemotePathFromNode;
pub use remote_schema::{
    RemoteGraphSchema, RemoteLayerSchema, RemoteNodeSchema, RemotePropertySchema,
};

#[cfg(test)]
mod write_guard_tests {
    //! Writes on viewed handles are refused before any RPC is attempted.
    //!
    //! The transport here fails the test if it is ever called: a guard that
    //! rejected *after* building or sending the op would not be a guard.

    use super::{RemoteEdge, RemoteGraph, RemoteNode};
    use crate::client::{
        op::{HandleCtx, Op, ReadExpr},
        ClientError, Transport,
    };
    use async_graphql::async_trait;
    use raphtory::prelude::NO_PROPS;
    use raphtory_api::core::entities::{properties::prop::Prop, GID};
    use std::sync::Arc;

    struct NoRpc;

    #[async_trait::async_trait]
    impl Transport for NoRpc {
        async fn execute(&self, _op: &Op) -> Result<Option<Prop>, ClientError> {
            panic!("a guarded write reached the transport")
        }
    }

    fn graph(expr: ReadExpr) -> RemoteGraph {
        RemoteGraph {
            path: "g".into(),
            transport: Arc::new(NoRpc),
            expr: expr.into(),
        }
    }

    fn root() -> Arc<ReadExpr> {
        Arc::new(ReadExpr::Root {
            path: "g".into(),
            graph_type: None,
        })
    }

    fn windowed() -> Arc<ReadExpr> {
        Arc::new(ReadExpr::Valid { input: root() })
    }

    fn node(input: Arc<ReadExpr>) -> RemoteNode {
        RemoteNode::with_expr(
            "g".into(),
            GID::from("a"),
            Arc::new(NoRpc),
            ReadExpr::Node {
                input: input.clone(),
                id: GID::from("a"),
            },
            HandleCtx::new(input),
        )
    }

    fn edge(input: Arc<ReadExpr>) -> RemoteEdge {
        RemoteEdge::with_expr(
            "g".into(),
            GID::from("a"),
            GID::from("b"),
            Arc::new(NoRpc),
            ReadExpr::Edge {
                input: input.clone(),
                src: GID::from("a"),
                dst: GID::from("b"),
            },
            HandleCtx::new(input),
        )
    }

    fn expect_refused<T>(what: &str, r: Result<T, ClientError>) {
        match r {
            Err(ClientError::InvalidInput(msg)) => {
                assert!(msg.contains(what), "{what}: wrong message: {msg}")
            }
            Err(other) => panic!("{what}: wrong error kind: {other}"),
            Ok(_) => panic!("{what} on a viewed handle: expected refusal, got Ok"),
        }
    }

    #[tokio::test]
    async fn graph_writes_are_refused_on_a_viewed_handle() {
        let g = graph(ReadExpr::Valid { input: root() }.into());
        expect_refused("add_node", g.add_node(1, "a", NO_PROPS, None, None).await);
        expect_refused(
            "create_node",
            g.create_node(1, "a", NO_PROPS, None, None).await,
        );
        expect_refused("add_edge", g.add_edge(1, "a", "b", NO_PROPS, None).await);
        expect_refused("delete_edge", g.delete_edge(1, "a", "b", None).await);
        expect_refused("add_properties", g.add_properties(1, NO_PROPS).await);
        expect_refused("add_metadata", g.add_metadata(NO_PROPS).await);
        expect_refused("update_metadata", g.update_metadata(NO_PROPS).await);
    }

    #[tokio::test]
    async fn node_writes_are_refused_on_a_viewed_handle() {
        let n = node(windowed());
        expect_refused("add_updates", n.add_updates(1, NO_PROPS, None).await);
        expect_refused("add_metadata", n.add_metadata(NO_PROPS).await);
        expect_refused("update_metadata", n.update_metadata(NO_PROPS).await);
        expect_refused("set_node_type", n.set_node_type("t".into()).await);
    }

    #[tokio::test]
    async fn edge_writes_are_refused_on_a_viewed_handle() {
        let e = edge(windowed());
        expect_refused("add_updates", e.add_updates(1, NO_PROPS, None).await);
        expect_refused("delete", e.delete(1, None).await);
        expect_refused("add_metadata", e.add_metadata(NO_PROPS, None).await);
        expect_refused("update_metadata", e.update_metadata(NO_PROPS, None).await);
    }

    #[tokio::test]
    async fn base_handles_pass_the_guard() {
        // The base handle reaches the transport (which fails the test loudly),
        // proving the guard's check is view-ness, not a blanket refusal.
        let g = graph(ReadExpr::Root {
            path: "g".into(),
            graph_type: None,
        });
        let result = std::panic::AssertUnwindSafe(g.add_node(1, "a", NO_PROPS, None, None));
        assert!(
            futures_util::FutureExt::catch_unwind(result).await.is_err(),
            "base-handle write should have reached the transport"
        );
    }
}
