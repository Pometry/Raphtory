//! Shared plumbing for the node-shaped collections — [`RemoteNodes`] and
//! [`RemotePathFromNode`].
//!
//! Both carry the same `path`/`transport`/`expr`/`ctx` fields, read their
//! members' ids through the same server fields (`list { id }` / `page { id }`),
//! and materialize them into the same `RemoteNode` handles. These helpers hold
//! that common behaviour so the two handle types only differ where they
//! genuinely differ — `slice()`, which only `RemoteNodes` can support, lives
//! there rather than here.

use crate::client::{
    op::{HandleCtx, Op, PageArgs, ReadExpr},
    remote_node::RemoteNode,
    transport::{expect_gid_list, Transport},
    ClientError,
};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

/// The ids of at most `limit` members, starting `page_index * limit + offset`
/// members in. Fires one RPC.
///
/// Unlike the unpaged `id()` this works against a server running with bulk list
/// endpoints disabled, which rejects `list` and `ids` outright.
pub(crate) async fn id_page(
    transport: &Arc<dyn Transport>,
    expr: &Arc<ReadExpr>,
    limit: usize,
    offset: Option<usize>,
    page_index: Option<usize>,
) -> Result<Vec<GID>, ClientError> {
    let op = Op::Read(ReadExpr::Ids {
        input: expr.clone(),
        page: Some(PageArgs {
            limit,
            offset,
            page_index,
        }),
    });
    expect_gid_list(transport.execute(&op).await?, "page")
}

/// Rebuild fetched ids as handles anchored on the parent graph view, replaying
/// the collection-level ops in application order so each member evaluates under
/// the same composed view as collection-level reads.
pub(crate) fn materialize(
    path: &str,
    transport: &Arc<dyn Transport>,
    ctx: &HandleCtx,
    ids: Vec<GID>,
) -> Vec<RemoteNode> {
    ids.into_iter()
        .map(|id| {
            RemoteNode::with_expr(
                path.to_string(),
                id.clone(),
                transport.clone(),
                ctx.node_handle_expr(id),
                ctx.clone(),
            )
        })
        .collect()
}
