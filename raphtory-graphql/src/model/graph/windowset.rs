use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        edge::GqlEdge,
        edges::GqlEdges,
        graph::GqlGraph,
        node::GqlNode,
        nodes::GqlNodes,
        path_from_node::GqlPathFromNode,
    },
    paths::UnlockedGraphFolder,
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{
    internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName},
    ResolvedObject, ResolvedObjectFields,
};
use raphtory::db::{
    api::{
        state::ops::DynNodeFilter,
        view::{DynamicGraph, TimeOps, WindowSet},
    },
    graph::{
        edge::EdgeView,
        edges::{DynEdgeItem, Edges},
        node::NodeView,
        nodes::Nodes,
        path::PathFromNode,
    },
};
use std::{borrow::Cow, marker::PhantomData};

/// A view that `rolling` / `expanding` can split into windows, together with
/// the GraphQL window-set type built over it: its name, its description, and
/// how each window becomes a GraphQL object.
pub trait WindowItem: TimeOps<'static> + Clone + Send + Sync + 'static {
    /// The GraphQL object each window is returned as.
    type Gql: OutputTypeName + for<'a> ResolveOwned<'a> + Send + Sync + 'static;
    /// What the window set carries besides the windows to build each `Gql`.
    type Ctx: Clone + Send + Sync + 'static;
    /// Registers `DESCRIPTION` on the window-set type; always
    /// `WindowSetDescription<Self>`.
    type Description: Register + 'static;
    /// The GraphQL name of the window-set type.
    const NAME: &'static str;
    /// The GraphQL description of the window-set type.
    const DESCRIPTION: &'static str;

    fn to_gql(window: Self::WindowedViewType, ctx: &Self::Ctx) -> Self::Gql;
}

/// Sets the description of the window-set type over `T`; a generic object
/// cannot take its description from a doc comment, as each instantiation
/// needs its own.
pub struct WindowSetDescription<T>(PhantomData<T>);

impl<T: WindowItem> Register for WindowSetDescription<T> {
    fn register(registry: Registry) -> Registry {
        registry.update_object(T::NAME, T::NAME, |object| {
            object.description(T::DESCRIPTION)
        })
    }
}

// A lazy sequence of windows of a view, produced by its `rolling` or
// `expanding`. The GraphQL name and description come from `WindowItem`.
#[derive(ResolvedObject, Clone)]
#[graphql(get_type_name, register(T::Description))]
pub struct GqlWindowSet<T: WindowItem> {
    pub(crate) ws: WindowSet<'static, T>,
    ctx: T::Ctx,
}

impl<T: WindowItem> TypeName for GqlWindowSet<T> {
    fn get_type_name() -> Cow<'static, str> {
        T::NAME.into()
    }
}

/// A window set whose windows need nothing beyond the window itself.
impl<T: WindowItem<Ctx = ()>> GqlWindowSet<T> {
    pub(crate) fn new(ws: WindowSet<'static, T>) -> Self {
        Self { ws, ctx: () }
    }
}

/// A graph's windows are built in the graph's folder.
impl GqlWindowSet<DynamicGraph> {
    pub(crate) fn new(ws: WindowSet<'static, DynamicGraph>, path: UnlockedGraphFolder) -> Self {
        Self { ws, ctx: path }
    }
}

#[ResolvedObjectFields]
impl<T: WindowItem> GqlWindowSet<T> {
    /// Number of windows in this set. Materialising all windows is expensive for
    /// large graphs — prefer `page` over `list` when iterating.
    pub async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ws.clone().count()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.

    pub async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of items to return on this page.")] limit: usize,
        #[graphql(desc = "Extra items to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<T::Gql>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .ws
                .clone()
                .skip(start)
                .take(limit)
                .map(|w| T::to_gql(w, &self_clone.ctx))
                .collect()
        })
        .await)
    }

    /// Materialise every window as a list. Rejected by the server when bulk list
    /// endpoints are disabled; use `page` for paginated access instead.
    pub async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<T::Gql>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            self_clone
                .ws
                .clone()
                .map(|w| T::to_gql(w, &self_clone.ctx))
                .collect()
        })
        .await)
    }
}

pub type GqlGraphWindowSet = GqlWindowSet<DynamicGraph>;
pub type GqlNodeWindowSet = GqlWindowSet<NodeView<'static, DynamicGraph>>;
pub type GqlNodesWindowSet =
    GqlWindowSet<Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>>;
pub type GqlPathFromNodeWindowSet = GqlWindowSet<PathFromNode<'static, DynamicGraph>>;
pub type GqlEdgeWindowSet = GqlWindowSet<EdgeView<DynamicGraph>>;
pub type GqlEdgesWindowSet = GqlWindowSet<Edges<'static, DynamicGraph, DynEdgeItem>>;

impl WindowItem for DynamicGraph {
    type Gql = GqlGraph;
    type Ctx = UnlockedGraphFolder;
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "GraphWindowSet";
    const DESCRIPTION: &'static str =
        "A lazy sequence of graph snapshots produced by `rolling` or `expanding`.\n\
        Each entry is a `Graph` at a different window over time. Iterate via\n\
        `list` / `page` (or count with `count`). Subsequent view ops apply\n\
        per-window.";

    fn to_gql(window: Self::WindowedViewType, path: &UnlockedGraphFolder) -> GqlGraph {
        GqlGraph::new(path.clone(), window)
    }
}

impl WindowItem for NodeView<'static, DynamicGraph> {
    type Gql = GqlNode;
    type Ctx = ();
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "NodeWindowSet";
    const DESCRIPTION: &'static str =
        "A lazy sequence of per-window views of a single node, produced by\n\
        `node.rolling` / `node.expanding`. Each entry is the node as it exists in\n\
        that window.";

    fn to_gql(window: Self::WindowedViewType, _: &()) -> GqlNode {
        window.into()
    }
}

impl WindowItem for Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter> {
    type Gql = GqlNodes;
    type Ctx = ();
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "NodesWindowSet";
    const DESCRIPTION: &'static str =
        "A lazy sequence of per-window node collections, produced by\n\
        `nodes.rolling` / `nodes.expanding`. Each entry is a `Nodes` collection\n\
        as it exists in that window.";

    fn to_gql(window: Self::WindowedViewType, _: &()) -> GqlNodes {
        GqlNodes::new(window)
    }
}

impl WindowItem for PathFromNode<'static, DynamicGraph> {
    type Gql = GqlPathFromNode;
    type Ctx = ();
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "PathFromNodeWindowSet";
    const DESCRIPTION: &'static str = "A lazy sequence of per-window neighbour sets, produced by\n\
        `neighbours.rolling` / `neighbours.expanding` (or the in/out variants).\n\
        Each entry is a `PathFromNode` scoped to that window.";

    fn to_gql(window: Self::WindowedViewType, _: &()) -> GqlPathFromNode {
        GqlPathFromNode::new(window)
    }
}

impl WindowItem for EdgeView<DynamicGraph> {
    type Gql = GqlEdge;
    type Ctx = ();
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "EdgeWindowSet";
    const DESCRIPTION: &'static str =
        "A lazy sequence of per-window views of a single edge, produced by\n\
        `edge.rolling` / `edge.expanding`. Each entry is the edge as it exists in\n\
        that window.";

    fn to_gql(window: Self::WindowedViewType, _: &()) -> GqlEdge {
        window.into()
    }
}

impl WindowItem for Edges<'static, DynamicGraph, DynEdgeItem> {
    type Gql = GqlEdges;
    type Ctx = ();
    type Description = WindowSetDescription<Self>;
    const NAME: &'static str = "EdgesWindowSet";
    const DESCRIPTION: &'static str = "A lazy sequence of per-window edge collections, produced by `edges.rolling` / `edges.expanding`. Each entry is an `Edges` collection as it exists in that window.";

    fn to_gql(window: Self::WindowedViewType, _: &()) -> GqlEdges {
        GqlEdges::new(window)
    }
}
