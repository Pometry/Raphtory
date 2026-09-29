use crate::{
    api::core::Direction,
    db::{
        api::{
            state::{
                ops::node::{Id, Name, Type},
                NodeStateValue, TypedNodeState,
            },
            view::internal::Static,
        },
        graph::views::filter::model::{
            dyn_factory::DynNodeFilterFactory,
            latest_filter::Latest,
            layered_filter::Layered,
            node_expr::{
                exprs::{DegreeExpr, NodeFieldExpr},
                EntityExpr,
            },
            node_state_filter::NodeStateBoolColOp,
            snapshot_filter::{SnapshotAt, SnapshotLatest},
            windowed_filter::Windowed,
            CreateView, EntityMarker, InternalViewWrapOps,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl Static for NodeFilter {}

impl From<NodeFilter> for EntityMarker {
    fn from(_value: NodeFilter) -> Self {
        EntityMarker::Node
    }
}

impl InternalViewWrapOps for NodeFilter {
    type Window = Windowed<NodeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

// ── expr-layer factory ──

pub trait NodeFilterFactory:
    InternalViewWrapOps<Window = Self::NodeWindow> + CreateView + EntityExpr
{
    type NodeWindow: NodeFilterFactory + DynNodeFilterFactory;

    /// Selects the node id field for filtering, read through this factory's views.
    #[inline]
    fn id(&self) -> NodeFieldExpr<Self, Id> {
        NodeFieldExpr {
            view_expr: self.clone(),
            field: Id,
        }
    }

    /// Selects the node name field for filtering.
    ///
    /// Read through this factory's views: a node a view does not hold has no
    /// name there. Use `.eq("Alice")`, `.contains("ali")`, `.is_in([…])`, etc.
    /// directly on the returned value.
    #[inline]
    fn name(&self) -> NodeFieldExpr<Self, Name> {
        NodeFieldExpr {
            view_expr: self.clone(),
            field: Name,
        }
    }

    /// Selects the node type field for filtering.
    ///
    /// Read through this factory's views, like [`Self::name`].
    #[inline]
    fn node_type(&self) -> NodeFieldExpr<Self, Type> {
        NodeFieldExpr {
            view_expr: self.clone(),
            field: Type,
        }
    }

    /// Build a filter from a boolean column inside a TypedNodeState.
    fn by_column<'graph, V, G, T>(
        state: &TypedNodeState<'graph, V, G, T>,
        col: &str,
    ) -> Result<NodeStateBoolColOp, GraphError>
    where
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        Self: Sized,
    {
        state.bool_col_filter(col)
    }

    /// Total degree expression — supports `.gt(n)`, `.lt(n)`, etc.
    fn degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::BOTH,
            view_expr: self.clone(),
        }
    }

    /// In-degree expression.
    fn in_degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::IN,
            view_expr: self.clone(),
        }
    }

    /// Out-degree expression.
    #[inline]
    fn out_degree(&self) -> DegreeExpr<Self> {
        DegreeExpr {
            dir: Direction::OUT,
            view_expr: self.clone(),
        }
    }
}

impl NodeFilterFactory for NodeFilter {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory> NodeFilterFactory for Windowed<T> {
    type NodeWindow = T::NodeWindow;
}

impl<T: NodeFilterFactory> NodeFilterFactory for Latest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory> NodeFilterFactory for SnapshotAt<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory> NodeFilterFactory for SnapshotLatest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory> NodeFilterFactory for Layered<T> {
    type NodeWindow = Self::Window;
}
