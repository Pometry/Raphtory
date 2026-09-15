use crate::{
    api::core::Direction,
    db::{
        api::state::{
            ops::node::{Id, Name, Type},
            NodeStateValue, TypedNodeState,
        },
        graph::views::filter::model::{
            is_active_node_filter::IsActiveNode,
            latest_filter::Latest,
            layered_filter::Layered,
            node_expr::{exprs::DegreeExpr, EntityExpr},
            node_state_filter::NodeStateBoolColOp,
            snapshot_filter::{SnapshotAt, SnapshotLatest},
            windowed_filter::Windowed,
            CombinedFilter, CreateView, EntityMarker, InternalViewWrapOps, NodeViewFilterOps,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct NodeFilter;

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

impl NodeViewFilterOps for NodeFilter {
    type Output<T: CombinedFilter> = T;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        IsActiveNode
    }
}

// ── expr-layer factory ──

pub trait NodeFilterFactory:
    InternalViewWrapOps<Window = Self::NodeWindow> + CreateView + EntityExpr
{
    type NodeWindow: NodeFilterFactory + NodeViewFilterOps;
    #[inline]
    fn id(&self) -> Id {
        Id
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns `Name` which implements `NodeExprFilterOps` — use `.eq("Alice")`,
    /// `.contains("ali")`, `.is_in([…])`, etc. directly on the returned value.
    #[inline]
    fn name(&self) -> Name {
        Name
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns `Type` which implements `NodeExprFilterOps`.
    #[inline]
    fn node_type(&self) -> Type {
        Type
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

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Windowed<T> {
    type NodeWindow = T::NodeWindow;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Latest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for SnapshotAt<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for SnapshotLatest<T> {
    type NodeWindow = Self::Window;
}

impl<T: NodeFilterFactory + NodeViewFilterOps> NodeFilterFactory for Layered<T> {
    type NodeWindow = Self::Window;
}
