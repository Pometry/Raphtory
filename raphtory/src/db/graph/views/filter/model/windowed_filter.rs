use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{ComposableFilter, CreateView, InternalViewWrapOps, Wrap},
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{fmt, fmt::Display};
use crate::db::graph::views::filter::model::EdgeViewFilterOps;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windowed<M> {
    pub start: EventTime,
    pub end: EventTime,
    pub inner: M,
}

impl<M: Display> Display for Windowed<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WINDOW[{}..{}]({})",
            self.start.t(),
            self.end.t(),
            self.inner
        )
    }
}

impl<M> Windowed<M> {
    #[inline]
    pub fn new(start: EventTime, end: EventTime, entity: M) -> Self {
        Self {
            start,
            end,
            inner: entity,
        }
    }

    #[inline]
    pub fn from_times<S: IntoTime, E: IntoTime>(start: S, end: E, entity: M) -> Self {
        let s = start.into_time();
        let e = end.into_time();
        Self::new(s, e, entity)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for Windowed<T> {
    type Window = T::Window;

    fn bounds(&self) -> (EventTime, EventTime) {
        (self.start, self.end)
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.inner.build_window(start, end)
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Windowed<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>,
    {
        self.inner
            .create_filter(graph.window(self.start.t(), self.end.t()))
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph,
    {
        self.inner
            .create_node_filter(graph.window(self.start.t(), self.end.t()))
    }
}

impl<T: ComposableFilter> ComposableFilter for Windowed<T> {}

impl<M> Wrap for Windowed<M> {
    type Wrapped<T> = Windowed<T>;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Windowed::new(self.start, self.end, value)
    }
}

impl<T: CreateView> CreateView for Windowed<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.window(self.start.t(), self.end.t()))
    }
}

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for Windowed<T> {}
