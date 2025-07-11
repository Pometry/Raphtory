use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::{CreateEdgeFilter, CreateExplodedEdgeFilter, CreateNodeFilter},
            model::{
                AsEdgeFilter, AsNodeFilter, InternalNodeFilterBuilderOps, NodeFilterBuilderOps,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
    python::types::{
        repr::Repr,
        wrappers::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
    },
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::{ops::Deref, sync::Arc};

impl Repr for Prop {
    fn repr(&self) -> String {
        match &self {
            Prop::Str(v) => v.repr(),
            Prop::Bool(v) => v.repr(),
            Prop::I64(v) => v.repr(),
            Prop::U8(v) => v.repr(),
            Prop::U16(v) => v.repr(),
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::NDTime(v) => v.repr(),
            Prop::Array(v) => format!("{:?}", v),
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
            Prop::Decimal(v) => v.repr(),
        }
    }
}

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(i64, Prop)>;

impl CreateNodeFilter for PyFilterExpr {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        match self.0 {
            PyInnerFilterExpr::Node(i) => i.create_node_filter(graph),
            PyInnerFilterExpr::Property(i) => i.create_node_filter(graph),
            PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
        }
    }
}

pub trait DynNodeFilterBuilderOps: Send + Sync {
    fn eq(&self, value: String) -> PyFilterExpr;

    fn ne(&self, value: String) -> PyFilterExpr;

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr;

    fn contains(&self, value: String) -> PyFilterExpr;

    fn not_contains(&self, value: String) -> PyFilterExpr;

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr;
}

impl<T> DynNodeFilterBuilderOps for T
where
    T: InternalNodeFilterBuilderOps,
{
    fn eq(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(NodeFilterBuilderOps::eq(
            self, value,
        ))))
    }

    fn ne(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(NodeFilterBuilderOps::ne(
            self, value,
        ))))
    }

    fn is_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::is_in(self, values),
        )))
    }

    fn is_not_in(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::is_not_in(self, values),
        )))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::contains(self, value),
        )))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::not_contains(self, value),
        )))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
            NodeFilterBuilderOps::fuzzy_search(self, value, levenshtein_distance, prefix_match),
        )))
    }
}

pub trait DynInternalNodeFilterOps: AsNodeFilter {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateNodeFilter + AsNodeFilter + Clone + 'static> DynInternalNodeFilterOps for T {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }
}

impl<T: DynInternalNodeFilterOps + ?Sized + 'static> CreateNodeFilter for Arc<T> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_node_filter(Arc::new(graph))
    }
}

impl CreateEdgeFilter for PyFilterExpr {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        match self.0 {
            PyInnerFilterExpr::Edge(i) => i.create_edge_filter(graph),
            PyInnerFilterExpr::Property(i) => i.create_edge_filter(graph),
            PyInnerFilterExpr::Node(_) => Err(GraphError::NodeFilterIsNotEdgeFilter),
        }
    }
}

impl CreateExplodedEdgeFilter for PyFilterExpr {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        match self.0 {
            PyInnerFilterExpr::Node(_) => Err(GraphError::NotExplodedEdgeFilter),
            PyInnerFilterExpr::Edge(_) => Err(GraphError::NotExplodedEdgeFilter),
            PyInnerFilterExpr::Property(i) => i.create_exploded_edge_filter(graph),
        }
    }
}

pub trait DynInternalEdgeFilterOps: AsEdgeFilter {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateEdgeFilter + AsEdgeFilter + Clone + 'static> DynInternalEdgeFilterOps for T {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_edge_filter(graph)?))
    }
}

impl<T: DynInternalEdgeFilterOps + ?Sized + 'static> CreateEdgeFilter for Arc<T> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        self.deref().create_dyn_edge_filter(Arc::new(graph))
    }
}

pub trait DynCreateExplodedEdgeFilter {
    fn create_dyn_exploded_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: CreateExplodedEdgeFilter + Clone + 'static> DynCreateExplodedEdgeFilter for T {
    fn create_dyn_exploded_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_exploded_edge_filter(graph)?))
    }
}

impl<T: DynCreateExplodedEdgeFilter + ?Sized + 'static> CreateExplodedEdgeFilter for Arc<T> {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        self.deref()
            .create_dyn_exploded_edge_filter(Arc::new(graph))
    }
}
