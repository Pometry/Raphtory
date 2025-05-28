use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::{CreateNodeFilter, InternalEdgeFilterOps, InternalExplodedEdgeFilterOps},
            model::{
                property_filter::PropertyRef, AsEdgeFilter, AsNodeFilter,
                InternalNodeFilterBuilderOps, NodeFilterBuilderOps,
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
    python::types::{
        repr::Repr,
        wrappers::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
    },
};
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::prop::Prop;
use std::{collections::HashSet, ops::Deref, sync::Arc};

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

#[pyclass(frozen, name = "PropertyFilter", module = "raphtory")]
#[derive(Clone)]
pub struct PyPropertyFilter(PropertyFilter);

impl InternalEdgeFilterOps for PyPropertyFilter {
    type EdgeFiltered<'graph, G>
        = <PropertyFilter as InternalEdgeFilterOps>::EdgeFiltered<'graph, G>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        self.0.create_edge_filter(graph)
    }
}

impl InternalExplodedEdgeFilterOps for PyPropertyFilter {
    type ExplodedEdgeFiltered<'graph, G>
        = <PropertyFilter as InternalExplodedEdgeFilterOps>::ExplodedEdgeFiltered<'graph, G>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        self.0.create_exploded_edge_filter(graph)
    }
}

impl CreateNodeFilter for PyPropertyFilter {
    type NodeFiltered<'graph, G>
        = <PropertyFilter as CreateNodeFilter>::NodeFiltered<'graph, G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        self.0.create_node_filter(graph)
    }
}

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

impl InternalEdgeFilterOps for PyFilterExpr {
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
            PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
        }
    }
}

pub trait DynInternalEdgeFilterOps: AsEdgeFilter {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: InternalEdgeFilterOps + AsEdgeFilter + Clone + 'static> DynInternalEdgeFilterOps for T {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_edge_filter(graph)?))
    }
}

impl<T: DynInternalEdgeFilterOps + ?Sized + 'static> InternalEdgeFilterOps for Arc<T> {
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

/// A reference to a property used for constructing filters
///
/// Use `==`, `!=`, `<`, `<=`, `>`, `>=` to filter based on
/// property value (these filters always exclude entities that do not
/// have the property) or use one of the methods to construct
/// other kinds of filters.
///
/// Arguments:
///     name (str): the name of the property
#[pyclass(frozen, name = "Prop", module = "raphtory")]
#[derive(Clone)]
pub struct PyPropertyRef {
    name: String,
}

#[pymethods]
impl PyPropertyRef {
    #[new]
    fn new(name: String) -> Self {
        PyPropertyRef { name }
    }

    fn __eq__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::eq(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __ne__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ne(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __lt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::lt(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __le__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::le(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __gt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::gt(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __ge__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ge(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities if they have the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_some(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_some(PropertyRef::Property(self.name.clone()));
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities that do not have the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_none(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_none(PropertyRef::Property(self.name.clone()));
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities that contains the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn contains(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::contains(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities that do not contain the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn not_contains(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::not_contains(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is in the set
    ///
    /// Arguments:
    ///     values (set[PropValue]): the set of values to match
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_in(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::is_in(PropertyRef::Property(self.name.clone()), values);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is not in the set or
    /// if they don't have the property
    ///
    /// Arguments:
    ///     values (set[PropValue]): the set of values to exclude
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_not_in(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::is_not_in(PropertyRef::Property(self.name.clone()), values);
        PyPropertyFilter(filter)
    }
}
