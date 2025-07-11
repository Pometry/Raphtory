use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, AndFilter,
        AsEdgeFilter, AsNodeFilter, NotFilter, OrFilter,
    },
    errors::GraphError,
    python::types::wrappers::prop::{
        DynCreateExplodedEdgeFilter, DynInternalEdgeFilterOps, DynInternalNodeFilterOps,
    },
};
use pyo3::{pyclass, pymethods};
use std::sync::Arc;

pub trait AsPropertyFilter:
    DynInternalNodeFilterOps + DynInternalEdgeFilterOps + DynCreateExplodedEdgeFilter
{
}

impl<
        T: DynInternalNodeFilterOps + DynInternalEdgeFilterOps + DynCreateExplodedEdgeFilter + ?Sized,
    > AsPropertyFilter for T
{
}

#[derive(Clone)]
pub enum PyInnerFilterExpr {
    Node(Arc<dyn DynInternalNodeFilterOps>),
    Edge(Arc<dyn DynInternalEdgeFilterOps>),
    Property(Arc<dyn AsPropertyFilter>),
}

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyFilterExpr(pub PyInnerFilterExpr);

impl PyFilterExpr {
    pub fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => Ok(i.as_node_filter()),
            PyInnerFilterExpr::Property(i) => Ok(i.as_node_filter()),
            PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
        }
    }

    pub fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Edge(i) => Ok(i.as_edge_filter()),
            PyInnerFilterExpr::Property(i) => Ok(i.as_edge_filter()),
            PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
        }
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => match &other.0 {
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Edge(i) => match &other.0 {
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Property(i) => match &other.0 {
                PyInnerFilterExpr::Property(j) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                    Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }),
                ))),
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
            },
        }
    }

    pub fn __or__(&self, other: &Self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => match &other.0 {
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Edge(i) => match &other.0 {
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Property(i) => match &other.0 {
                PyInnerFilterExpr::Property(j) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                    Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }),
                ))),
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
            },
        }
    }

    fn __invert__(&self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
                NotFilter(i.clone()),
            )))),
            PyInnerFilterExpr::Edge(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(
                NotFilter(i.clone()),
            )))),
            PyInnerFilterExpr::Property(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                Arc::new(NotFilter(i.clone())),
            ))),
        }
    }
}
