use crate::{
    db::graph::views::filter::model::{
        node_filter::{NodeNameFilter, NodeTypeFilter},
        Wrap,
    },
    prelude::Filter,
};
use std::{ops::Deref, sync::Arc};

pub trait InternalNodeIdFilterBuilderOps: Send + Sync + Wrap {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeIdFilterBuilderOps> InternalNodeIdFilterBuilderOps for Arc<T> {
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

#[derive(Clone, Debug)]
pub struct NodeIdFilterBuilder;

impl Wrap for NodeIdFilterBuilder {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalNodeIdFilterBuilderOps for NodeIdFilterBuilder {
    #[inline]
    fn field_name(&self) -> &'static str {
        "node_id"
    }
}

pub trait InternalNodeFilterBuilderOps: Send + Sync + Wrap {
    type FilterType: From<Filter>;
    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeFilterBuilderOps> InternalNodeFilterBuilderOps for Arc<T> {
    type FilterType = T::FilterType;
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

#[derive(Clone, Debug)]
pub struct NodeNameFilterBuilder;

impl Wrap for NodeNameFilterBuilder {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalNodeFilterBuilderOps for NodeNameFilterBuilder {
    type FilterType = NodeNameFilter;

    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

#[derive(Clone, Debug)]
pub struct NodeTypeFilterBuilder;

impl Wrap for NodeTypeFilterBuilder {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalNodeFilterBuilderOps for NodeTypeFilterBuilder {
    type FilterType = NodeTypeFilter;
    fn field_name(&self) -> &'static str {
        "node_type"
    }
}
