use crate::db::{
    api::state::ops::{Name, Type},
    graph::views::filter::model::{
        filter::Filter,
        node_filter::{NodeNameFilter, NodeTypeFilter},
        Wrap,
    },
};
use std::{ops::Deref, sync::Arc};

// TODO: remove this trait (and NodeFilterOps, NodeNameFilter, NodeTypeFilter, the search executors)
// when the Tantivy search feature is removed.
pub trait InternalNodeFilterBuilder: Send + Sync + Wrap {
    type FilterType: From<Filter>;
    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for Arc<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

impl Wrap for Name {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalNodeFilterBuilder for Name {
    type FilterType = NodeNameFilter;

    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

impl Wrap for Type {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalNodeFilterBuilder for Type {
    type FilterType = NodeTypeFilter;

    fn field_name(&self) -> &'static str {
        "node_type"
    }
}

// ── restored from db_v4 (lost in a silent auto-merge) ──

pub trait InternalNodeIdFilterBuilder: Send + Sync + Wrap {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder for Arc<T> {
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

impl InternalNodeIdFilterBuilder for NodeIdFilterBuilder {
    #[inline]
    fn field_name(&self) -> &'static str {
        "node_id"
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

impl InternalNodeFilterBuilder for NodeNameFilterBuilder {
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

impl InternalNodeFilterBuilder for NodeTypeFilterBuilder {
    type FilterType = NodeTypeFilter;
    fn field_name(&self) -> &'static str {
        "node_type"
    }
}
