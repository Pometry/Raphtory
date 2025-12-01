use crate::db::graph::views::filter::model::{
    property_filter::{Op, PropertyFilter, PropertyRef},
    CombinedFilter, TemporalPropertyFilterFactory, Wrap,
};
use std::{ops::Deref, sync::Arc};

pub trait InternalPropertyFilterBuilderOps: Send + Sync {
    type Filter: CombinedFilter;
    type Chained: InternalPropertyFilterBuilderOps;
    type Marker: Send + Sync + Clone + 'static;

    fn property_ref(&self) -> PropertyRef;

    fn ops(&self) -> &[Op];

    fn entity(&self) -> Self::Marker;

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter;

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained;
}

impl<T: InternalPropertyFilterBuilderOps> InternalPropertyFilterBuilderOps for Arc<T> {
    type Filter = T::Filter;
    type Chained = T::Chained;
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.deref().ops()
    }

    fn entity(&self) -> Self::Marker {
        self.deref().entity()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.deref().filter(filter)
    }

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained {
        self.deref().chained(builder)
    }
}

#[derive(Clone)]
pub struct PropertyFilterBuilder<M>(String, pub M);

impl<M> Wrap for PropertyFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> PropertyFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> InternalPropertyFilterBuilderOps for PropertyFilterBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps,
{
    type Filter = PropertyFilter<M>;
    type Chained = OpChainBuilder<M>;
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.1.clone()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        filter
    }

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained {
        builder
    }
}

impl<T> TemporalPropertyFilterFactory for PropertyFilterBuilder<T>
where
    T: Send + Sync + Clone + 'static,
    PropertyFilter<T>: CombinedFilter,
{
}

#[derive(Clone)]
pub struct MetadataFilterBuilder<M>(pub String, pub M);

impl<M> Wrap for MetadataFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> MetadataFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> InternalPropertyFilterBuilderOps for MetadataFilterBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps,
{
    type Filter = PropertyFilter<M>;
    type Chained = OpChainBuilder<M>;
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Metadata(self.0.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.1.clone()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        filter
    }

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained {
        builder
    }
}

#[derive(Clone)]
pub struct OpChainBuilder<M> {
    pub prop_ref: PropertyRef,
    pub ops: Vec<Op>,
    pub entity: M,
}

impl<M> Wrap for OpChainBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> OpChainBuilder<M> {
    pub fn with_op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    pub fn with_ops(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.ops.extend(ops);
        self
    }

    pub fn first(self) -> Self {
        self.with_op(Op::First)
    }

    pub fn last(self) -> Self {
        self.with_op(Op::Last)
    }

    pub fn any(self) -> Self {
        self.with_op(Op::Any)
    }

    pub fn all(self) -> Self {
        self.with_op(Op::All)
    }

    pub fn len(self) -> Self {
        self.with_op(Op::Len)
    }

    pub fn sum(self) -> Self {
        self.with_op(Op::Sum)
    }

    pub fn avg(self) -> Self {
        self.with_op(Op::Avg)
    }

    pub fn min(self) -> Self {
        self.with_op(Op::Min)
    }

    pub fn max(self) -> Self {
        self.with_op(Op::Max)
    }
}

impl<M> InternalPropertyFilterBuilderOps for OpChainBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
{
    type Filter = PropertyFilter<M>;
    type Chained = OpChainBuilder<M>;
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.prop_ref.clone()
    }

    fn ops(&self) -> &[Op] {
        &self.ops
    }

    fn entity(&self) -> Self::Marker {
        self.entity.clone()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        filter
    }

    fn chained(&self, builder: OpChainBuilder<Self::Marker>) -> Self::Chained {
        builder
    }
}

impl<M> PropertyFilterBuilder<M> {
    pub fn temporal(self) -> OpChainBuilder<M> {
        OpChainBuilder {
            prop_ref: PropertyRef::TemporalProperty(self.0),
            ops: vec![],
            entity: self.1,
        }
    }
}
