use crate::db::graph::views::filter::model::{
    property_filter::{Op, PropertyFilter, PropertyFilterInput, PropertyRef},
    CombinedFilter, EntityMarker, InternalPropertyFilterBuilder, TemporalPropertyFilterFactory,
    Wrap,
};

#[derive(Clone)]
pub struct PropertyFilterBuilder<M>(pub String, pub M);

impl<M> PropertyFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> Wrap for PropertyFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> InternalPropertyFilterBuilder for PropertyFilterBuilder<M>
where
    M: Into<EntityMarker> + Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder,
{
    type Filter = PropertyFilter<M>;
    type ExprBuilder = PropertyExprBuilder<M>;
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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        filter.with_entity(self.entity())
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}

impl<T> TemporalPropertyFilterFactory for PropertyFilterBuilder<T>
where
    T: Into<EntityMarker> + Send + Sync + Clone + 'static,
    PropertyFilter<T>: CombinedFilter,
{
}

#[derive(Clone)]
pub struct MetadataFilterBuilder<M>(pub String, pub M);

impl<M> MetadataFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> Wrap for MetadataFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> InternalPropertyFilterBuilder for MetadataFilterBuilder<M>
where
    M: Into<EntityMarker> + Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder,
{
    type Filter = PropertyFilter<M>;
    type ExprBuilder = PropertyExprBuilder<M>;
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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        filter.with_entity(self.entity())
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}

pub struct PropertyExprBuilderInput {
    pub prop_ref: PropertyRef,
    pub ops: Vec<Op>,
}

impl PropertyExprBuilderInput {
    pub fn with_entity<M>(self, entity: M) -> PropertyExprBuilder<M> {
        PropertyExprBuilder {
            prop_ref: self.prop_ref,
            ops: self.ops,
            entity,
        }
    }
}

#[derive(Clone)]
pub struct PropertyExprBuilder<M> {
    pub prop_ref: PropertyRef,
    pub ops: Vec<Op>,
    pub entity: M,
}

impl<M> PropertyExprBuilder<M> {
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

impl<M> Wrap for PropertyExprBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> InternalPropertyFilterBuilder for PropertyExprBuilder<M>
where
    M: Into<EntityMarker> + Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
{
    type Filter = PropertyFilter<M>;
    type ExprBuilder = PropertyExprBuilder<M>;
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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        filter.with_entity(self.entity())
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}
