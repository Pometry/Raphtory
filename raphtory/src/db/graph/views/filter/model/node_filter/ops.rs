use crate::db::graph::views::filter::model::{
    filter::Filter,
    node_filter::{
        builders::{InternalNodeFilterBuilder, InternalNodeIdFilterBuilder},
        NodeIdFilter,
    },
};
use raphtory_api::core::entities::GID;

pub trait NodeIdFilterOps: InternalNodeIdFilterBuilder {
    fn eq<T: Into<GID>>(&self, value: T) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::eq_id(self.field_name(), value);
        self.wrap(NodeIdFilter(filter))
    }

    fn ne<T: Into<GID>>(&self, value: T) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::ne_id(self.field_name(), value).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn is_in<I, T>(&self, values: I) -> Self::Wrapped<NodeIdFilter>
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        let filter = Filter::is_in_id(self.field_name(), values).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn is_not_in<I, T>(&self, values: I) -> Self::Wrapped<NodeIdFilter>
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        let filter = Filter::is_not_in_id(self.field_name(), values).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn lt<V: Into<GID>>(&self, value: V) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::lt(self.field_name(), value).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn le<V: Into<GID>>(&self, value: V) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::le(self.field_name(), value).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn gt<V: Into<GID>>(&self, value: V) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::gt(self.field_name(), value).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn ge<V: Into<GID>>(&self, value: V) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::ge(self.field_name(), value).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn starts_with<S: Into<String>>(&self, s: S) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::starts_with(self.field_name(), s.into()).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn ends_with<S: Into<String>>(&self, s: S) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::ends_with(self.field_name(), s.into()).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn contains<S: Into<String>>(&self, s: S) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::contains(self.field_name(), s.into()).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn not_contains<S: Into<String>>(&self, s: S) -> Self::Wrapped<NodeIdFilter> {
        let filter = Filter::not_contains(self.field_name(), s.into()).into();
        self.wrap(NodeIdFilter(filter))
    }

    fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Wrapped<NodeIdFilter> {
        let filter =
            Filter::fuzzy_search(self.field_name(), s, levenshtein_distance, prefix_match).into();
        self.wrap(NodeIdFilter(filter))
    }
}

impl<T: InternalNodeIdFilterBuilder + ?Sized> NodeIdFilterOps for T {}

pub trait NodeFilterOps: InternalNodeFilterBuilder {
    fn eq(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::eq(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn ne(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::ne(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::is_in(self.field_name(), values);
        self.wrap(filter.into())
    }

    fn is_not_in(
        &self,
        values: impl IntoIterator<Item = String>,
    ) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::is_not_in(self.field_name(), values);
        self.wrap(filter.into())
    }

    fn starts_with(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::starts_with(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn ends_with(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::ends_with(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn contains(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::contains(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn not_contains(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::not_contains(self.field_name(), value.into());
        self.wrap(filter.into())
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Wrapped<Self::FilterType> {
        let filter =
            Filter::fuzzy_search(self.field_name(), value, levenshtein_distance, prefix_match);
        self.wrap(filter.into())
    }
}

impl<T: InternalNodeFilterBuilder + ?Sized> NodeFilterOps for T {}
