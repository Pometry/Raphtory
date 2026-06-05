use crate::db::graph::views::filter::model::{
    filter::Filter, node_filter::builders::InternalNodeFilterBuilder,
};

pub trait NodeFilterOps: InternalNodeFilterBuilder {
    fn eq(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::eq(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn ne(&self, value: impl Into<String>) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::ne(self.field_name(), value);
        self.wrap(filter.into())
    }

    fn is_in(
        &self,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self::Wrapped<Self::FilterType> {
        let filter = Filter::is_in(self.field_name(), values);
        self.wrap(filter.into())
    }

    fn is_not_in(
        &self,
        values: impl IntoIterator<Item = impl Into<String>>,
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
