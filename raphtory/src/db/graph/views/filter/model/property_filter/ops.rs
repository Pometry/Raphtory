use crate::db::graph::views::filter::model::{
    property_filter::{builders::OpChainBuilder, Op, PropertyFilter, PropertyFilterValue},
    FilterOperator, InternalPropertyFilterBuilder,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
use std::sync::Arc;

pub trait PropertyFilterOps: InternalPropertyFilterBuilder {
    fn eq(&self, value: impl Into<Prop>) -> Self::Filter;
    fn ne(&self, value: impl Into<Prop>) -> Self::Filter;
    fn le(&self, value: impl Into<Prop>) -> Self::Filter;
    fn ge(&self, value: impl Into<Prop>) -> Self::Filter;
    fn lt(&self, value: impl Into<Prop>) -> Self::Filter;
    fn gt(&self, value: impl Into<Prop>) -> Self::Filter;
    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> Self::Filter;
    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> Self::Filter;
    fn is_none(&self) -> Self::Filter;
    fn is_some(&self) -> Self::Filter;
    fn starts_with(&self, value: impl Into<Prop>) -> Self::Filter;
    fn ends_with(&self, value: impl Into<Prop>) -> Self::Filter;
    fn contains(&self, value: impl Into<Prop>) -> Self::Filter;
    fn not_contains(&self, value: impl Into<Prop>) -> Self::Filter;
    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Filter;
}

impl<T: ?Sized + InternalPropertyFilterBuilder> PropertyFilterOps for T {
    fn eq(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Eq,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn ne(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Ne,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn le(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Le,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn ge(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Ge,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn lt(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Lt,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn gt(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Gt,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Set(Arc::new(values.into_iter().collect())),
            operator: FilterOperator::IsIn,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Set(Arc::new(values.into_iter().collect())),
            operator: FilterOperator::IsNotIn,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn is_none(&self) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn is_some(&self) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn starts_with(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::StartsWith,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn ends_with(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::EndsWith,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn contains(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Contains,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn not_contains(&self, value: impl Into<Prop>) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::NotContains,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Filter {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(Prop::Str(ArcStr::from(prop_value.into()))),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.filter(filter)
    }
}

pub trait ElemQualifierOps: InternalPropertyFilterBuilder {
    fn any(&self) -> Self::Chained
    where
        Self: Sized,
    {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Any]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn all(&self) -> Self::Chained
    where
        Self: Sized,
    {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::All]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }
}

impl<T: InternalPropertyFilterBuilder> ElemQualifierOps for T {}

pub trait ListAggOps: InternalPropertyFilterBuilder {
    fn len(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Len]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn sum(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Sum]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn avg(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Avg]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn min(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Min]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn max(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Max]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn first(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::First]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }

    fn last(&self) -> Self::Chained {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Last]).collect(),
            entity: self.entity(),
        };
        self.chained(builder)
    }
}

impl<T: InternalPropertyFilterBuilder> ListAggOps for T {}
