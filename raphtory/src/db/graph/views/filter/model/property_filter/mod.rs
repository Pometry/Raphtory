use crate::db::graph::views::filter::model::FilterOperator;
use itertools::Itertools;
use raphtory_api::core::entities::properties::prop::{
    prop_hashable::HashableProp, sort_comparable_props, Prop,
};
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};

pub(crate) mod evaluate;
mod validate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    // Selectors
    First,
    Last,
    // Aggregators
    Len,
    Sum,
    Avg,
    Min,
    Max,
    // Qualifiers (quantifiers)
    Any,
    All,
}

impl Op {
    #[inline]
    pub fn is_selector(self) -> bool {
        matches!(self, Op::First | Op::Last)
    }

    #[inline]
    pub fn is_aggregator(self) -> bool {
        matches!(self, Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max)
    }

    #[inline]
    pub fn is_qualifier(self) -> bool {
        matches!(self, Op::Any | Op::All)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRef {
    Property(String),
    Metadata(String),
    TemporalProperty(String),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name) => write!(f, "TemporalProperty({})", name),
            PropertyRef::Metadata(name) => write!(f, "Metadata({})", name),
            PropertyRef::Property(name) => write!(f, "Property({})", name),
        }
    }
}

impl PropertyRef {
    pub fn name(&self) -> &str {
        match self {
            PropertyRef::Property(name)
            | PropertyRef::Metadata(name)
            | PropertyRef::TemporalProperty(name) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<HashableProp>>),
}

pub struct PropertyFilterInput {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
    pub ops: Vec<Op>, // validated by validate_chain_and_infer_effective_dtype
}

impl PropertyFilterInput {
    pub fn with_entity<M>(self, entity: M) -> PropertyFilter<M> {
        PropertyFilter {
            prop_ref: self.prop_ref,
            prop_value: self.prop_value,
            operator: self.operator,
            ops: self.ops,
            entity,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PropertyFilter<M> {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
    pub ops: Vec<Op>, // validated by validate_chain_and_infer_effective_dtype
    pub entity: M,
}

impl<M> Display for PropertyFilter<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut expr = match &self.prop_ref {
            PropertyRef::Property(name) => name.to_string(),
            PropertyRef::Metadata(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name) => format!("temporal({})", name),
        };

        for op in &self.ops {
            expr = match op {
                Op::First => format!("first({})", expr),
                Op::Last => format!("last({})", expr),
                Op::Len => format!("len({})", expr),
                Op::Sum => format!("sum({})", expr),
                Op::Avg => format!("avg({})", expr),
                Op::Min => format!("min({})", expr),
                Op::Max => format!("max({})", expr),
                Op::Any => format!("any({})", expr),
                Op::All => format!("all({})", expr),
            };
        }

        match &self.prop_value {
            PropertyFilterValue::None => write!(f, "{} {}", expr, self.operator),
            PropertyFilterValue::Single(value) => write!(f, "{} {} {}", expr, self.operator, value),
            PropertyFilterValue::Set(values) => {
                let sorted = sort_comparable_props(values.iter().map(Into::into).collect_vec());
                let values_str = sorted.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "{} {} [{}]", expr, self.operator, values_str)
            }
        }
    }
}

impl<M> PropertyFilter<M> {
    pub fn matches(&self, other: Option<&Prop>) -> bool {
        if self.ops.is_empty() {
            return self.operator.apply_to_property(&self.prop_value, other);
        }
        self.eval_scalar_and_apply(other.cloned())
    }
}
