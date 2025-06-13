use crate::{
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::{node::NodeViewOps, EdgeViewOps},
        },
        graph::{
            edge::EdgeView, node::NodeView, views::filter::model::filter_operator::FilterOperator,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::{
        meta::Meta,
        prop::{sort_comparable_props, Prop, PropType},
    },
    storage::arc_str::ArcStr,
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Temporal {
    Any,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRef {
    Property(String),
    ConstantProperty(String),
    TemporalProperty(String, Temporal),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name, temporal) => {
                write!(f, "TemporalProperty({}, {:?})", name, temporal)
            }
            PropertyRef::ConstantProperty(name) => write!(f, "ConstantProperty({})", name),
            PropertyRef::Property(name) => write!(f, "Property({})", name),
        }
    }
}

impl PropertyRef {
    pub fn name(&self) -> &str {
        match self {
            PropertyRef::Property(name)
            | PropertyRef::ConstantProperty(name)
            | PropertyRef::TemporalProperty(name, _) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyFilter {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
}

impl Display for PropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prop_ref_str = match &self.prop_ref {
            PropertyRef::Property(name) => name.to_string(),
            PropertyRef::ConstantProperty(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Any) => format!("temporal_any({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Latest) => {
                format!("temporal_latest({})", name)
            }
        };

        match &self.prop_value {
            PropertyFilterValue::None => {
                write!(f, "{} {}", prop_ref_str, self.operator)
            }
            PropertyFilterValue::Single(value) => {
                write!(f, "{} {} {}", prop_ref_str, self.operator, value)
            }
            PropertyFilterValue::Set(values) => {
                let sorted_values = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", prop_ref_str, self.operator, values_str)
            }
        }
    }
}

impl PropertyFilter {
    pub fn eq(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn le(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
        }
    }

    pub fn ge(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
        }
    }

    pub fn lt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
        }
    }

    pub fn gt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
        }
    }

    pub fn is_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn is_not_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn is_none(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
        }
    }

    pub fn is_some(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
        }
    }

    pub fn contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Contains,
        }
    }

    pub fn not_contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::NotContains,
        }
    }

    pub fn fuzzy_search(
        prop_ref: PropertyRef,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(Prop::Str(ArcStr::from(prop_value.into()))),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
        }
    }

    pub fn resolve_temporal_prop_id(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        match self.prop_ref {
            PropertyRef::ConstantProperty(_) => Ok(None),
            _ => {
                let prop_name = self.prop_ref.name();
                if let PropertyFilterValue::Single(value) = &self.prop_value {
                    Ok(meta
                        .temporal_prop_meta()
                        .get_and_validate(prop_name, value.dtype())?)
                } else {
                    Ok(meta.temporal_prop_meta().get_id(prop_name))
                }
            }
        }
    }

    pub fn resolve_constant_prop_id(
        &self,
        meta: &Meta,
        resolve_to_map: bool,
    ) -> Result<Option<usize>, GraphError> {
        let prop_name = self.prop_ref.name();
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            if resolve_to_map {
                return if let PropType::Map(map) = value.dtype() {
                    if let Some((_k, v)) = map.iter().next() {
                        Ok(meta
                            .const_prop_meta()
                            .get_and_validate(prop_name, v.clone())?)
                    } else {
                        Err(GraphError::InvalidProperty {
                            reason: "Empty constant property map".to_owned(),
                        })?
                    }
                } else {
                    Err(GraphError::InvalidProperty {
                        reason: "Expected PropType::Map".to_owned(),
                    })?
                };
            }
            Ok(meta
                .const_prop_meta()
                .get_and_validate(prop_name, value.dtype())?)
        } else {
            Ok(meta.const_prop_meta().get_id(prop_name))
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        let value = &self.prop_value;
        self.operator.apply_to_property(value, other)
    }

    fn is_property_matched<I: PropertiesOps + Clone>(
        &self,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        props: Properties<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Property(_) => {
                let prop_value = t_prop_id
                    .and_then(|prop_id| {
                        props
                            .temporal()
                            .get_by_id(prop_id)
                            .and_then(|prop_view| prop_view.latest())
                    })
                    .or_else(|| c_prop_id.and_then(|prop_id| props.constant().get_by_id(prop_id)));
                self.matches(prop_value.as_ref())
            }
            PropertyRef::ConstantProperty(_) => {
                let prop_value = c_prop_id.and_then(|prop_id| props.constant().get_by_id(prop_id));
                self.matches(prop_value.as_ref())
            }
            PropertyRef::TemporalProperty(_, Temporal::Any) => t_prop_id.is_some_and(|prop_id| {
                props
                    .temporal()
                    .get_by_id(prop_id)
                    .filter(|prop_view| prop_view.values().any(|v| self.matches(Some(&v))))
                    .is_some()
            }),
            PropertyRef::TemporalProperty(_, Temporal::Latest) => {
                let prop_value = t_prop_id.and_then(|prop_id| {
                    props
                        .temporal()
                        .get_by_id(prop_id)
                        .and_then(|prop_view| prop_view.latest())
                });
                self.matches(prop_value.as_ref())
            }
        }
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        node: NodeStorageRef,
    ) -> bool {
        let props = NodeView::new_internal(graph, node.vid()).properties();
        self.is_property_matched(t_prop_id, c_prop_id, props)
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        edge: EdgeStorageRef,
    ) -> bool {
        let props = EdgeView::new(graph, edge.out_ref()).properties();
        self.is_property_matched(t_prop_id, c_prop_id, props)
    }
}
