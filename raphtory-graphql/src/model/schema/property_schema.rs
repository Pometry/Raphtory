use crate::model::graph::property::GqlPropTypeOutput;
use dynamic_graphql::SimpleObject;
use raphtory_api::core::entities::properties::prop::PropType;

#[derive(SimpleObject, Clone, Debug)]
pub struct PropertySchema {
    key: String,
    /// The type rendered as text — kept for existing consumers; prefer `dtype`.
    property_type: String,
    /// The structured property type.
    dtype: GqlPropTypeOutput,
    variants: Vec<String>,
}

impl PropertySchema {
    pub(crate) fn new(key: String, dtype: PropType, variants: Vec<String>) -> Self {
        PropertySchema {
            key,
            property_type: dtype.to_string(),
            dtype: GqlPropTypeOutput(dtype),
            variants,
        }
    }
}

impl PartialEq for PropertySchema {
    fn eq(&self, other: &Self) -> bool {
        (&self.key, &self.property_type, &self.variants)
            == (&other.key, &other.property_type, &other.variants)
    }
}
impl Eq for PropertySchema {}
impl PartialOrd for PropertySchema {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PropertySchema {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.key, &self.property_type, &self.variants).cmp(&(
            &other.key,
            &other.property_type,
            &other.variants,
        ))
    }
}

impl<S: AsRef<str>, I: IntoIterator<Item = S>> From<((S, PropType), I)> for PropertySchema {
    fn from(value: ((S, PropType), I)) -> Self {
        let ((key, prop_type), set) = value;
        PropertySchema::new(
            key.as_ref().to_string(),
            prop_type,
            Vec::from_iter(set.into_iter().map(|s| s.as_ref().to_string())),
        )
    }
}
