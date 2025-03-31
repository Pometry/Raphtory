use dynamic_graphql::SimpleObject;

#[derive(SimpleObject, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub(crate) struct PropertySchema {
    key: String,
    property_type: String,
    variants: Vec<String>,
}

impl PropertySchema {
    pub(crate) fn new(key: String, property_type: String, variants: Vec<String>) -> Self {
        PropertySchema {
            key,
            property_type,
            variants,
        }
    }
}

impl<S: AsRef<str>, I: IntoIterator<Item = S>> From<((S, S), I)> for PropertySchema {
    fn from(value: ((S, S), I)) -> Self {
        let ((key, prop_type), set) = value;
        PropertySchema {
            key: key.as_ref().to_string(),
            property_type: prop_type.as_ref().to_string(),
            variants: Vec::from_iter(set.into_iter().map(|s| s.as_ref().to_string())),
        }
    }
}
