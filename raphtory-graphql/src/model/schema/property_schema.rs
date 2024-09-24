use dynamic_graphql::SimpleObject;
use std::collections::HashSet;

#[derive(SimpleObject)]
pub(crate) struct PropertySchema {
    key: String,
    property_type: String,
    variants: Vec<String>,
}

// impl PropertySchema {
//     pub fn new(key: String, values: Vec<String>) -> Self {
//         Self { key, values }
//     }
// }

impl From<((String, String), HashSet<String>)> for PropertySchema {
    fn from(value: ((String, String), HashSet<String>)) -> Self {
        let ((key, prop_type), set) = value;
        PropertySchema {
            key,
            property_type: prop_type,
            variants: Vec::from_iter(set),
        }
    }
}
