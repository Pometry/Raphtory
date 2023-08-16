use dynamic_graphql::SimpleObject;
use raphtory::db::api::{properties::Properties, view::internal::BoxableGraphView};
use std::collections::HashSet;

#[derive(SimpleObject)]
pub(crate) struct PropertySchema {
    key: String,
    variants: Vec<String>,
}

// impl PropertySchema {
//     pub fn new(key: String, values: Vec<String>) -> Self {
//         Self { key, values }
//     }
// }

impl From<(String, HashSet<String>)> for PropertySchema {
    fn from(value: (String, HashSet<String>)) -> Self {
        let (key, set) = value;
        PropertySchema {
            key,
            variants: Vec::from_iter(set),
        }
    }
}
