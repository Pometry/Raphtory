//! Pure Rust GraphQL client for Raphtory GraphQL server.

mod error;
pub mod raphtory_client;
pub mod remote_graph;

pub use error::ClientError;
pub use remote_graph::GraphQLRemoteGraph;

use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;

/// Check if a server at the given URL is online (responds with 200).
pub fn is_online(url: &str) -> bool {
    reqwest::blocking::Client::new()
        .get(url)
        .send()
        .map(|response| response.status().as_u16() == 200)
        .unwrap_or(false)
}

pub(crate) fn inner_collection(value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::U8(value) => format!("{{ u64: {} }}", value),
        Prop::U16(value) => format!("{{ u64: {} }}", value),
        Prop::I32(value) => format!("{{ i64: {} }}", value),
        Prop::I64(value) => format!("{{ i64: {} }}", value),
        Prop::U32(value) => format!("{{ u64: {} }}", value),
        Prop::U64(value) => format!("{{ u64: {} }}", value),
        Prop::F32(value) => format!("{{ f64: {} }}", value),
        Prop::F64(value) => format!("{{ f64: {} }}", value),
        Prop::Bool(value) => format!("{{ bool: {} }}", value),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|p| inner_collection(&p)).collect();
            format!("{{ list: [{}] }}", vec.join(", "))
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{{ key: {}, value: {} }}",
                        serde_json::to_string(k).unwrap(),
                        inner_collection(v)
                    )
                })
                .collect();
            format!("{{ object: [{}] }}", properties_array.join(", "))
        }
        Prop::DTime(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::NDTime(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::Decimal(value) => format!("{{ decimal: {} }}", value),
    }
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::U8(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U16(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I32(value) => format!(
            "{{ key: {}, value: {{ i64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I64(value) => format!(
            "{{ key: {}, value: {{ i64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U32(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U64(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::F32(value) => format!(
            "{{ key: {}, value: {{ f64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::F64(value) => format!(
            "{{ key: {}, value: {{ f64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::Bool(value) => format!(
            "{{ key: {}, value: {{ bool: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|p| inner_collection(&p)).collect();
            format!(
                "{{ key: {}, value: {{ list: [{}] }} }}",
                serde_json::to_string(key).unwrap(),
                vec.join(", ")
            )
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{{ key: {}, value: {} }}",
                        serde_json::to_string(k).unwrap(),
                        inner_collection(v)
                    )
                })
                .collect();
            format!(
                "{{ key: {}, value: {{ object: [{}] }} }}",
                serde_json::to_string(key).unwrap(),
                properties_array.join(", ")
            )
        }
        Prop::DTime(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::NDTime(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::Decimal(value) => format!(
            "{{ key: {}, value: {{ decimal: \"{}\" }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
    }
}

pub(crate) fn build_property_string(properties: HashMap<String, Prop>) -> String {
    let properties_array: Vec<String> = properties
        .iter()
        .map(|(k, v)| to_graphql_valid(k, v))
        .collect();

    format!("[{}]", properties_array.join(", "))
}
