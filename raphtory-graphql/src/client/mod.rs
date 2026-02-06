//! Pure Rust GraphQL client for Raphtory GraphQL server.
//!
//! No Python dependency; usable from Rust and from PyO3 bindings.

mod error;
pub mod graphql_client;
mod queries;
pub mod remote_graph;

pub use error::ClientError;
pub use remote_graph::GraphQLRemoteGraph;

use crate::client::queries::GqlPropertyTypes;
use raphtory_api::core::entities::properties::prop::Prop;
use serde_json::{Number, Value as JsonValue};
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
        Prop::Str(value) => format!("{{ str: \"{}\" }}", value),
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
                .map(|(k, v)| format!("{{ key: \"{}\", value: {} }}", k, inner_collection(v)))
                .collect();
            format!("{{ object: [{}] }}", properties_array.join(", "))
        }
        Prop::DTime(value) => format!("{{ str: \"{}\" }}", value),
        Prop::NDTime(value) => format!("{{ str: \"{}\" }}", value),
        Prop::Decimal(value) => format!("{{ decimal: {} }}", value),
    }
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::U8(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::U16(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::I32(value) => format!("{{ key: \"{}\", value: {{ i64: {} }} }}", key, value),
        Prop::I64(value) => format!("{{ key: \"{}\", value: {{ i64: {} }} }}", key, value),
        Prop::U32(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::U64(value) => format!("{{ key: \"{}\", value: {{ u64: {} }} }}", key, value),
        Prop::F32(value) => format!("{{ key: \"{}\", value: {{ f64: {} }} }}", key, value),
        Prop::F64(value) => format!("{{ key: \"{}\", value: {{ f64: {} }} }}", key, value),
        Prop::Bool(value) => format!("{{ key: \"{}\", value: {{ bool: {} }} }}", key, value),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|p| inner_collection(&p)).collect();
            format!(
                "{{ key: \"{}\", value: {{ list: [{}] }} }}",
                key,
                vec.join(", ")
            )
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| format!("{{ key: \"{}\", value: {} }}", k, inner_collection(v)))
                .collect();
            format!(
                "{{ key: \"{}\", value: {{ object: [{}] }} }}",
                key,
                properties_array.join(", ")
            )
        }
        Prop::DTime(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::NDTime(value) => format!("{{ key: \"{}\", value: {{ str: \"{}\" }} }}", key, value),
        Prop::Decimal(value) => format!(
            "{{ key: \"{}\", value: {{ decimal: \"{}\" }} }}",
            key, value
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

fn single_prop_to_value<M: GqlPropertyTypes>(p: &Prop) -> M::PropertyValue {
    match p {
        Prop::U8(x) => M::v_u64(*x as i64),
        Prop::U16(x) => M::v_u64(*x as i64),
        Prop::U32(x) => M::v_u64(*x as i64),
        Prop::U64(x) => match i64::try_from(*x) {
            Ok(v) => M::v_u64(v),
            Err(_) => M::v_str(x.to_string()),
        },

        Prop::I32(x) => M::v_i64(*x as i64),
        Prop::I64(x) => M::v_i64(*x),

        Prop::F32(x) => {
            let f = *x as f64;
            if f.is_finite() {
                M::v_f64(f)
            } else {
                M::v_str(f.to_string())
            }
        }
        Prop::F64(x) => {
            if x.is_finite() {
                M::v_f64(*x)
            } else {
                M::v_str(x.to_string())
            }
        }

        Prop::Str(s) => M::v_str(s.to_string()),
        Prop::Bool(b) => M::v_bool(*b),

        Prop::List(arr) => {
            let inner = arr
                .iter()
                .map(|pp| single_prop_to_value::<M>(&pp))
                .collect();
            M::v_list(inner)
        }

        Prop::Map(m) => {
            let entries = m
                .iter()
                .map(|(k, v)| M::obj_entry(k.to_string(), single_prop_to_value::<M>(v)))
                .collect();
            M::v_object(entries)
        }

        Prop::NDTime(ndt) => M::v_str(ndt.to_string()),
        Prop::DTime(dt) => M::v_str(dt.to_string()),
        Prop::Decimal(bd) => M::v_str(bd.to_plain_string()),
    }
}

pub fn props_to_property_inputs<M>(props: &HashMap<String, Prop>) -> Vec<M::PropertyInput>
where
    M: GqlPropertyTypes,
{
    props
        .iter()
        .map(|(k, v)| M::prop_input(k.clone(), single_prop_to_value::<M>(v)))
        .collect()
}

fn single_prop_to_expected_json(p: &Prop) -> JsonValue {
    match p {
        Prop::List(arr) => JsonValue::Array(
            arr.iter()
                .map(|prop| single_prop_to_expected_json(&prop))
                .collect(),
        ),
        Prop::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in m.iter() {
                obj.insert(k.to_string(), single_prop_to_expected_json(v));
            }
            JsonValue::Object(obj)
        }
        Prop::NDTime(ndt) => JsonValue::String(ndt.to_string()),
        Prop::DTime(dt) => JsonValue::String(dt.to_string()),
        _ => serde_json::to_value(p)
            .unwrap()
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .to_owned(),
    }
}

pub fn props_to_json_values(props: &HashMap<String, Prop>) -> HashMap<String, JsonValue> {
    props
        .iter()
        .map(|(k, v)| (k.to_string(), single_prop_to_expected_json(v)))
        .collect()
}

pub fn json_eq_relaxed(a: &JsonValue, b: &JsonValue) -> bool {
    fn json_num_eq(a: &Number, b: &Number) -> bool {
        // Integers are always the same
        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai == bi;
        }
        if let (Some(au), Some(bu)) = (a.as_u64(), b.as_u64()) {
            return au == bu;
        }

        let af = a.as_f64().unwrap();
        let bf = b.as_f64().unwrap();

        if !af.is_finite() || !bf.is_finite() {
            return if af.is_nan() && bf.is_nan() {
                true
            } else if (af.is_infinite() && af.is_sign_positive())
                && (bf.is_infinite() && bf.is_sign_positive())
            {
                true
            } else if (af.is_infinite() && af.is_sign_negative())
                && (bf.is_infinite() && bf.is_sign_negative())
            {
                true
            } else {
                false
            };
        }

        // rounding errors on floats when serializing/deserializing to/from JSON, try to account for these
        let diff = (af - bf).abs();
        let scale = af.abs().max(bf.abs()).max(1.0);

        diff <= 1e-12 * scale || diff <= 1e-300
    }

    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(x), JsonValue::Bool(y)) => x == y,
        (JsonValue::String(x), JsonValue::String(y)) => x == y,
        (JsonValue::Number(x), JsonValue::Number(y)) => json_num_eq(x, y),
        (JsonValue::Array(xs), JsonValue::Array(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| json_eq_relaxed(x, y))
        }
        (JsonValue::Object(xm), JsonValue::Object(ym)) => {
            xm.len() == ym.len()
                && xm
                    .iter()
                    .all(|(k, xv)| ym.get(k).is_some_and(|yv| json_eq_relaxed(xv, yv)))
        }
        _ => false,
    }
}
