use crate::core::entities::properties::prop::{IntoPropMap, Prop};
use serde_json::Value;
use std::collections::HashMap;

impl TryFrom<Value> for Prop {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Null => Err("Null property not valid".to_string()),
            Value::Bool(value) => Ok(value.into()),
            // Preserve integer width: try i64, then u64 (values in i64::MAX+1..=u64::MAX stay
            // exact rather than dropping to f64), then f64.
            // Beyond u64::MAX there is no exact path here: serde_json (no arbitrary_precision) has
            // already parsed the number to f64, so the digits are gone before this runs. Preserving
            // those as Prop::Decimal needs arbitrary_precision enabled and the raw string parsed to
            // BigDecimal.
            Value::Number(value) => value
                .as_i64()
                .map(Prop::I64)
                .or_else(|| value.as_u64().map(Prop::U64))
                .or_else(|| value.as_f64().map(Prop::F64))
                .ok_or(format!("Number conversion error for: {}", value)),
            Value::String(value) => Ok(value.as_str().into()),
            Value::Array(value) => value
                .into_iter()
                .map(|item| item.try_into())
                .collect::<Result<Vec<Prop>, Self::Error>>()
                .map(|item| item.into()),
            Value::Object(value) => value
                .into_iter()
                .map(|(key, value)| {
                    let prop = value.try_into()?;
                    Ok((key, prop))
                })
                .collect::<Result<HashMap<String, Prop>, Self::Error>>()
                .map(|item| item.into_prop_map()),
        }
    }
}

impl From<Prop> for Value {
    fn from(prop: Prop) -> Self {
        match prop {
            Prop::Str(value) => Value::String(value.to_string()),
            Prop::U8(value) => Value::Number(value.into()),
            Prop::U16(value) => Value::Number(value.into()),
            Prop::I32(value) => Value::Number(value.into()),
            Prop::I64(value) => Value::Number(value.into()),
            Prop::U32(value) => Value::Number(value.into()),
            Prop::U64(value) => Value::Number(value.into()),
            Prop::F32(value) => serde_json::Number::from_f64(value as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Prop::F64(value) => serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Prop::Bool(value) => Value::Bool(value),
            Prop::List(values) => Value::Array(values.iter().map(Value::from).collect()),
            Prop::Map(map) => {
                let json_map: serde_json::Map<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), Value::from(v.clone())))
                    .collect();
                Value::Object(json_map)
            }
            Prop::NDTime(value) => Value::String(value.to_string()),
            Prop::DTime(value) => Value::String(value.to_string()),
            _ => Value::Null,
        }
    }
}
