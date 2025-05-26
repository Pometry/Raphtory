use crate::core::entities::properties::prop::Prop;
use minijinja::Value;

// FIXME: this is eagerly allocating a lot of stuff, we should implement Object instead for Prop
impl From<Prop> for Value {
    fn from(value: Prop) -> Self {
        match value {
            Prop::Bool(value) => Value::from(value),
            Prop::F32(value) => Value::from(value),
            Prop::F64(value) => Value::from(value),
            Prop::I32(value) => Value::from(value),
            Prop::I64(value) => Value::from(value),
            Prop::U8(value) => Value::from(value),
            Prop::U16(value) => Value::from(value),
            Prop::U32(value) => Value::from(value),
            Prop::U64(value) => Value::from(value),
            Prop::Str(value) => Value::from(value.0.to_owned()),
            Prop::DTime(value) => Value::from(value.timestamp_millis()),
            Prop::NDTime(value) => Value::from(value.and_utc().timestamp_millis()),
            Prop::Array(value) => Value::from(value.to_vec_u8()),
            Prop::List(value) => value.iter().cloned().collect(),
            Prop::Map(value) => value
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            Prop::Decimal(value) => Value::from(value.to_string()),
        }
    }
}
