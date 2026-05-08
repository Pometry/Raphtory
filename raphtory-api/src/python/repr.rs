use crate::core::{
    entities::{
        properties::prop::{Prop, PropArray, PropUntagged},
        GID,
    },
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, EventTime},
    },
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, TimeZone};
use indexmap::IndexMap;
use itertools::Itertools;
use pyo3::{prelude::PyAnyMethods, Bound, Py, PyAny, Python};
use std::{collections::HashMap, error::Error, ops::Deref, sync::Arc};

pub fn iterator_repr<I: Iterator<Item = V>, V: Repr>(iter: I) -> String {
    let values: Vec<String> = iter.take(11).map(|v| v.repr()).collect();
    if values.len() < 11 {
        values.join(", ")
    } else {
        values[0..10].join(", ") + ", ..."
    }
}

pub fn iterator_dict_repr<I: Iterator<Item = (K, V)>, K: Repr, V: Repr>(iter: I) -> String {
    let values: Vec<String> = iter
        .take(11)
        .map(|(k, v)| format!("{}: {}", k.repr(), v.repr()))
        .collect();
    if values.len() < 11 {
        values.join(", ")
    } else {
        values[0..10].join(", ") + ", ..."
    }
}

pub struct StructReprBuilder {
    value: String,
    has_fields: bool,
}

impl StructReprBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            value: name.to_owned() + "(",
            has_fields: false,
        }
    }

    fn add_field_sep(&mut self) {
        if self.has_fields {
            self.value.push_str(", ");
        } else {
            self.has_fields = true;
        }
    }

    pub fn add_field<V: Repr>(mut self, name: &str, value: V) -> Self {
        self.add_field_sep();
        self.value.push_str(name);
        self.value.push('=');
        self.value.push_str(&value.repr());
        self
    }

    pub fn add_fields_from_iter<I: Iterator<Item = (K, V)>, K: Repr, V: Repr>(
        mut self,
        iter: I,
    ) -> Self {
        self.add_field_sep();
        self.value.push_str(&iterator_dict_repr(iter));
        self
    }

    pub fn finish(self) -> String {
        let mut value = self.value;
        value.push(')');
        value
    }
}

pub trait Repr {
    fn repr(&self) -> String;
}

impl Repr for PropUntagged {
    fn repr(&self) -> String {
        self.0.repr()
    }
}

impl Repr for Prop {
    fn repr(&self) -> String {
        match &self {
            Prop::Str(v) => v.repr(),
            Prop::Bool(v) => v.repr(),
            Prop::I64(v) => v.repr(),
            Prop::U8(v) => v.repr(),
            Prop::U16(v) => v.repr(),
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::NDTime(v) => v.repr(),
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
            Prop::Decimal(v) => v.repr(),
        }
    }
}

impl<T: Repr, const N: usize> Repr for [T; N] {
    fn repr(&self) -> String {
        self.as_slice().repr()
    }
}

impl Repr for Py<PyAny> {
    fn repr(&self) -> String {
        Python::attach(|py| Repr::repr(self.bind(py)))
    }
}

impl<'py> Repr for Bound<'py, PyAny> {
    fn repr(&self) -> String {
        let repr = PyAnyMethods::repr(self);
        match repr {
            Ok(repr) => repr.to_string(),
            Err(_) => "<unknown>".to_string(),
        }
    }
}

impl Repr for GID {
    fn repr(&self) -> String {
        match self {
            GID::U64(v) => v.repr(),
            GID::Str(v) => v.repr(),
        }
    }
}

impl Repr for bool {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for u32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for u8 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for u16 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for u64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for i32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for i64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for usize {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for f32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for f64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for String {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for ArcStr {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for &str {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for NaiveDateTime {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for EventTime {
    fn repr(&self) -> String {
        let mut builder = StructReprBuilder::new("EventTime").add_field("t", self.t());
        if let Ok(dt) = self.dt() {
            builder = builder.add_field("dt", dt)
        }
        builder.add_field("event_id", self.i()).finish()
    }
}

impl Repr for BigDecimal {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl<T: TimeZone> Repr for DateTime<T> {
    fn repr(&self) -> String {
        self.to_rfc2822()
    }
}

impl<T: Repr> Repr for Option<T> {
    fn repr(&self) -> String {
        match &self {
            Some(v) => v.repr(),
            None => "None".to_string(),
        }
    }
}

impl<T: Repr, E: Error> Repr for Result<T, E> {
    fn repr(&self) -> String {
        match self {
            Ok(v) => v.repr(),
            Err(e) => e.to_string(),
        }
    }
}

impl<T: Repr> Repr for &[T] {
    fn repr(&self) -> String {
        let repr = self.iter().map(|v| v.repr()).join(", ");
        format!("[{}]", repr)
    }
}

impl<T: Repr> Repr for Vec<T> {
    fn repr(&self) -> String {
        self.deref().repr()
    }
}

impl Repr for PropArray {
    fn repr(&self) -> String {
        let repr = self.iter().map(|v| v.repr()).join(", ");
        format!("[{}]", repr)
    }
}

impl<T: Repr> Repr for Arc<[T]> {
    fn repr(&self) -> String {
        self.deref().repr()
    }
}

impl<K: Repr, V: Repr, S> Repr for HashMap<K, V, S> {
    fn repr(&self) -> String {
        let repr = self
            .iter()
            .map(|(k, v)| format!("{}: {}", k.repr(), v.repr()))
            .join(", ");
        format!("{{{}}}", repr)
    }
}

impl<K: Repr, V: Repr, S> Repr for IndexMap<K, V, S> {
    fn repr(&self) -> String {
        let repr = self
            .iter()
            .map(|(k, v)| format!("{}: {}", k.repr(), v.repr()))
            .join(", ");
        format!("{{{}}}", repr)
    }
}

impl<S: Repr, T: Repr> Repr for (S, T) {
    fn repr(&self) -> String {
        format!("({}, {})", self.0.repr(), self.1.repr())
    }
}

// three element tuple
impl<S: Repr, T: Repr, U: Repr> Repr for (S, T, U) {
    fn repr(&self) -> String {
        format!("({}, {}, {})", self.0.repr(), self.1.repr(), self.2.repr())
    }
}
impl<S: Repr, T: Repr, U: Repr, V: Repr> Repr for (S, T, U, V) {
    fn repr(&self) -> String {
        format!(
            "({}, {}, {}, {})",
            self.0.repr(),
            self.1.repr(),
            self.2.repr(),
            self.3.repr()
        )
    }
}

impl<R: Repr> Repr for &R {
    fn repr(&self) -> String {
        R::repr(self)
    }
}

#[cfg(test)]
mod repr_tests {
    use super::*;

    #[test]
    fn test_option_some() {
        let v = Some(1);

        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_option_none() {
        let v: Option<String> = None;
        assert_eq!(v.repr(), "None")
    }

    #[test]
    fn test_int() {
        let v = 1;
        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_int_ref() {
        let v = 1;
        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_string_ref() {
        let v = "test";

        assert_eq!(v.repr(), "test")
    }
}
