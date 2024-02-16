use crate::core::{storage::locked_view::LockedView, ArcStr};
use chrono::{DateTime, NaiveDateTime, TimeZone};
use itertools::Itertools;
use std::{collections::HashMap, ops::Deref};

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

    pub fn add_field<V: Repr>(mut self, name: &str, value: V) -> Self {
        if self.has_fields {
            self.value.push_str(", ");
        } else {
            self.has_fields = true;
        }
        self.value.push_str(name);
        self.value.push('=');
        self.value.push_str(&value.repr());
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

impl<T: Repr> Repr for Vec<T> {
    fn repr(&self) -> String {
        let repr = self.iter().map(|v| v.repr()).join(", ");
        format!("[{}]", repr)
    }
}

impl<K: Repr, V: Repr> Repr for HashMap<K, V> {
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

impl<'a, T: Repr> Repr for LockedView<'a, T> {
    fn repr(&self) -> String {
        self.deref().repr()
    }
}

impl<'a, R: Repr> Repr for &'a R {
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
        assert_eq!((&v).repr(), "1")
    }

    #[test]
    fn test_string_ref() {
        let v = "test";

        assert_eq!(v.repr(), "test")
    }
}
