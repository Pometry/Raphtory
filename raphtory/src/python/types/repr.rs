use crate::core::storage::locked_view::LockedView;
use chrono::NaiveDateTime;
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

impl Repr for &str {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for &NaiveDateTime {
    fn repr(&self) -> String {
        self.to_string()
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
