use crate::{
    core::storage::locked_view::LockedView,
    db::api::state::{LazyNodeState, NodeState},
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps},
};
use chrono::{DateTime, NaiveDateTime, TimeZone};
use itertools::Itertools;
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};
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

impl Repr for GID {
    fn repr(&self) -> String {
        match self {
            GID::U64(v) => v.repr(),
            GID::I64(v) => v.repr(),
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

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        V: Repr + Clone + Send + Sync + 'graph,
    > Repr for LazyNodeState<'graph, V, G, GH>
{
    fn repr(&self) -> String {
        StructReprBuilder::new("LazyNodeState")
            .add_fields_from_iter(self.iter().map(|(n, v)| (n.name(), v)))
            .finish()
    }
}

impl<
        'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        V: Repr + Clone + Send + Sync + 'graph,
    > Repr for NodeState<'graph, V, G, GH>
{
    fn repr(&self) -> String {
        StructReprBuilder::new("NodeState")
            .add_fields_from_iter(self.iter().map(|(n, v)| (n.name(), v)))
            .finish()
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
