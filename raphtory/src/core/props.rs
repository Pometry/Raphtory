use crate::core::lazy_vec::IllegalSet;
use crate::core::Prop;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("cannot mutate static property '{name}'")]
pub struct IllegalMutate {
    pub name: String,
    pub source: IllegalSet<Option<Prop>>,
}

impl IllegalMutate {
    pub(crate) fn from_source(source: IllegalSet<Option<Prop>>, prop: &str) -> IllegalMutate {
        IllegalMutate {
            name: prop.to_string(),
            source,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum PropId {
    Static(usize),
    Temporal(usize),
}
