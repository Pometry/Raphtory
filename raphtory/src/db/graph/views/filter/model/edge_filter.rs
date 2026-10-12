use crate::db::api::view::internal::Static;
use serde::{Deserialize, Serialize};

// User facing entry for building edge filters.
#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct EdgeFilter;

impl Static for EdgeFilter {}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    Src,
    Dst,
}
