use async_graphql::{Error, Value as GqlValue};
use dynamic_graphql::{Scalar, ScalarValue};
use raphtory::core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use raphtory_api::core::entities::GID;
use serde::{Deserialize, Serialize};
use serde_json::Number;

/// Identifier for a node — either a string (`"alice"`) or a non-negative
/// integer (`42`). Use whichever form matches how the graph was indexed
/// when nodes were added.
#[derive(Scalar, Clone, Debug, Serialize, Deserialize)]
#[graphql(name = "NodeId")]
pub struct GqlNodeId(pub GID);

impl ScalarValue for GqlNodeId {
    fn from_value(value: GqlValue) -> Result<Self, Error> {
        match value {
            GqlValue::String(s) => Ok(GqlNodeId(GID::Str(s))),
            GqlValue::Number(n) => n
                .as_u64()
                .map(|u| GqlNodeId(GID::U64(u)))
                .ok_or_else(|| Error::new("NodeId integer must be a non-negative Int.")),
            _ => Err(Error::new(
                "Expected NodeId as a String or non-negative Int.",
            )),
        }
    }

    fn to_value(&self) -> GqlValue {
        match &self.0 {
            GID::Str(s) => GqlValue::String(s.clone()),
            GID::U64(u) => GqlValue::Number(Number::from(*u)),
        }
    }
}

impl From<GqlNodeId> for GID {
    fn from(value: GqlNodeId) -> GID {
        value.0
    }
}

impl From<&str> for GqlNodeId {
    fn from(value: &str) -> Self {
        GqlNodeId(GID::Str(value.to_owned()))
    }
}

impl From<String> for GqlNodeId {
    fn from(value: String) -> Self {
        GqlNodeId(GID::Str(value))
    }
}

impl From<u64> for GqlNodeId {
    fn from(value: u64) -> Self {
        GqlNodeId(GID::U64(value))
    }
}

impl AsNodeRef for GqlNodeId {
    fn as_node_ref(&self) -> NodeRef<'_> {
        self.0.as_node_ref()
    }
}

impl GqlNodeId {
    /// Returns the id as a `String`. Integer ids are formatted as decimal.
    /// Useful for callers that need a string id.
    pub fn to_string(&self) -> String {
        match &self.0 {
            GID::Str(s) => s.clone(),
            GID::U64(u) => u.to_string(),
        }
    }
}
