use async_graphql::{Error, Value as GqlValue};
use dynamic_graphql::{Scalar, ScalarValue};
use raphtory::core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use raphtory_api::core::entities::GID;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Number;

/// Identifier for a node — either a string (`"alice"`) or a non-negative
/// integer (`42`). Use whichever form matches how the graph was indexed
/// when nodes were added.
// Its serde form is the scalar's: a string or an integer, as a client sends it
// in variables and a stored filter keeps it.
#[derive(Scalar, Clone, Debug)]
#[graphql(name = "NodeId")]
pub struct GqlNodeId(pub GID);

/// The scalar spelling of a node id.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum NodeIdWire {
    U64(u64),
    Str(String),
}

impl Serialize for GqlNodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            GID::U64(u) => NodeIdWire::U64(*u),
            GID::Str(s) => NodeIdWire::Str(s.clone()),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GqlNodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(GqlNodeId(match NodeIdWire::deserialize(deserializer)? {
            NodeIdWire::U64(u) => GID::U64(u),
            NodeIdWire::Str(s) => GID::Str(s),
        }))
    }
}

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
