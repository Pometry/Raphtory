//! Client-side representation of the graph schema tree.
//!
//! Unlike other `Remote*` types (which are handles that fire RPCs per
//! terminal), the schema is materialized eagerly in one RPC. The whole
//! tree is small — realistic graphs have a bounded set of node types and
//! layers — and users always want the full descriptor at once.
//!
//! The five types here are pure data (no `expr` / `base_graph` fields, no
//! transport handle). They're built from a single response payload by
//! walking the nested `Prop::Map` / `Prop::List` tree that
//! `parse_read` decoded.

use crate::client::{
    transport::{expect_list, expect_prop_type, expect_string_list, expect_typed, extract_element},
    ClientError,
};
use raphtory_api::core::entities::properties::prop::PropType;
use serde_json::Map;

/// A single property schema entry — one key on a node/edge type, with its
/// observed property type and (for string-valued properties) the set of
/// distinct values seen.
#[derive(Clone, Debug, PartialEq)]
pub struct RemotePropertySchema {
    pub key: String,
    pub property_type: PropType,
    pub variants: Vec<String>,
}

/// Schema for edges between a specific `(src_type, dst_type)` pair within
/// one layer.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteEdgeSchema {
    pub src_type: String,
    pub dst_type: String,
    pub properties: Vec<RemotePropertySchema>,
    pub metadata: Vec<RemotePropertySchema>,
}

/// Schema for a single edge layer — its name and the per `(srcType, dstType)`
/// edge schemas observed within it.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteLayerSchema {
    pub name: String,
    pub edges: Vec<RemoteEdgeSchema>,
}

/// Schema for nodes of a specific type — its property and metadata keys
/// with their observed types.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteNodeSchema {
    pub type_name: String,
    pub properties: Vec<RemotePropertySchema>,
    pub metadata: Vec<RemotePropertySchema>,
}

/// The full schema of a remote graph — one entry per node type and one
/// per edge layer.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteGraphSchema {
    pub nodes: Vec<RemoteNodeSchema>,
    pub layers: Vec<RemoteLayerSchema>,
}

// ============ Decoding from Prop::Map tree ============

impl RemoteGraphSchema {
    /// Decode a `Prop`-shaped tree (produced by `parse_read` on the `Schema`
    /// terminal) into a typed schema tree.
    pub(crate) fn from_query(prop: serde_json::Value) -> Result<Self, ClientError> {
        let mut map = expect_map(prop, "schema")?;
        Ok(Self {
            nodes: expect_list(extract_element(&mut map, "nodes")?, "schema.nodes")?
                .into_iter()
                .map(RemoteNodeSchema::from_query)
                .collect::<Result<_, _>>()?,
            layers: expect_list(extract_element(&mut map, "layers")?, "schema.layers")?
                .into_iter()
                .map(RemoteLayerSchema::from_query)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RemoteNodeSchema {
    fn from_query(value: serde_json::Value) -> Result<Self, ClientError> {
        let mut map = expect_map(value, "nodeSchema")?;
        Ok(Self {
            type_name: expect_typed(
                extract_element(&mut map, "typeName")?,
                "nodeSchema.typeName",
            )?,
            properties: decode_property_schemas(extract_element(&mut map, "properties")?)?,
            metadata: decode_property_schemas(extract_element(&mut map, "metadata")?)?,
        })
    }
}

impl RemoteLayerSchema {
    fn from_query(value: serde_json::Value) -> Result<Self, ClientError> {
        let mut map = expect_map(value, "layerSchema")?;
        Ok(Self {
            name: expect_typed(extract_element(&mut map, "name")?, "layerSchema.name")?,
            edges: expect_list(extract_element(&mut map, "edges")?, "layerSchema.edges")?
                .into_iter()
                .map(RemoteEdgeSchema::from_query)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RemoteEdgeSchema {
    fn from_query(value: serde_json::Value) -> Result<Self, ClientError> {
        let mut map = expect_map(value, "edgeSchema")?;
        Ok(Self {
            src_type: expect_typed(extract_element(&mut map, "srcType")?, "edgeSchema.srcType")?,
            dst_type: expect_typed(extract_element(&mut map, "dstType")?, "edgeSchema.dstType")?,
            properties: decode_property_schemas(extract_element(&mut map, "properties")?)?,
            metadata: decode_property_schemas(extract_element(&mut map, "metadata")?)?,
        })
    }
}

impl RemotePropertySchema {
    fn from_query(result: serde_json::Value) -> Result<Self, ClientError> {
        let mut map = expect_map(result, "propertySchema")?;
        Ok(Self {
            key: expect_typed(extract_element(&mut map, "key")?, "propertySchema.key")?,
            property_type: expect_prop_type(
                extract_element(&mut map, "dtype")?,
                "propertySchema.dtype",
            )?,
            variants: expect_string_list(
                extract_element(&mut map, "variants")?,
                "propertySchema.variants",
            )?,
        })
    }
}

fn decode_property_schemas(
    value: serde_json::Value,
) -> Result<Vec<RemotePropertySchema>, ClientError> {
    expect_list(value, "propertySchemas")?
        .into_iter()
        .map(RemotePropertySchema::from_query)
        .collect()
}

// ============ Prop tree helpers ============

fn expect_map(
    result: serde_json::Value,
    context: &str,
) -> Result<Map<String, serde_json::Value>, ClientError> {
    match result {
        serde_json::Value::Object(m) => Ok(m),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` expected map",
            context
        ))),
    }
}
