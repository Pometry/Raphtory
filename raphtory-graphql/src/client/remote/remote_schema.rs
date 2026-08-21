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
    transport::{prop_list, prop_map_get, prop_str},
    ClientError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropMap, PropType};

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
    pub(crate) fn from_prop(prop: Prop) -> Result<Self, ClientError> {
        let map = expect_map(prop, "schema")?;
        Ok(Self {
            nodes: prop_list(prop_map_get(&map, "nodes")?, "schema.nodes")?
                .into_iter()
                .map(RemoteNodeSchema::from_prop)
                .collect::<Result<_, _>>()?,
            layers: prop_list(prop_map_get(&map, "layers")?, "schema.layers")?
                .into_iter()
                .map(RemoteLayerSchema::from_prop)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RemoteNodeSchema {
    fn from_prop(prop: Prop) -> Result<Self, ClientError> {
        let map = expect_map(prop, "nodeSchema")?;
        Ok(Self {
            type_name: prop_str(prop_map_get(&map, "typeName")?, "nodeSchema.typeName")?,
            properties: decode_property_schemas(prop_map_get(&map, "properties")?)?,
            metadata: decode_property_schemas(prop_map_get(&map, "metadata")?)?,
        })
    }
}

impl RemoteLayerSchema {
    fn from_prop(prop: Prop) -> Result<Self, ClientError> {
        let map = expect_map(prop, "layerSchema")?;
        Ok(Self {
            name: prop_str(prop_map_get(&map, "name")?, "layerSchema.name")?,
            edges: prop_list(prop_map_get(&map, "edges")?, "layerSchema.edges")?
                .into_iter()
                .map(RemoteEdgeSchema::from_prop)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl RemoteEdgeSchema {
    fn from_prop(prop: Prop) -> Result<Self, ClientError> {
        let map = expect_map(prop, "edgeSchema")?;
        Ok(Self {
            src_type: prop_str(prop_map_get(&map, "srcType")?, "edgeSchema.srcType")?,
            dst_type: prop_str(prop_map_get(&map, "dstType")?, "edgeSchema.dstType")?,
            properties: decode_property_schemas(prop_map_get(&map, "properties")?)?,
            metadata: decode_property_schemas(prop_map_get(&map, "metadata")?)?,
        })
    }
}

impl RemotePropertySchema {
    fn from_prop(prop: Prop) -> Result<Self, ClientError> {
        let map = expect_map(prop, "propertySchema")?;
        Ok(Self {
            key: prop_str(prop_map_get(&map, "key")?, "propertySchema.key")?,
            property_type: {
                let json = prop_str(prop_map_get(&map, "dtype")?, "propertySchema.dtype")?;
                serde_json::from_str(&json).map_err(|e| {
                    ClientError::InvalidResponse(format!("propertySchema.dtype: {e}"))
                })?
            },
            variants: prop_list(prop_map_get(&map, "variants")?, "propertySchema.variants")?
                .into_iter()
                .map(|p| prop_str(p, "propertySchema.variants[]"))
                .collect::<Result<_, _>>()?,
        })
    }
}

fn decode_property_schemas(prop: Prop) -> Result<Vec<RemotePropertySchema>, ClientError> {
    prop_list(prop, "propertySchemas")?
        .into_iter()
        .map(RemotePropertySchema::from_prop)
        .collect()
}

// ============ Prop tree helpers ============

fn expect_map(prop: Prop, context: &str) -> Result<std::sync::Arc<PropMap>, ClientError> {
    match prop {
        Prop::Map(m) => Ok(m),
        _ => Err(ClientError::InvalidResponse(format!(
            "`{}` expected Prop::Map",
            context
        ))),
    }
}
