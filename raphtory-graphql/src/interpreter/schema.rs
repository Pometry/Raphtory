//! SDL-derived type map, used to **validate** a query and to drive planning.
//!
//! The locked decision was to keep [`schema.graphql`](../../schema.graphql)
//! authoritative. async-graphql's own validator (`check_rules`) is `pub(crate)`
//! and can't be called from here, so instead we parse the SDL into a
//! `type → field → return-type` map and validate the query against it during the
//! planning walk: a field that isn't defined on its parent type is rejected
//! before any execution. The same map provides each field's return type, which
//! tells the planner what type the field's children are selected on.

use async_graphql::parser::{
    parse_schema,
    types::{BaseType, TypeKind, TypeSystemDefinition},
};
use std::{collections::HashMap, sync::OnceLock};

/// The GraphQL schema the server exposes, embedded at build time.
const SCHEMA_SDL: &str = include_str!("../../schema.graphql");

/// What the SDL says about one field on an object type.
pub struct FieldInfo {
    /// The innermost named return type, e.g. `Node` for `list: [Node!]!`.
    pub return_type: String,
    /// Whether the field is nullable (outermost `!` absent).
    pub nullable: bool,
}

/// `type name → (field name → field info)` distilled from the SDL.
pub struct SchemaTypes {
    objects: HashMap<String, HashMap<String, FieldInfo>>,
}

impl SchemaTypes {
    /// Parse `schema.graphql` once and cache it for the process lifetime.
    pub fn get() -> &'static SchemaTypes {
        static TYPES: OnceLock<SchemaTypes> = OnceLock::new();
        TYPES.get_or_init(|| {
            SchemaTypes::parse(SCHEMA_SDL).expect("embedded schema.graphql must parse")
        })
    }

    fn parse(sdl: &str) -> Result<SchemaTypes, String> {
        let doc = parse_schema(sdl).map_err(|e| e.to_string())?;
        let mut objects = HashMap::new();
        for def in doc.definitions {
            let TypeSystemDefinition::Type(ty) = def else {
                continue;
            };
            let ty = ty.node;
            let TypeKind::Object(obj) = ty.kind else {
                continue;
            };
            let mut fields = HashMap::new();
            for f in obj.fields {
                let f = f.node;
                fields.insert(
                    f.name.node.to_string(),
                    FieldInfo {
                        return_type: base_type_name(&f.ty.node.base),
                        nullable: f.ty.node.nullable,
                    },
                );
            }
            objects.insert(ty.name.node.to_string(), fields);
        }
        Ok(SchemaTypes { objects })
    }

    /// Look up a field on a type. `None` means the field does not exist on that
    /// type in the schema — i.e. a validation failure.
    pub fn field(&self, type_name: &str, field: &str) -> Option<&FieldInfo> {
        self.objects.get(type_name)?.get(field)
    }
}

/// The innermost named type, unwrapping any `!` and `[]` wrappers.
fn base_type_name(base: &BaseType) -> String {
    match base {
        BaseType::Named(name) => name.to_string(),
        BaseType::List(inner) => base_type_name(&inner.base),
    }
}
