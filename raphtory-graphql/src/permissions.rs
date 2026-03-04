use serde::Deserialize;
use std::{collections::HashMap, fs, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PermissionsError {
    #[error("Failed to read permissions store file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse permissions store file: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Read-only (`ro`) or read-write (`rw`) access level.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ContentAccess {
    Ro,
    Rw,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GraphPermissions {
    /// Graph name this entry applies to. Use `"*"` to match all graphs.
    pub name: String,
    /// Access level for nodes. Absent means denied.
    pub nodes: Option<ContentAccess>,
    /// Access level for edges. Absent means denied.
    pub edges: Option<ContentAccess>,
    /// Per-graph introspection override (counts, uniqueLayers, schema stats).
    /// When absent, falls back to the role-level `introspection` setting.
    pub introspection: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RolePermissions {
    pub graphs: Vec<GraphPermissions>,
    /// Whether this role may perform GraphQL schema introspection (`__schema` / `__type`).
    /// Defaults to `false` — deny introspection unless explicitly granted.
    #[serde(default)]
    pub introspection: bool,
}

#[derive(Debug, Deserialize)]
pub struct PermissionsStore {
    pub roles: HashMap<String, RolePermissions>,
}

impl PermissionsStore {
    pub fn load(path: &Path) -> Result<Self, PermissionsError> {
        let content = fs::read_to_string(path)?;
        let store = serde_json::from_str(&content)?;
        Ok(store)
    }

    /// Returns the matching `GraphPermissions` entry for the given role and graph name,
    /// or `None` if the role has no entry covering that graph.
    /// Wildcard entry (`name: "*"`) matches any graph but a specific entry takes precedence.
    pub fn get_graph_permissions<'a>(
        &'a self,
        role: &str,
        graph: &str,
    ) -> Option<&'a GraphPermissions> {
        let role_perms = self.roles.get(role)?;
        let specific = role_perms.graphs.iter().find(|g| g.name == graph);
        if specific.is_some() {
            return specific;
        }
        role_perms.graphs.iter().find(|g| g.name == "*")
    }

    /// Returns whether the given role is allowed to perform GraphQL schema introspection
    /// (`__schema` / `__type`). Uses the role-level setting only (schema is not graph-specific).
    /// Defaults to `false` when the role is not found.
    pub fn is_introspection_allowed(&self, role: &str) -> bool {
        self.roles
            .get(role)
            .map(|r| r.introspection)
            .unwrap_or(false)
    }

    /// Returns whether the given role is allowed to perform introspection on a specific graph
    /// (e.g. `countNodes`, `countEdges`, `uniqueLayers`).
    /// The per-graph `introspection` field takes precedence; falls back to the role-level setting.
    pub fn is_graph_introspection_allowed(&self, role: &str, graph: &str) -> bool {
        let role_perms = match self.roles.get(role) {
            Some(r) => r,
            None => return false,
        };
        if let Some(graph_perms) = self.get_graph_permissions(role, graph) {
            if let Some(override_val) = graph_perms.introspection {
                return override_val;
            }
        }
        role_perms.introspection
    }
}
