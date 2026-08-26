use serde::{Deserialize, Serialize};

/// Role-management settings. At most one of the source sub-tables (`ldap`/`opa`/`json`/`admin`) may
/// be set; each is a distinct role source. None set → RBAC is off. Disabled by default and inert in
/// the open-source build.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RbacConfig {
    /// Re-sync interval for a polled source, in seconds. Unset → load once at startup.
    pub poll_interval_secs: Option<u64>,
    /// Polled, read-only: roles from an LDAP/AD directory.
    pub ldap: Option<LdapSourceConfig>,
    /// Polled, read-only: roles from evaluating an OPA/Rego policy file.
    pub opa: Option<OpaSourceConfig>,
    /// Polled, read-only: roles from a JSON permissions-store file.
    pub json: Option<JsonSourceConfig>,
    /// Update-driven: roles managed by admin GraphQL commands, optionally seeded.
    pub admin: Option<AdminSourceConfig>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LdapSourceConfig {
    /// Directory URL, e.g. `ldap://dc:389` or `ldaps://…`.
    pub url: Option<String>,
    /// Bind (service-account) DN.
    pub bind_dn: Option<String>,
    /// Name of the environment variable holding the bind password. Takes precedence over
    /// `bind_password` when both are set.
    pub bind_password_env: Option<String>,
    /// Bind password given inline. Prefer `bind_password_env` to keep secrets out of the config
    /// file; this is provided for deployments that inject the config from a secret store.
    pub bind_password: Option<String>,
    /// Base DN for the role subtree search.
    pub group_base_dn: Option<String>,
    /// Filter selecting role entries (e.g. `(objectClass=group)`).
    pub group_filter: Option<String>,
    /// Attribute holding each role's grant spec as JSON. Defaults to `description`.
    pub permissions_attribute: Option<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpaSourceConfig {
    /// Path to the `.rego` policy file.
    pub path: Option<String>,
    /// Query producing the role map. Defaults to `data.raphtory.roles`.
    pub query: Option<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JsonSourceConfig {
    /// Path to the JSON permissions-store file to poll.
    pub path: Option<String>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminSourceConfig {
    /// Optional path to a permissions-store file to seed the store from on first startup.
    pub seed_path: Option<String>,
}
