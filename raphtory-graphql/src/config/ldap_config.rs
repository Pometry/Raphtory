use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Optional LDAP/Active Directory connection and schedule settings. Disabled by default.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LdapConfig {
    /// When false (the default), the section is inert regardless of other fields.
    pub enabled: bool,
    /// Directory URL, e.g. `ldap://dc.corp.example.com:389` or `ldaps://…`.
    pub url: Option<String>,
    /// Bind (service-account) DN used to search the directory.
    pub bind_dn: Option<String>,
    /// Name of the environment variable holding the bind password.
    pub bind_password_env: Option<String>,
    /// Base DN for the group subtree search.
    pub group_base_dn: Option<String>,
    /// LDAP filter selecting group objects (e.g. `(objectClass=group)`).
    pub group_filter: Option<String>,
    /// Path to the mapping file.
    pub mapping_path: Option<PathBuf>,
    /// Re-sync interval, in seconds.
    pub sync_interval_secs: Option<u64>,
}
