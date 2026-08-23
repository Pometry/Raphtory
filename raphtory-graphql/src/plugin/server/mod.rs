//! Interface for defining server extensions which can define new command-line arguments and config
//!
//! The interface comes in two parts, the `ServerPlugin` trait which defines the constructor for the
//! `ServerExtension` which defines the hook that is called during server initialisation.
//!
//!

use crate::{
    plugin::server::{internal::ServerPluginImpl, plugin::PluginRegistration},
    server::ServerError,
};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use std::{
    iter::{IntoIterator, Iterator},
    ops::Deref,
    sync::Mutex,
};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum PluginRegistrationError {
    #[error("Multiple cli plugins registered with name '{0}'")]
    Multiple(String),
    /// Extension settings sit at the top level beside the built-in sections, and the built-ins are
    /// matched first — by `AppConfigFieldName` on the json path, and by serde's named fields before
    /// `flatten` on the file path. A colliding name would therefore never receive its config.
    #[error("Extension '{0}' collides with a built-in config section and would never be configured")]
    ShadowsConfigSection(String),
    #[error("No registered plugin with name '{0}'")]
    Unknown(String),
}

static EXTENSIONS: Lazy<IndexMap<String, Box<dyn ServerPluginImpl>>> = Lazy::new(|| {
    let mut map = IndexMap::new();
    for registration in inventory::iter::<PluginRegistration> {
        let plugin = registration.0();
        // Last registration wins on a name clash rather than panicking here — this runs inside a
        // `Lazy` and cannot report an error. `check_no_duplicate_plugins`, called at server start,
        // is where a clash is turned into a startup error.
        map.insert(plugin.name().to_string(), plugin);
    }
    map
});

/// Fail if two plugins registered under the same name.
///
/// A clash means one silently shadows the other in [`EXTENSIONS`], so its CLI flags and config
/// section would never take effect. It can only happen at build time (two `register_cli_plugin!`
/// invocations, or two linked crates, claiming one name), so this is a deterministic check run once
/// at startup rather than anything a user can trigger. Kept separate from the `Lazy` above, and
/// from the clap `augment_args` path, neither of which can return an error.
pub(crate) fn check_no_duplicate_plugins() -> Result<(), PluginRegistrationError> {
    let mut seen = std::collections::HashSet::new();
    for registration in inventory::iter::<PluginRegistration> {
        let name = registration.0().name().to_string();
        if !seen.insert(name.clone()) {
            return Err(PluginRegistrationError::Multiple(name));
        }
    }
    Ok(())
}

fn get_plugin(name: &str) -> Result<&dyn ServerPluginImpl, PluginRegistrationError> {
    EXTENSIONS
        .get(name)
        .map(|plugin| plugin.as_ref())
        .ok_or_else(|| PluginRegistrationError::Unknown(name.to_string()))
}

fn get_plugins() -> impl Iterator<Item = &'static dyn ServerPluginImpl> {
    EXTENSIONS.values().map(|ext| ext.as_ref())
}

/// Whether a plugin with this name was registered at compile time. Lets a build report which
/// optional extensions it was compiled with.
pub(crate) fn is_registered(name: &str) -> bool {
    EXTENSIONS.contains_key(name)
}

pub mod extension;
pub mod plugin;

mod internal;
