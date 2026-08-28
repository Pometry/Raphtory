use crate::plugin::server::{extension::ServerExtension, internal::ServerPluginImpl};
use inventory::collect;

/// re-export for use in macro
#[doc(hidden)]
pub use inventory::submit;

/// Interface for defining a command-line plugin. This only defines the constructor for the actual
/// plugin, the actual parsing is defined by `Self::Extension`.
pub trait ServerPlugin: Clone + Send + Sync + 'static {
    type Extension: ServerExtension + clap::Args + Clone;

    /// Create a new plugin instance

    fn new(&self) -> Self::Extension;
}

/// Type used for plugin registration. Use `register_cli_plugin` instead of constructing this type
#[doc(hidden)]
pub struct PluginRegistration(pub fn() -> Box<dyn ServerPluginImpl>);

collect!(PluginRegistration);

#[macro_export]
/// Register a command line interface plugin at compile time
///
/// The input for this plugin is a type that implements the `ServerPlugin` trait. This macro uses the
/// inventory and does
/// not run any code. It should be placed outside any function.
macro_rules! register_cli_plugin {
    ($plugin:tt) => {
        $crate::plugin::server::plugin::submit! {
            $crate::plugin::server::plugin::PluginRegistration(|| Box::new($plugin))
        }
    };
}
