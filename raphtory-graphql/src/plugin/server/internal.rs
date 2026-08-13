//! Internal interface traits which define a dyn-compatible interface on top of Clap.

use crate::{
    plugin::server::{extension::ServerExtension, plugin::ServerPlugin},
    server::ServerError,
};
use clap::{ArgMatches, Command, FromArgMatches};
use config::ConfigError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Dynamic extension trait
pub trait ServerExtensionImpl: ServerExtension {
    /// handle parsing of arguments
    fn dyn_update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error>;

    /// implement clone for dynamic trait objects
    fn boxed_clone(&self) -> Box<dyn ServerExtensionImpl>;
}

impl<'de, T: ServerExtension + FromArgMatches + Clone> ServerExtensionImpl for T {
    fn dyn_update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        self.update_from_arg_matches(matches)
    }

    fn boxed_clone(&self) -> Box<dyn ServerExtensionImpl> {
        Box::new(self.clone())
    }
}

pub trait ServerPluginImpl: Send + Sync + 'static {
    fn new_boxed_args(&self) -> Box<dyn ServerExtensionImpl>;

    fn augment_args(&self, cmd: Command) -> Command;

    fn augment_args_for_update(&self, cmd: Command) -> Command;

    fn boxed_clone(&self) -> Box<dyn ServerPluginImpl>;

    fn name(&self) -> String {
        self.new_boxed_args().name().to_string()
    }
}

impl<T: ServerPlugin> ServerPluginImpl for T {
    fn new_boxed_args(&self) -> Box<dyn ServerExtensionImpl> {
        Box::new(self.new())
    }

    fn augment_args(&self, cmd: Command) -> Command {
        <T::Extension as clap::Args>::augment_args(cmd)
    }

    fn augment_args_for_update(&self, cmd: Command) -> Command {
        <T::Extension as clap::Args>::augment_args_for_update(cmd)
    }

    fn boxed_clone(&self) -> Box<dyn ServerPluginImpl> {
        Box::new(self.clone())
    }
}
