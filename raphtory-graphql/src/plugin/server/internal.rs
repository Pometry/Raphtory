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

    /// convert to json value for serialization
    fn to_json(&self) -> Result<Value, ServerError>;
}

impl<'de, T: ServerExtension + FromArgMatches + Clone + Serialize + Deserialize<'de>>
    ServerExtensionImpl for T
{
    fn dyn_update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        self.update_from_arg_matches(matches)
    }

    fn boxed_clone(&self) -> Box<dyn ServerExtensionImpl> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<Value, ServerError> {
        let value =
            serde_json::to_value(self).map_err(|err| ConfigError::Foreign(Box::new(err)))?;
        Ok(value)
    }
}

pub trait ServerPluginImpl: Send + Sync + 'static {
    fn new_boxed_args(&self) -> Box<dyn ServerExtensionImpl>;

    fn augment_args(&self, cmd: Command) -> Command;

    fn augment_args_for_update(&self, cmd: Command) -> Command;

    fn from_json(&self, value: Value) -> Result<Box<dyn ServerExtensionImpl>, ServerError>;
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

    fn from_json(&self, value: Value) -> Result<Box<dyn ServerExtensionImpl>, ServerError> {
        Ok(Box::new(
            T::Extension::deserialize(value).map_err(|err| ConfigError::Foreign(Box::new(err)))?,
        ))
    }
}
