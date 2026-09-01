pub(crate) use crate::plugin::server::internal::ServerExtensionImpl;
use crate::{
    plugin::server::{get_plugin, get_plugins},
    server::ServerError,
    GraphServer,
};
use clap::{ArgMatches, Command};
use config::ConfigError;
use indexmap::{map::IntoValues, IndexMap};
use serde::{
    de,
    de::{MapAccess, Visitor},
    ser,
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::Value;
use std::fmt::{Debug, Formatter};

pub type BoxedExtension = Box<dyn ServerExtensionImpl>;
pub type ExtensionRef<'a> = &'a dyn ServerExtensionImpl;

/// Interface for defining a server extension
pub trait ServerExtension: Debug + Send + Sync + 'static {
    /// name of the extension (used for registration/serialisation)
    ///
    /// Must be unique across registered plugins: two sharing a name are refused at server start,
    /// since one would otherwise silently shadow the other.
    fn name(&self) -> &str;

    /// hook that gets called on the parsed arguments during server creation
    fn apply(&self, server: GraphServer) -> Result<GraphServer, ServerError>;

    /// hook that gets called when deserialising config
    ///
    /// When parsing command-line arguments with the `--config-file` option specified, this hook is
    /// called on the extension after parsing the command-line arguments. The extension needs to
    /// handle the precedence of arguments accordingly!
    fn update_from_json(&mut self, value: &Value) -> Result<(), ServerError>;

    /// serialise config to json
    fn to_json(&self) -> Result<Value, ServerError>;

    /// serialise the extension's full config schema: every field it accepts, including ones that
    /// are unset by default, so a config-completeness check can enumerate them all. Defaults to
    /// [`to_json`](Self::to_json), which is exhaustive already when nothing is skipped when unset.
    fn config_schema(&self) -> Result<Value, ServerError> {
        self.to_json()
    }
}

#[derive(Debug)]
pub struct ArgExtensions(IndexMap<String, BoxedExtension>);

impl Default for ArgExtensions {
    /// One default instance of every registered extension. The command-line path builds exactly
    /// this (clap constructs each plugin's args whether or not a flag was given), so the config
    /// file path must start here too — otherwise an extension that is meant to configure itself
    /// from elsewhere is simply never asked, purely because the file has no block naming it.
    fn default() -> Self {
        let mut exts = ArgExtensions(IndexMap::new());
        for plugin in get_plugins() {
            exts.push_boxed(plugin.new_boxed_args());
        }
        exts
    }
}

impl IntoIterator for ArgExtensions {
    type Item = BoxedExtension;
    type IntoIter = IntoValues<String, BoxedExtension>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_values()
    }
}

impl Serialize for ArgExtensions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer
            .serialize_map(Some(self.0.len()))
            .map_err(ser::Error::custom)?;
        for ext in self.iter() {
            map.serialize_entry(ext.name(), &ext.to_json().map_err(ser::Error::custom)?)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ArgExtensions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor;

        impl<'de> Visitor<'de> for MapVisitor {
            type Value = ArgExtensions;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                write!(formatter, "a map of name and extension config")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut exts = ArgExtensions(IndexMap::new());
                while let Some((key, value)) = map.next_entry::<&str, Value>()? {
                    let plugin_factory = get_plugin(key).map_err(de::Error::custom)?;
                    let mut plugin = plugin_factory.new_boxed_args();
                    plugin.update_from_json(&value).map_err(de::Error::custom)?;
                    exts.push_boxed(plugin);
                }
                Ok(exts)
            }
        }

        deserializer.deserialize_map(MapVisitor)
    }
}

impl Clone for ArgExtensions {
    fn clone(&self) -> Self {
        ArgExtensions(
            self.0
                .iter()
                .map(|(key, extension)| (key.clone(), extension.boxed_clone()))
                .collect(),
        )
    }
}

impl PartialEq for ArgExtensions {
    fn eq(&self, other: &Self) -> bool {
        if let Ok(this_value) = serde_json::to_value(self) {
            if let Ok(other_value) = serde_json::to_value(other) {
                return this_value == other_value;
            }
        }
        false
    }
}

impl ArgExtensions {
    pub fn process(&self, mut server: GraphServer) -> Result<GraphServer, ServerError> {
        for plugin in self.iter() {
            server = plugin.apply(server)?;
        }
        Ok(server)
    }

    pub fn push(&mut self, extension: impl ServerExtensionImpl) {
        self.push_boxed(Box::new(extension))
    }

    pub fn push_boxed(&mut self, extension: BoxedExtension) {
        self.0.insert(extension.name().to_string(), extension);
    }

    pub fn iter(&self) -> impl Iterator<Item = ExtensionRef<'_>> {
        self.0.values().map(|ext| ext.as_ref())
    }

    /// update or insert extensions from json map
    pub fn update_from_json(&mut self, value: &Value) -> Result<(), ServerError> {
        match value {
            Value::Object(map) => {
                for (name, value) in map {
                    match self.0.get_mut(name) {
                        None => {
                            let mut ext = get_plugin(name)?.new_boxed_args();
                            ext.update_from_json(value)?;
                            self.push_boxed(ext);
                        }
                        Some(ext) => {
                            ext.update_from_json(value)?;
                        }
                    }
                }
            }
            _ => {
                Err(ConfigError::Message(
                    "expected a map for extensions".to_string(),
                ))?;
            }
        }
        Ok(())
    }
}

impl clap::FromArgMatches for ArgExtensions {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        Ok(ArgExtensions(
            get_plugins()
                .map(|ext| {
                    let mut plugin = ext.new_boxed_args();
                    plugin.dyn_update_from_arg_matches(matches)?;
                    Ok::<_, clap::Error>((plugin.name().to_string(), plugin))
                })
                .collect::<Result<_, clap::Error>>()?,
        ))
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        for plugin in self.0.values_mut() {
            plugin.dyn_update_from_arg_matches(matches)?;
        }
        Ok(())
    }
}

impl clap::Args for ArgExtensions {
    fn augment_args(mut cmd: Command) -> Command {
        for plugin in get_plugins() {
            cmd = plugin.augment_args(cmd);
        }
        cmd
    }

    fn augment_args_for_update(mut cmd: Command) -> Command {
        for plugin in get_plugins() {
            cmd = plugin.augment_args_for_update(cmd);
        }
        cmd
    }
}
