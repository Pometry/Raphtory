use crate::{plugin::server::EXTENSIONS, server::ServerError, GraphServer};
use clap::{ArgMatches, Command};
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

pub(crate) use crate::plugin::server::internal::ServerExtensionImpl;

pub type BoxedExtension = Box<dyn ServerExtensionImpl>;
pub type ExtensionRef<'a> = &'a dyn ServerExtensionImpl;

pub trait ServerExtension: Debug + Send + Sync + 'static {
    /// name of the extension
    fn name(&self) -> &str;

    /// hook that gets called on the parsed arguments during server creation
    fn apply(&self, server: GraphServer) -> Result<GraphServer, ServerError>;
}

#[derive(Debug, Default)]
pub struct ArgExtensions(IndexMap<String, BoxedExtension>);

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
                    let guard = EXTENSIONS.lock().expect("extensions lock poisoned");
                    let plugin_factory = guard
                        .get(key)
                        .ok_or_else(|| de::Error::custom(format!("unknown plugin {key}")))?;
                    let plugin = plugin_factory.from_json(value).map_err(de::Error::custom)?;
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
}

impl clap::FromArgMatches for ArgExtensions {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        Ok(ArgExtensions(
            EXTENSIONS
                .lock()
                .expect("plugin lock poisoned")
                .iter()
                .map(|(name, ext)| {
                    let mut plugin = ext.new_boxed_args();
                    plugin.dyn_update_from_arg_matches(matches)?;
                    Ok::<_, clap::Error>((name.clone(), plugin))
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
        for plugin in EXTENSIONS.lock().expect("plugin lock poisoned").values() {
            cmd = plugin.augment_args(cmd);
        }
        cmd
    }

    fn augment_args_for_update(mut cmd: Command) -> Command {
        for plugin in EXTENSIONS.lock().expect("plugin lock poisoned").values() {
            cmd = plugin.augment_args_for_update(cmd);
        }
        cmd
    }
}
