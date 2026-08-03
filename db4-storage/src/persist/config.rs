use crate::error::StorageError;
use clap::{
    Args as ClapArgs, Command,
    error::{ContextKind, ContextValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{iter, path::Path};
use tempfile::NamedTempFile;
use tracing::error;

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 600_000; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 6_000_000; // 2^20
pub const CONFIG_FILE_NAME: &str = "config.json";

/// Trait for managing user-provided config arguments.
pub trait ArgsOps: Serialize + DeserializeOwned + Sized + Clone {
    type Config: ConfigOps + From<Self>;

    /// Update the config arguments stored in `dir` with the arguments in `self`.
    fn update_in_dir(self, dir: &Path) -> Result<Self, StorageError> {
        let mut args_in_dir = Self::load_from_dir(dir)?;

        args_in_dir.update(self);
        args_in_dir.save_to_dir(dir)?;

        Ok(args_in_dir)
    }

    fn load_from_dir(dir: &Path) -> Result<Self, StorageError> {
        let config_file = dir.join(CONFIG_FILE_NAME);
        let config_file = std::fs::File::open(config_file)?;
        let config = serde_json::from_reader(config_file)?;
        Ok(config)
    }

    fn save_to_dir(&self, dir: &Path) -> Result<(), StorageError> {
        let config_path = dir.join(CONFIG_FILE_NAME);
        let mut tmp_file = NamedTempFile::new_in(dir)?;
        serde_json::to_writer_pretty(&mut tmp_file, self)?;
        tmp_file.as_file().sync_all()?;
        tmp_file
            .persist(&config_path)
            .map_err(std::io::Error::from)?;
        Ok(())
    }

    fn update(&mut self, new_args: Self);
}

/// Trait for graph storage configuration.
pub trait ConfigOps: Serialize + DeserializeOwned + ClapArgs + Sized {
    type Args: ArgsOps<Config = Self> + From<Self>;

    fn max_node_page_len(&self) -> u32;

    fn max_edge_page_len(&self) -> u32;

    fn node_types(&self) -> &[String];

    fn with_max_node_page_len(self, page_len: u32) -> Self;

    fn with_max_edge_page_len(self, page_len: u32) -> Self;

    fn with_node_types(&self, node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ClapArgs)]
#[serde(default)]
pub struct BaseConfig {
    #[arg(long, default_value_t=DEFAULT_MAX_PAGE_LEN_NODES, env="RAPHTORY_MAX_NODE_PAGE_LEN")]
    max_node_page_len: u32,

    #[arg(long, default_value_t=DEFAULT_MAX_PAGE_LEN_EDGES, env="RAPHTORY_MAX_EDGE_PAGE_LEN")]
    max_edge_page_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseArgs {
    max_node_page_len: Option<u32>,
    max_edge_page_len: Option<u32>,
}

impl BaseArgs {
    pub fn max_node_page_len(&self) -> Option<u32> {
        self.max_node_page_len
    }

    pub fn max_edge_page_len(&self) -> Option<u32> {
        self.max_edge_page_len
    }

    pub fn node_types(&self) -> &[String] {
        &[]
    }

    pub fn with_max_node_page_len(mut self, page_len: u32) -> Self {
        self.max_node_page_len = Some(page_len);
        self
    }

    pub fn with_max_edge_page_len(mut self, page_len: u32) -> Self {
        self.max_edge_page_len = Some(page_len);
        self
    }

    pub fn with_node_types(&self, _node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.clone()
    }
}

impl From<BaseArgs> for BaseConfig {
    fn from(args: BaseArgs) -> Self {
        let mut config = BaseConfig::default();
        if let Some(v) = args.max_node_page_len {
            config = config.with_max_node_page_len(v);
        }
        if let Some(v) = args.max_edge_page_len {
            config = config.with_max_edge_page_len(v);
        }
        config
    }
}

impl From<BaseConfig> for BaseArgs {
    fn from(config: BaseConfig) -> Self {
        Self {
            max_node_page_len: Some(config.max_node_page_len),
            max_edge_page_len: Some(config.max_edge_page_len),
        }
    }
}

impl ArgsOps for BaseArgs {
    type Config = BaseConfig;

    fn update(&mut self, new_args: Self) {
        if let Some(v) = new_args.max_node_page_len {
            self.max_node_page_len = Some(v);
        }
        if let Some(v) = new_args.max_edge_page_len {
            self.max_edge_page_len = Some(v);
        }
    }
}

pub trait ClapDefault: ClapArgs {
    fn clap_default() -> Self;
}

fn display_error(err: &clap::Error, cm: &Command) -> String {
    if let Some(ContextValue::String(variable)) = err.get(ContextKind::InvalidArg)
        && let Some(ContextValue::String(value)) = err.get(ContextKind::InvalidValue)
        && let Some(arg) = cm.get_arguments().find(|arg| {
            arg.get_long().is_some_and(|long| {
                variable.starts_with(&format!("--{long}"))
                    || arg
                        .get_short()
                        .is_some_and(|short| variable.starts_with(&format!("-{short}")))
            })
        })
        && let Some(env) = arg.get_env()
    {
        let id = arg.get_id();
        let env = env.display();
        return format!("Invalid value from environment for '{id}': '{env}={value}'");
    }
    err.to_string()
}

impl<T: ClapArgs + Default> ClapDefault for T {
    fn clap_default() -> Self {
        let cm = Self::augment_args(Command::default().no_binary_name(true));
        cm.clone()
            .try_get_matches_from(iter::empty::<String>())
            .and_then(|mut matches| Self::from_arg_matches_mut(&mut matches))
            .unwrap_or_else(|err| {
                error!(
                    "{}, ignoring environment variables.",
                    display_error(&err, &cm)
                );
                // unset environment variables and try again
                cm.mut_args(|arg| arg.env(None))
                    .try_get_matches_from(iter::empty::<String>())
                    .and_then(|mut matches| Self::from_arg_matches_mut(&mut matches))
                    .expect("Reading defaults without environment variables should not fail.")
            })
    }
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self::clap_default()
    }
}

impl BaseConfig {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self {
            max_node_page_len,
            max_edge_page_len,
        }
    }
}

impl ConfigOps for BaseConfig {
    type Args = BaseArgs;

    fn max_node_page_len(&self) -> u32 {
        self.max_node_page_len
    }

    fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    fn with_max_node_page_len(mut self, page_len: u32) -> Self {
        self.max_node_page_len = page_len;
        self
    }

    fn with_max_edge_page_len(mut self, page_len: u32) -> Self {
        self.max_edge_page_len = page_len;
        self
    }

    fn node_types(&self) -> &[String] {
        &[]
    }

    fn with_node_types(&self, _node_types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use crate::persist::config::{
        BaseConfig, DEFAULT_MAX_PAGE_LEN_EDGES, DEFAULT_MAX_PAGE_LEN_NODES,
    };

    #[test_log::test]
    fn test_default() {
        let default = BaseConfig::default();
        assert_eq!(default.max_edge_page_len, DEFAULT_MAX_PAGE_LEN_EDGES);
        assert_eq!(default.max_node_page_len, DEFAULT_MAX_PAGE_LEN_NODES);
    }

    #[test]
    fn test_deserialize() {
        let default: BaseConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(default.max_edge_page_len, DEFAULT_MAX_PAGE_LEN_EDGES);
        assert_eq!(default.max_node_page_len, DEFAULT_MAX_PAGE_LEN_NODES);
    }
}
