use crate::{
    error::StorageError,
    persist::config::{
        BaseConfig, CONFIG_FILE_NAME, ConfigOps, DEFAULT_MAX_PAGE_LEN_EDGES,
        DEFAULT_MAX_PAGE_LEN_NODES,
    },
};
use clap::{
    Args as ClapArgs, Command,
    error::{ContextKind, ContextValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{iter, path::Path};
use tempfile::NamedTempFile;
use tracing::error;

/// Trait for managing user-provided config arguments.
///
/// `Args` represent the input config values passed by the user through
/// the CLI, env vars, or a config file. Public-facing methods should always
/// use `Args` to accept config values from the user.
///
/// `Config` represents the final config values derived from `Args`, where fields
/// that are not set by the user are filled with default values. `Config` is then used
/// internally to configure the storage implementation.
pub trait ArgsOps: Serialize + DeserializeOwned + Sized + Clone + ClapArgs {
    type Config: ConfigOps + From<Self>;

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

    /// Update the config arguments stored in `dir` with the arguments in `self`.
    fn update_in_dir(self, dir: &Path) -> Result<Self, StorageError> {
        let mut args_in_dir = Self::load_from_dir(dir)?;

        args_in_dir.update(self);
        args_in_dir.save_to_dir(dir)?;

        Ok(args_in_dir)
    }

    /// Generate values for args from their environment variables.
    ///
    /// On invalid env values, logs the error and sets all fields to `None`.
    fn from_env() -> Self {
        let cm = Self::augment_args(Command::default().no_binary_name(true));

        // Try to parse arg values from environment variables.
        cm.clone()
            .try_get_matches_from(iter::empty::<String>())
            .and_then(|mut matches| Self::from_arg_matches_mut(&mut matches))
            .unwrap_or_else(|err| {
                error!(
                    "{}, ignoring environment variables.",
                    display_error(&err, &cm)
                );

                // On error return arg with all fields set to `None`.
                cm.mut_args(|arg| arg.env(None))
                    .try_get_matches_from(iter::empty::<String>())
                    .and_then(|mut matches| Self::from_arg_matches_mut(&mut matches))
                    .expect("Reading defaults without environment variables should not fail.")
            })
    }

    /// Update the config stored in `self` with the values in `new_args`.
    fn update(&mut self, new_args: Self);
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

#[derive(Debug, Clone, Serialize, Deserialize, ClapArgs)]
pub struct BaseArgs {
    #[arg(long, env = "RAPHTORY_MAX_NODE_PAGE_LEN")]
    max_node_page_len: Option<u32>,

    #[arg(long, env = "RAPHTORY_MAX_EDGE_PAGE_LEN")]
    max_edge_page_len: Option<u32>,
}

impl Default for BaseArgs {
    fn default() -> Self {
        Self::from_env()
    }
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
        Self::new(
            args.max_node_page_len.unwrap_or(DEFAULT_MAX_PAGE_LEN_NODES),
            args.max_edge_page_len.unwrap_or(DEFAULT_MAX_PAGE_LEN_EDGES),
        )
    }
}

impl From<BaseConfig> for BaseArgs {
    fn from(config: BaseConfig) -> Self {
        Self {
            max_node_page_len: Some(config.max_node_page_len()),
            max_edge_page_len: Some(config.max_edge_page_len()),
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
