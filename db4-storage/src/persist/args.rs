use crate::{
    error::StorageError,
    persist::config::{
        BaseConfig, ConfigOps, DEFAULT_MAX_PAGE_LEN_EDGES, DEFAULT_MAX_PAGE_LEN_NODES,
    },
};
use clap::{
    Args as ClapArgs, Command,
    error::{ContextKind, ContextValue},
};
use serde::Deserialize;
use std::iter;
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
pub trait ArgsOps: Sized + Clone + ClapArgs {
    type Config: ConfigOps<Args = Self> + From<Self>;

    /// Merge the `Some` values in `new_args` into `self`.
    ///
    /// Fields that are `None` in `new_args` are ignored.
    fn merge(&mut self, new_args: Self) -> Result<(), StorageError>;

    /// Apply `self` as overrides on top of the existing `config` and return the result.
    fn apply_to_config(self, config: Self::Config) -> Result<Self::Config, StorageError>;
}

/// Generate values for clap [`Args`](ClapArgs) from their environment variables.
///
/// On invalid env values, logs the error and sets all fields to `None`.
pub fn clap_args_from_env<T: ClapArgs>() -> T {
    let cm = T::augment_args(Command::default().no_binary_name(true));

    // Try to parse arg values from environment variables.
    cm.clone()
        .try_get_matches_from(iter::empty::<String>())
        .and_then(|mut matches| T::from_arg_matches_mut(&mut matches))
        .unwrap_or_else(|err| {
            error!(
                "{}, ignoring environment variables.",
                display_error(&err, &cm)
            );

            // On error return arg with all fields set to `None`.
            cm.mut_args(|arg| arg.env(None))
                .try_get_matches_from(iter::empty::<String>())
                .and_then(|mut matches| T::from_arg_matches_mut(&mut matches))
                .expect("Reading defaults without environment variables should not fail.")
        })
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

#[derive(Debug, Clone, Deserialize, ClapArgs)]
#[serde(default)]
pub struct BaseArgs {
    #[arg(long, env = "RAPHTORY_MAX_NODE_PAGE_LEN")]
    max_node_page_len: Option<u32>,

    #[arg(long, env = "RAPHTORY_MAX_EDGE_PAGE_LEN")]
    max_edge_page_len: Option<u32>,
}

impl Default for BaseArgs {
    fn default() -> Self {
        // Use values from env if present.
        clap_args_from_env()
    }
}

impl BaseArgs {
    pub fn max_node_page_len(&self) -> Option<u32> {
        self.max_node_page_len
    }

    pub fn max_edge_page_len(&self) -> Option<u32> {
        self.max_edge_page_len
    }

    pub fn with_max_node_page_len(mut self, page_len: u32) -> Self {
        self.max_node_page_len = Some(page_len);
        self
    }

    pub fn with_max_edge_page_len(mut self, page_len: u32) -> Self {
        self.max_edge_page_len = Some(page_len);
        self
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

    fn merge(&mut self, new_args: Self) -> Result<(), StorageError> {
        if let Some(v) = new_args.max_node_page_len {
            self.max_node_page_len = Some(v);
        }
        if let Some(v) = new_args.max_edge_page_len {
            self.max_edge_page_len = Some(v);
        }

        Ok(())
    }

    fn apply_to_config(self, config: Self::Config) -> Result<Self::Config, StorageError> {
        if self.max_node_page_len.is_some() || self.max_edge_page_len.is_some() {
            return Err(StorageError::GenericFailure(
                "Page sizes cannot be overridden after graph creation".to_string(),
            ));
        }

        let mut args = Self::from(config);
        args.merge(self)?;
        let new_config: BaseConfig = args.into();
        Ok(new_config)
    }
}
