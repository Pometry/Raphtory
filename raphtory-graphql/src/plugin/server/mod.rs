//! Interface for defining server extensions which can define new command-line arguments and config
//!
//! The interface comes in two parts, the `ServerPlugin` trait which defines the constructor for the
//! `ServerExtension` which defines the hook that is called during server initialisation.
//!
//!

use crate::plugin::server::internal::ServerPluginImpl;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use std::sync::Mutex;

static EXTENSIONS: Lazy<Mutex<IndexMap<String, Box<dyn ServerPluginImpl>>>> =
    Lazy::new(|| Mutex::new(IndexMap::new()));

pub mod extension;
pub mod plugin;

mod internal;
