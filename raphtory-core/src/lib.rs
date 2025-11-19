//! # raphtory
//!
//! `raphtory` is the core module for the raphtory library.
//!
//! The raphtory library is a temporal graph analytics tool, which allows users to create
//! and analyze graph data with time.
//!
//! This crate provides the core data structures and functions for working with temporal graphs,
//! as well as building and evaluating algorithms.
//!
//! **Note** this module is not meant to be used as a standalone crate, but in conjunction with the
//! raphtory_db crate.
//!
//! For example code, please see the raphtory_db crate.
//!
//! ## Supported Platforms
//!
//! `raphtory` supports  support for the following platforms:
//!
//! **Note** they must have Rust 1.83 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!

use std::{thread, time::Duration};

use parking_lot::RwLock;

pub mod entities;
#[cfg(feature = "python")]
mod python;
pub mod storage;
pub mod utils;

pub(crate) fn loop_lock_write<A>(l: &RwLock<A>) -> parking_lot::RwLockWriteGuard<'_, A> {
    const MAX_BACKOFF_US: u64 = 1000; // 1ms max
    let mut backoff_us = 1;
    loop {
        if let Some(guard) = l.try_write_for(Duration::from_micros(50)) {
            return guard;
        }
        thread::park_timeout(Duration::from_micros(backoff_us));
        backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
    }
}
