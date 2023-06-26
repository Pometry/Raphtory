//! A data structure for sharding a temporal graph.
//!
//! When a raphtory graph is created, the code will automatically shard the graph depending
//! on how many shards you set the graph to originally have when initializing it.
//!
//! For example, Graph::new(4) will create a graph with 4 shards.
//!
//! Each of these shards will be stored in a separate file, and will be loaded into memory when needed.
//!
//! Each shard will have its own set of vertex and edge data, and will be able to be queried independently.

use self::errors::GraphError;
use std::{ops::Deref, hash::BuildHasherDefault};

use dashmap::mapref::one::Ref;

pub enum LockedView<'a, T> {
    Locked(parking_lot::MappedRwLockReadGuard<'a, T>),
    DashMap(Ref<'a, usize, T, BuildHasherDefault<rustc_hash::FxHasher>>)
}

impl<'a, T> Deref for LockedView<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            LockedView::Locked(guard) => guard.deref(),
            LockedView::DashMap(r) => (*r).deref(),
        }
    }
}


pub mod errors {
    use crate::core::tgraph::errors::MutateGraphError;
    use crate::core::time::error::ParseTimeError;

    #[derive(thiserror::Error, Debug)]
    pub enum GraphError {
        #[error("Immutable graph reference already exists. You can access mutable graph apis only exclusively.")]
        IllegalGraphAccess,
        #[error("Incorrect property given.")]
        IncorrectPropertyType,
        #[error("Failed to mutate graph")]
        FailedToMutateGraph { source: MutateGraphError },
        #[error("Failed to mutate graph property")]
        FailedToMutateGraphProperty { source: MutateGraphError },
        #[error("Failed to parse time string")]
        ParseTime {
            #[from]
            source: ParseTimeError,
        },
        // wasm
        #[error("Vertex is not String or Number")]
        VertexIdNotStringOrNumber,
        #[error("Invalid layer.")]
        InvalidLayer,
        #[error("Bincode operation failed")]
        BinCodeError { source: Box<bincode::ErrorKind> },
        #[error("IO operation failed")]
        IOError { source: std::io::Error },
    }
}

impl From<bincode::Error> for GraphError {
    fn from(source: bincode::Error) -> Self {
        GraphError::BinCodeError { source }
    }
}

impl From<std::io::Error> for GraphError {
    fn from(source: std::io::Error) -> Self {
        GraphError::IOError { source }
    }
}