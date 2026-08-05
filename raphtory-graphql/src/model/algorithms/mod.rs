//! Statically defined graph algorithms exposed through `Graph.algorithm`.
pub mod inputs;
pub(crate) mod resolvers;

pub mod outputs;
#[cfg(test)]
mod tests;

pub use resolvers::GqlAlgorithms;
