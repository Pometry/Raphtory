pub mod algorithms;
pub mod graph_gen;
pub mod graph_loader;

pub mod base_modules;
#[cfg(feature = "rdf")]
pub mod rdf;
#[cfg(feature = "shacl")]
pub mod shacl;
#[cfg(feature = "vectors")]
pub mod vectors;
