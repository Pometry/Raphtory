pub(crate) mod incremental;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

pub use proto::Graph as ProtoGraph;
pub use serialise::{GraphCache, StableDecode, StableEncoder};
