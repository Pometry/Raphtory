pub(crate) mod incremental;
mod proto_ext;
mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

pub use proto::Graph as ProtoGraph;
pub use serialise::{CacheOps, StableDecode, StableEncode};
