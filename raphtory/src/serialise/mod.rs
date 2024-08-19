pub mod incremental;
pub mod serialise;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/serialise.rs"));
}

pub use proto::Graph as ProtoGraph;
