use std::io::Result;
#[cfg(feature = "proto")]
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/serialise/graph.proto"], &["src/serialise"])?;
    println!("cargo::rerun-if-changed=src/graph.proto");
    Ok(())
}

#[cfg(not(feature = "proto"))]
fn main() -> Result<()> {
    Ok(())
}
