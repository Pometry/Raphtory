use std::io::Result;
#[cfg(feature = "proto")]
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/graph.proto"], &["src/"])?;
    println!("cargo::rerun-if-changed=src/graph.proto");
    Ok(())
}

#[cfg(not(feature = "proto"))]
fn main() -> Result<()> {
    Ok(())
}
