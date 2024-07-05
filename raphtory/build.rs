use std::io::Result;
#[cfg(feature = "proto")]
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/graph.proto"], &["src/"])?;
    Ok(())
}

#[cfg(not(feature = "proto"))]
fn main() -> Result<()> {
    Ok(())
}
