use std::io::Result;
#[cfg(feature = "proto")]
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/serialise/graph.proto"], &["src/serialise"])?;
    println!("cargo::rerun-if-changed=src/serialise/graph.proto");

    println!("cargo::rustc-check-cfg=cfg(has_debug_symbols)");
    if let Ok("true" | "1" | "2") = std::env::var("DEBUG").as_deref() {
        println!("cargo::rustc-cfg=has_debug_symbols");
    }
    Ok(())
}

#[cfg(not(feature = "proto"))]
fn main() -> Result<()> {
    println!("cargo::rustc-check-cfg=cfg(has_debug_symbols)");
    if let Ok("true" | "1" | "2") = std::env::var("DEBUG").as_deref() {
        println!("cargo::rustc-cfg=has_debug_symbols");
    }
    Ok(())
}
