use std::io::Result;

fn main() -> Result<()> {
    println!("cargo::rustc-check-cfg=cfg(has_debug_symbols)");
    if let Ok("true") = std::env::var("DEBUG").as_deref() {
        println!("cargo::rustc-cfg=has_debug_symbols");
    }

    Ok(())
}
