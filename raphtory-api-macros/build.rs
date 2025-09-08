use std::io::Result;
fn main() -> Result<()> {
    println!("cargo::rustc-check-cfg=cfg(has_debug_symbols)");

    if let Ok(profile) = std::env::var("PROFILE") {
        if profile.contains("debug") {
            println!("cargo::rustc-cfg=has_debug_symbols");
        }
    }
    Ok(())
}
