use std::path::PathBuf;

extern crate cpp_build;

fn main() {
    if let Ok(paths) = std::env::var("LD_LIBRARY_PATH") {
        for path in paths.split(":") {
            if path != "" {
                println!("cargo:rustc-link-search={}", path);
            }
        }
    };

    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-search=/usr/lib");

    if get_os_type() == "macos" {
        println!("cargo:rustc-link-lib=omp");
        println!("cargo:rustc-link-lib=faiss");
    } else {
        println!("cargo:rustc-link-lib=static=faiss");
        println!("cargo:rustc-link-lib=gomp");
        println!("cargo:rustc-link-lib=blas");
        println!("cargo:rustc-link-lib=lapack");
    }

    cpp_build::Config::new()
        .include(PathBuf::from(
            "/Users/pedrorico/pometry/raphtory/faiss-rs/faiss-sys/faiss",
        ))
        .build("src/lib.rs");
}

fn get_os_type() -> &'static str {
    if cfg!(target_os = "linux") {
        return "linux";
    } else if cfg!(target_os = "macos") {
        return "macos";
    } else {
        panic!("unknow os type");
    }
}
