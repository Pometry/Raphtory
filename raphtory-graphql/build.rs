fn main() {
    let ui_index_path =
        std::env::var("RAPHTORY_UI_INDEX_PATH").unwrap_or("../resources/index.html".to_owned());
    println!(
        "{}",
        format!("cargo:rustc-env=RAPHTORY_UI_INDEX_PATH={ui_index_path}")
    );
    println!("cargo:rerun-if-env-changed=RAPHTORY_UI_INDEX_PATH")
}
