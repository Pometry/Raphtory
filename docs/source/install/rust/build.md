# Build

Building the rust core is done using cargo. The following command will build the core.
Note: This requires that you have the rust toolchain installed.

    make rust-build
    or
    cargo build

## Import the raphtory package into a rust project

To use the raphtory core in a rust project, add the following to your Cargo.toml file:
Note: The path should be the path to the raphtory directory


    [dependencies]
    raphtory = {path = "../raphtory", version = "0.0.11" }
     
    or 

    [dependencies]
    raphtory = "0.0.11"

