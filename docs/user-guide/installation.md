# Installation

Raphtory is a library for Python and Rust. Installation is as simple as invoking the package manager of the corresponding programming language.

/// tab | :fontawesome-brands-python: Python
``` bash
pip install raphtory
```
///

/// tab | :fontawesome-brands-rust: Rust
``` shell
cargo add raphtory

# Or Cargo.toml
[dependencies]
raphtory = { version = "x"}
```
///

## Importing

To use the library import it into your project:

/// tab | :fontawesome-brands-python: Python
``` python
import raphtory as rp
```
///

/// tab | :fontawesome-brands-rust: Rust
``` rust
use raphtory::prelude::*;
```
///

## Docker image

Both the Python and Rust packages are available as official Docker images from the [Pometry Docker Hub](https://hub.docker.com/r/pometry/raphtory) page.

To download these using the docker CLI run:

/// tab | :fontawesome-brands-python: Python
``` bash
docker pull pometry/raphtory:latest-python
```
///

/// tab | :fontawesome-brands-rust: Rust
``` shell
docker pull pometry/raphtory
```
///

Running either container will start a Raphtory server by default, if this is all you need then the Rust image is sufficient.

However, the Python image contains the Raphtory Python package and all the required dependencies. You should use this image if you want to develop using the Python APIs in a containerised environment.

You can run a Raphtory container with the following Docker command:

```docker
docker run --rm -p 1736:1736 -v "$(pwd):/home/raphtory_server" pometry/raphtory:latest-python
```

For more information about running and configuring containers see the [Docker documentation](https://docs.docker.com/).
