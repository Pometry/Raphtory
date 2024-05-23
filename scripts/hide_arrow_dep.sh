#!/usr/bin/env bash
set -e
set -x
# Hide arrow dependency in all Cargo.toml files

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

$SCRIPT_DIR/flip_ra.py Cargo.toml > Cargo.toml.bk
mv Cargo.toml.bk Cargo.toml
