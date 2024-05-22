#!/usr/bin/env bash
set -e
set -x
# Hide arrow dependency in all Cargo.toml files
find . -name "Cargo.toml" -type f -exec sed -i '/raphtory-arrow/ s/^/# /' {} +