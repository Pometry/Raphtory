#!/usr/bin/env bash
set -e
set -x
# Hide arrow dependency in all Cargo.toml files

# Detect the operating system
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    find . -name "Cargo.toml" -type f -exec sed -i '' '/raphtory-arrow/ s/^/# /' {} +
else
    # Linux and other UNIX-like systems
    find . -name "Cargo.toml" -type f -exec sed -i '/raphtory-arrow/ s/^/# /' {} +
fi
