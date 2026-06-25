#!/usr/bin/env bash
#
# Idempotently publish workspace crates to crates.io.
#
# For each crate (in the order given) it reads the local version from
# `cargo metadata`, checks whether that exact version is already on
# crates.io, and only publishes the ones that are missing. This makes
# re-running a release safe even if some crates published on a previous
# attempt (plain `cargo publish --workspace` aborts the moment one
# version already exists).
#
# Usage:
#   scripts/publish_crates.sh [-p crate ...] | [crate ...]
#
# Env:
#   CARGO_REGISTRY_TOKEN   token used by `cargo publish` (required to publish)
#   DRY_RUN=true           only run `cargo publish --dry-run`, never upload
#
# Both "-p a -p b" and "a b" forms are accepted so the CI input can be
# passed through verbatim.

set -euo pipefail

# crates.io asks for a descriptive User-Agent on API requests.
UA="raphtory-release (fabian.murariu@pometry.com)"

# Default list mirrors the CI default (dependency order matters).
DEFAULT_CRATES="raphtory-api pometry-storage raphtory-core raphtory-storage raphtory raphtory-graphql"

crates=()
for tok in "$@"; do
  # Skip flag-style tokens: "-p" (cargo's per-package flag) and
  # "--workspace" both mean "use the full default list", which we
  # enumerate explicitly below in dependency order.
  case "$tok" in
    -*) continue ;;
  esac
  crates+=("$tok")
done
if [ "${#crates[@]}" -eq 0 ]; then
  # shellcheck disable=SC2206
  crates=($DEFAULT_CRATES)
fi

DRY_RUN="${DRY_RUN:-false}"

for crate in "${crates[@]}"; do
  version=$(cargo metadata --no-deps --format-version 1 \
    | jq -r --arg c "$crate" '.packages[] | select(.name==$c) | .version')
  if [ -z "$version" ] || [ "$version" = "null" ]; then
    echo "::error::could not determine local version for $crate" >&2
    exit 1
  fi

  http_code=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "User-Agent: $UA" \
    "https://crates.io/api/v1/crates/$crate/$version")

  if [ "$http_code" = "200" ]; then
    echo "✓ $crate@$version already on crates.io — skipping"
    continue
  fi

  echo "→ $crate@$version not published (HTTP $http_code) — publishing"
  cargo publish -p "$crate" --dry-run
  if [ "$DRY_RUN" != "true" ]; then
    cargo publish -p "$crate"
  fi
done
