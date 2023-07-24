#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd "$DIR/.."

log() {
  echo "$(basename ${BASH_SOURCE[0]}): $@"
}

install_hooks() {
  git config core.hooksPath \
    || git config core.hooksPath ./dev/hooks
}

install_rust_nightly() {
  rustup toolchain install nightly
}

log 'installing rust nightly...'
install_rust_nightly
log 'configuring hooks...'
install_hooks