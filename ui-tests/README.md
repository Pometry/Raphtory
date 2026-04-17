# pometry-ui E2E Tests

Warning: This repository is not meant to be used standalone, but is to be included as a submodule in the pometry-ui or
raphtory monorepos for testing the UI.

This repo contains a set of playwright tests for testing the UI with raphtory from end to end.

It assumes that the vanilla UI exists at a provided base URL. This base URL is read by dotenv, and can be set by .env
file or environment variable.
