# Raphtory Documentation

This directory contains the docs website which uses:
- [MkDocs](https://www.mkdocs.org/)
- [Material theme](https://squidfunk.github.io/mkdocs-material/)
- [PyMdown extensions](https://facelessuser.github.io/pymdown-extensions/)
- [mkdocstrings](https://mkdocstrings.github.io/)
    - Using the [mkdocstrings-python](https://mkdocstrings.github.io/python/) handler with a [literate-nav](https://oprypin.github.io/mkdocs-literate-nav/index.html) to create API pages from the stubs generated by `Raphtory/stub_gen/stub_gen.py`.

Note that some files (such as the `mkdocs.yml`) may be stored in the root of the repo rather than in this directory.

## Hosting

This website is hosted on ReadtheDocs which is configured in `Raphtory/.readthedocs.yaml` and via the web interface (which includes redirects).

ReadtheDocs builds and tests are triggered by the `.github/workflows/build_readthedocs.yml` action.

## Contributions

Contributions should loosely follow the [Microsoft style guide](https://learn.microsoft.com/en-us/style-guide/welcome/) using British english to follow the conventions in the codebase. 

You can use [Vale](https://vale.sh/) to enforce a style guide locally. 
