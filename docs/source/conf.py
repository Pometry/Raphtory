# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from pathlib import Path
sys.path.insert(0, os.path.abspath('exts'))


# -- Project information -----------------------------------------------------

project = 'Raphtory'
copyright = '2022, Ben Steer'
author = 'Ben Steer'

# The full version, including alpha/beta/rc tags
release = '0.1.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'extractScalaAlgoDocs',
    'sphinx.ext.autodoc',
    'myst_parser',
    'sphinx_tabs.tabs',
    "nbsphinx"
]

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'inherited-members': True
}

# Extension options
myst_enable_extensions = ["deflist", "dollarmath"]
myst_heading_anchors = 3

raphtory_root = str(Path(__file__).resolve().parents[2])
raphtory_src_root = str(Path(__file__).resolve().parents[2] / "core" / "src" / "main" / "scala")
autodoc_packages = [
    "com.raphtory.algorithms.generic",
    "com.raphtory.algorithms.temporal",
    "com.raphtory.algorithms.filters"
]

# Uncomment to turn of rebuilding of scala and algorithm docs (use when writing other docs to speed up compile)
# build_scaladocs = False
# build_algodocs = False

sphinx_tabs_valid_builders = ['linkcheck']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'navigation_depth': -1,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static', 'images', '_scaladoc']
nbsphinx_kernel_name = 'python3'