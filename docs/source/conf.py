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
import subprocess
from pathlib import Path
from pyraphtory import __version__
sys.path.insert(0, os.path.abspath('exts'))

# This is needed by the own-algorithms notebook:
subprocess.run(['curl', '-o', '/tmp/lotr.csv', 'https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv'])


# -- Project information -----------------------------------------------------

project = 'Raphtory'
copyright = '2022, Ben Steer'
author = 'Ben Steer'

# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'extractScalaAlgoDocs',
    'sphinx.ext.autodoc',
    'processWrapperDocs',
    'myst_parser',
    'sphinx_tabs.tabs',
    "nbsphinx"
]

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'inherited-members': True,
    'special-members': "__call__",
    'show-inheritance': True
}

autodoc_member_order = 'groupwise'
autodoc_typehints_format = "short"

autosummary_generate = True
autosummary_ignore_module_all = False
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
build_scaladocs = True
build_algodocs = True

sphinx_tabs_valid_builders = ['linkcheck']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_templates"]

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

rst_prolog = f"""
.. |binder_link| replace:: Click here to launch the notebook
.. _binder_link: https://mybinder.org/v2/gh/Raphtory/Raphtory/v{__version__}?labpath=examples%2Fbinder_python%2Findex.ipynb
"""
