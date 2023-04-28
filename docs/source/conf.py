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
sys.path.insert(0, os.path.abspath('exts'))

# # This is needed by the own-algorithms notebook:
# subprocess.run(['curl', '-o', '/tmp/lotr.csv', 'https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv'])


# -- Project information -----------------------------------------------------

project = 'raphtory'
copyright = '2023, Pometry'
author = 'Pometry'

# The full version, including alpha/beta/rc tags
release = "0.0.9"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    # 'extractScalaAlgoDocs',
    # 'extractRustDocs', 
    'sphinx.ext.autodoc',
    #'processWrapperDocs',
    # 'sphinx_tabs.tabs',
    "nbsphinx",
    'sphinx.ext.napoleon',
    'myst_parser',
]

suppress_warnings = ['myst.header', 'myst.anchor']

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'inherited-members': True,
    'special-members': "__call__, __new__, __init__",
    'show-inheritance': True
}

autodoc_member_order = 'groupwise'
autodoc_typehints_format = "short"

autosummary_generate = True
autosummary_ignore_module_all = True
# Extension options
myst_enable_extensions = ["deflist", "dollarmath"]
myst_heading_anchors = 3

raphtory_root = str(Path(__file__).resolve().parents[2])
# raphtory_src_root = str(Path(__file__).resolve().parents[2] / "core" / "src" / "main" / "scala")
# autodoc_packages = [
#     "com.raphtory.algorithms.generic",
#     "com.raphtory.algorithms.temporal",
#     "com.raphtory.algorithms.filters"
# ]

# Uncomment to turn of rebuilding of scala and algorithm docs (use when writing other docs to speed up compile)
build_scaladocs = False
build_algodocs = False
build_rustdocs = False

sphinx_tabs_valid_builders = ['linkcheck']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_templates"]
source_suffix = ['.rst', '.md']
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
html_static_path = ['images', '_rustdoc']
# nbsphinx_kernel_name = 'python3'

# rst_prolog = f"""
# .. |binder_link| replace:: Click here to launch the notebook
# .. _binder_link: https://mybinder.org/v2/gh/Raphtory/Raphtory/v{__version__}?labpath=example%2Fbinder_python%2Findex.ipynb
# """
