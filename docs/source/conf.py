# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
from sphinx.ext.autosummary import _import_by_name

import os
import sys
import warnings
import raphtory

import jinja2

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Raphtory'
copyright = '2023, Pometry'
author = 'Pometry'
release = '2023'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
    "matplotlib.sphinxext.plot_directive",
    "numpydoc",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_toggleprompt",
    "sphinx.ext.autodoc",
    'sphinx.ext.autosummary',
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "nbsphinx",
    "autodocsumm",
]

templates_path = ['_templates']
exclude_patterns = [
    "**.ipynb_checkpoints",
]

header = f"""\
.. currentmodule:: raphtory

.. ipython:: python
   :suppress:

   import raphtory
   from raphtory import vis
   import os   
   os.chdir(r'{os.path.dirname(os.path.dirname(__file__))}')

"""


html_context = {
    "header": header,
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"


html_static_path = ['_static', 'images']
html_css_files = [
    "css/custom.css",
    "css/getting_started.css",
    "css/raphtory.css",
]
html_logo = "_static/logo.svg"

html_use_modindex = True
htmlhelp_basename = "raphtory"

# -- Options for nbsphinx ------------------------------------------------

nbsphinx_allow_errors = True
# extlinks alias
extlinks = {
    "issue": ("https://github.com/pometry/raphtory/issues/%s", "GH %s"),
}

autosummary_generate = True
autosummary_imported_members = True

# numpydoc
def rstjinja(app, docname, source):
    """
    Render our pages as a jinja template for fancy templating goodness.
    """
    # https://www.ericholscher.com/blog/2016/jul/25/integrating-jinja-rst-sphinx/
    # Make sure we're outputting HTML
    if app.builder.format != "html":
        return
    src = source[0]
    rendered = app.builder.templates.render_string(src, app.config.html_context)
    source[0] = rendered

def setup(app):
    app.connect("source-read", rstjinja)

