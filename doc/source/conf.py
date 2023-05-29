# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
from sphinx.ext.autosummary import _import_by_name

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Raphtory'
copyright = '2023, Pometry'
author = 'Pometry'
release = '2020'

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
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "nbsphinx",
]

templates_path = ['_templates']
exclude_patterns = [
    "**.ipynb_checkpoints",
]

header = f"""\
.. currentmodule:: pandas

.. ipython:: python
   :suppress:

   import raphtory
"""


html_context = {
    "header": header,
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"


html_logo = "_static/logo.svg"
html_static_path = ['_static']
html_css_files = [
    "css/getting_started.css",
    "css/raphtory.css",
]
#html_favicon = "../../web/pandas/static/img/favicon.ico"

html_use_modindex = True
htmlhelp_basename = "raphtory"

# -- Options for nbsphinx ------------------------------------------------

nbsphinx_allow_errors = True
# extlinks alias
extlinks = {
    "issue": ("https://github.com/pometry/raphtory/issues/%s", "GH %s"),
}

# numpydoc
