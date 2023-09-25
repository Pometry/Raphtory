# do not add from __future__ import annotations as it will break the typehints parsing
# (I think this is a bug in autodoc and may be fixed at some point)

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from sphinx.ext.autosummary import _import_by_name

import os
import re
import sys
import warnings
import raphtory
from sphinx.util.typing import stringify_annotation
from sphinx.util import inspect

# for type annotations resolution (need to actually import everything that we want to use in a type hint in the docs)
from typing import *
from raphtory import *

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
   from raphtory import export
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

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

autosummary_generate = True
autosummary_imported_members = True
autodoc_typehints = "both"
autodoc_typehints_description_target = "documented"
autodoc_type_aliases = {}


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


def add_typehints(app, objtype: str, name: str, obj,
                  options: dict, args: str, retann: str) -> tuple[str | Any, str | Any] | tuple[str, None]:
    """Record type hints to env object.

    This function does the same as the sphinx.ext.autodoc.typehints extension but for
    signatures that are defined in the docstring.
    """
    if not hasattr(obj, "__annotations__"):
        # If an object has annotations, typehints extension will handle it, otherwise,
        # we need to look at the signature for the type hints

        # make sure we set the configuration option in the same way
        if app.config.autodoc_typehints_format == 'short':
            mode = 'smart'
        else:
            mode = 'fully-qualified'

        try:
            if callable(obj):
                # build a mock function from the signature to get the correct annotations
                exec_parts = [f"def _annotations_moc"]
                if args is not None:
                    exec_parts.append(args)
                else:
                    exec_parts.append("()")
                if retann:
                    exec_parts.append(f" -> {retann}")
                exec_parts.append(":\n    pass")
                res = globals()
                exec("".join(exec_parts), res)

                # extract type hints and store them in the appropriate temp data
                # (this is the same as what the typehints extension does)
                annotations = app.env.temp_data.setdefault('annotations', {})
                annotation = annotations.setdefault(name, {})
                sig = inspect.signature(res["_annotations_moc"], type_aliases=app.config.autodoc_type_aliases)
                for param in sig.parameters.values():
                    if param.annotation is not param.empty:
                        annotation[param.name] = stringify_annotation(param.annotation, mode)
                if sig.return_annotation is not sig.empty:
                    retann = stringify_annotation(sig.return_annotation, mode)
                    annotation['return'] = retann
                kwargs = {}
                if app.config.autodoc_typehints in ('none', 'description'):
                    kwargs.setdefault('show_annotation', False)
                if app.config.autodoc_typehints_format == "short":
                    kwargs.setdefault('unqualified_typehints', True)

                # we need to reparse the signature to get the correct formatting for links to work
                # and to enable the 'description' option to strip the type hints from the signature
                args = inspect.stringify_signature(sig, **kwargs)
                if args:
                    matched = re.match(r'^(\(.*\))\s+->\s+(.*)$', args)
                    if matched:
                        args = matched.group(1)
                        retann = matched.group(2)
                        return args, retann
                    else:
                        return args, None
        except (TypeError, ValueError):
            pass


def setup(app):
    app.connect("source-read", rstjinja)
    app.connect('autodoc-process-signature', add_typehints, priority=0)
