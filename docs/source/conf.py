# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import inspect
import sys
import warnings
from pathlib import Path
from typing import Any

import raphtory.typing
import sphinx_autosummary_accessors
from raphtory import __version__
from sphinx.ext.intersphinx import missing_reference

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Raphtory"
copyright = "2023, Pometry"
author = "Pometry"
release = __version__
git_ref = "master"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    # Sphinx extensions
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    # "sphinx.ext.linkcode",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    # Third-party extensions
    "autodocsumm",
    "numpydoc",
    "notfound.extension",
    "sphinx_autosummary_accessors",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_favicon",
    "sphinx.ext.napoleon",
]

templates_path = ["_templates", sphinx_autosummary_accessors.templates_path]

exclude_patterns = []

# -- Extensions -------------------------------------------------

numpydoc_show_class_members = False
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]
html_show_sourcelink = False

# Root paths
github_root = "https://github.com/Pometry/Raphtory"
static_assets_root = "https://raw.githubusercontent.com/Pometry/Raphtory/master"
html_logo = "_static/logos/raphtory-logo-bright-medium.png"

html_context = {"default_mode": "auto"}

html_theme_options = {
    "nosidebar": True,
    "search_bar_text": "Search here...",
    "external_links": [
        {
            "name": "User Guide",
            "url": "https://www.raphtory.com",
        },
        {
            "name": "Created by Pometry",
            "url": "https://www.pometry.com/",
        },
    ],
    "icon_links": [
        {
            "name": "GitHub",
            "url": github_root,
            "icon": "fa-brands fa-github",
        },
        {
            "name": "Slack",
            "url": "https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA",
            "icon": "fa-brands fa-slack",
        },
        {
            "name": "X (Twitter)",
            "url": "https://twitter.com/Raphtory",
            "icon": "fa-brands fa-twitter",
        },
    ],
    "logo": {
        "image_auto": "_static/logos/raphtory-logo-bright-medium.png",
        "image_light": "_static/logos/raphtory-logo-bright-medium.png",
        "image_dark": "_static/logos/raphtory-logo-bright-medium.png",
        "logo_url": "reference/index.html",
        "alt_text": "Raphtory - Home",
    },
    "show_version_warning_banner": True,
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "check_switcher": False,
    "show_toc_level": 3,
}

# sphinx-favicon - Add support for custom favicons
# https://github.com/tcmetzger/sphinx-favicon
favicons = [
    {
        "rel": "icon",
        "sizes": "32x32",
        "href": "icons/favicon-32x32.png",
    },
    {
        "rel": "apple-touch-icon",
        "sizes": "180x180",
        "href": "icons/touchicon-180x180.png",
    },
]

# sphinx view code
viewcode_line_numbers = True

autodoc_typehints = "description"
add_module_names = False
autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_ignore_module_all = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "networkx": ("https://networkx.org/documentation/stable/", None),
    "pyvis": ("https://pyvis.readthedocs.io/en/latest/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "matplotlib": ("https://matplotlib.org/stable/", None),
}


# see https://github.com/sphinx-doc/sphinx/issues/10785 for why this is needed.
def resolve_type_aliases(app, env, node, contnode):
    """Add a fallback for missing :class: references to type aliases as :type: instead."""
    if node["refdomain"] == "py" and node["reftype"] == "class":
        return app.env.get_domain("py").resolve_xref(
            env, node["refdoc"], app.builder, "type", node["reftarget"], node, contnode
        )


def resolve_none(app, env, node, contnode):
    """Add a fallback for resolving None in type annotations"""
    if (
        node["refdomain"] == "py"
        and node["reftype"] == "class"
        and node["reftarget"] == "None"
    ):
        node["reftype"] = "obj"
        res = missing_reference(app, env, node, contnode)
        node["reftype"] = "class"
        return res


def resolve_in_typing_module(app, env, node, contnode):
    """Add a fallback for resolving missing :class: references in typing module"""
    if node["refdomain"] == "py" and node["reftype"] == "class":
        old_target = node["reftarget"]
        node["reftarget"] = "typing." + old_target
        node["reftype"] = "obj"
        res = missing_reference(app, env, node, contnode)
        node["reftarget"] = old_target
        node["reftype"] = "class"
        return res


RESOLVE_ALIASES = {
    "datetime": "datetime.datetime",
    "DataFrame": "pandas.DataFrame",
    "nx": "networkx",
    "np": "numpy",
    "pd": "pandas",
}


def resolve_aliases(app, env, node, contnode):
    """Add a fallback for resolving specific aliases used in type hints"""
    if node["refdomain"] == "py":
        split_target = node["reftarget"].split(".")
        if any(s in RESOLVE_ALIASES for s in split_target):
            old_target = node["reftarget"]
            node["reftarget"] = ".".join(
                RESOLVE_ALIASES.get(s, s) for s in split_target
            )
            res = missing_reference(app, env, node, contnode)
            node["reftarget"] = old_target
            return res


def setup(app):
    app.connect("missing-reference", resolve_type_aliases)
    app.connect("missing-reference", resolve_in_typing_module)
    app.connect("missing-reference", resolve_aliases)
    app.connect("missing-reference", resolve_none)
