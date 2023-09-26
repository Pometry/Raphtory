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
import sphinx_autosummary_accessors

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Raphtory'
copyright = '2023, Pometry'
author = 'Pometry'
release = '2021'
git_ref = os.environ.get("RAPHTORY_VERSION", "main")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    # Sphinx extensions
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "sphinx.ext.mathjax",
    # Third-party extensions
    "autodocsumm",
    "numpydoc",
    "sphinx_autosummary_accessors",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_favicon",
]

templates_path = ["_templates", sphinx_autosummary_accessors.templates_path]

exclude_patterns = []

# -- Extensions -------------------------------------------------

numpydoc_show_class_members = False
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
html_css_files = [
    "css/custom.css",
]
html_show_sourcelink = False

# Root paths
github_root = "https://github.com/Pometry/Raphtory"
static_assets_root = "https://raw.githubusercontent.com/Pometry/Raphtory/master"
html_logo = "_static/logos/raphtory-logo-bright-medium.png"

html_context = {
    "default_mode": "auto"
}

html_theme_options = {
    'nosidebar': True,
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
        "image_auto":  "_static/logos/raphtory-logo-bright-medium.png",
        "image_light": "_static/logos/raphtory-logo-bright-medium.png",
        "image_dark":  "_static/logos/raphtory-logo-bright-medium.png",
        "alt_text": "Raphtory - Home",
    },
    "show_version_warning_banner": True,
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "check_switcher": False,
    "show_toc_level": 3
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


# sphinx-ext-linkcode - Add external links to source code
# https://www.sphinx-doc.org/en/master/usage/extensions/linkcode.html
def linkcode_resolve(domain: str, info: dict[str, Any]) -> str | None:
    """
    Determine the URL corresponding to Python object.

    Based on pandas equivalent:
    https://github.com/pandas-dev/pandas/blob/main/doc/source/conf.py#L629-L686
    """
    if domain != "py":
        return None

    modname = info["module"]
    fullname = info["fullname"]

    submod = sys.modules.get(modname)
    if submod is None:
        return None

    obj = submod
    for part in fullname.split("."):
        try:
            with warnings.catch_warnings():
                # Accessing deprecated objects will generate noisy warnings
                warnings.simplefilter("ignore", FutureWarning)
                obj = getattr(obj, part)
        except AttributeError:
            return None

    try:
        fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError:
        try:  # property
            fn = inspect.getsourcefile(inspect.unwrap(obj.fget))
        except (AttributeError, TypeError):
            fn = None
    if not fn:
        return None

    try:
        source, lineno = inspect.getsourcelines(obj)
    except TypeError:
        try:  # property
            source, lineno = inspect.getsourcelines(obj.fget)
        except (AttributeError, TypeError):
            lineno = None
    except OSError:
        lineno = None

    linespec = f"#L{lineno}-L{lineno + len(source) - 1}" if lineno else ""

    conf_dir_path = Path(__file__).absolute().parent
    raphtory_root = (conf_dir_path.parent.parent / "raphtory").absolute()

    fn = os.path.relpath(fn, start=raphtory_root)
    return f"{github_root}/blob/{git_ref}/pometry/raphtory/{fn}{linespec}"

