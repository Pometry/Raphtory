from sphinx.application import Sphinx, Config
from pyraphtory.interop import ScalaClassProxy


def process_base_classes(app, name, obj, options, bases):
    if issubclass(obj, ScalaClassProxy):
        bases[:] = [f"..autoclass:: {obj.__class__}"]
    else:
        bases[:] = []


def setup(app: Sphinx):
    app.connect("autodoc-process-bases", process_base_classes)