import sys as _sys
from .raphtory import *

_sys.modules["raphtory.node_state"] = node_state
_sys.modules["raphtory.algorithms"] = algorithms
_sys.modules["raphtory.graph_gen"] = graph_gen
_sys.modules["raphtory.graph_loader"] = graph_loader
_sys.modules["raphtory.vectors"] = vectors
_sys.modules["raphtory.graphql"] = graphql

__doc__ = raphtory.__doc__
if hasattr(raphtory, "__all__"):
    __all__ = raphtory.__all__
else:
    __all__ = []

__all__.extend(["nullmodels", "plottingutils"])  # add the python modules

algorithms.__doc__ = "Algorithmic functions that can be run on Raphtory graphs"
graph_gen.__doc__ = "Generate Raphtory graphs from attachment models"
graph_loader.__doc__ = "Load and save Raphtory graphs from/to file(s)"

try:
    from importlib.metadata import version as _version

    __version__ = _version(__name__)
except Exception:
    # either 3.7 or package not installed, just don't set a version
    pass
