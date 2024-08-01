import sys
from .raphtory import *

sys.modules["raphtory.algorithms"] = algorithms
sys.modules["raphtory.graph_gen"] = graph_gen
sys.modules["raphtory.graph_loader"] = graph_loader
sys.modules["raphtory.vectors"] = vectors
sys.modules["raphtory.graphql"] = graphql

__doc__ = raphtory.__doc__
if hasattr(raphtory, "__all__"):
    __all__ = raphtory.__all__

algorithms.__doc__ = "Algorithmic functions that can be run on Raphtory graphs"
graph_gen.__doc__ = "Generate Raphtory graphs from attachment models"
graph_loader.__doc__ = "Load and save Raphtory graphs from/to file(s)"

try:
    from importlib.metadata import version as _version

    __version__ = _version(__name__)
except Exception:
    # either 3.7 or package not installed, just don't set a version
    pass
