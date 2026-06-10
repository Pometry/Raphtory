import sys as _sys
import inspect as _inspect

from . import _raphtory
from ._raphtory import *


def _init_submodules(path: str, module):
    for name, submodule in _inspect.getmembers(module, _inspect.ismodule):
        submodule_path = f"{path}.{name}"
        _sys.modules[submodule_path] = submodule
        _init_submodules(submodule_path, submodule)


_init_submodules("raphtory", _raphtory)

__doc__ = _raphtory.__doc__
if hasattr(_raphtory, "__all__"):
    __all__ = _raphtory.__all__
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
