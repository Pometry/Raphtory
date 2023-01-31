import sys
from pyraphtory._context import local, remote

if sys.version_info[:2] >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version(__package__)
__all__ = [local, remote]
