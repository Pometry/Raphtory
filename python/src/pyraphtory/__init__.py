import pyraphtory.vertex
import pyraphtory.input
import pyraphtory.graph
import pyraphtory.scala
import pyraphtory.spouts
import sys

if sys.version_info[:2] >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version(__package__)