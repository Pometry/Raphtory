"""Python wrappers for Raphtory"""
import sys
from pyraphtory._context import local, remote
from pyraphtory._config import add_classpath, get_java_args, set_java_args

if sys.version_info[:2] >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version(__package__)
__all__ = ["local", "remote", "add_classpath", "get_java_args", "set_java_args"]
