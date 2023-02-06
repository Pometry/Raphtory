"""Classes and methods for interacting with the jvm

.. data:: logger

   Wrapper for the java logger
"""
from ._interop import ScalaClassProxy, GenericScalaProxy, ScalaPackage, ScalaClassProxyWithImplicits, logger, register, to_jvm, to_python

__all__ = [
    "ScalaClassProxy",
    "GenericScalaProxy",
    "ScalaPackage",
    "logger",
    "register"
]
