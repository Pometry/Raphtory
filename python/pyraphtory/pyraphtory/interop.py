import re
from collections.abc import Iterable, Mapping
from pemja import findClass
import pyraphtory.proxy as proxy


_interop = findClass('com.raphtory.internals.management.PythonInterop')
_method_cache = {}
logger = _interop.logger()


def is_PyJObject(obj):
    return type(obj).__name__ == "PyJObject"


def snake_to_camel(name: str):
    """
    Convert python snake case names to scala-style camelCase preserving leading underscores
    (multiple intermediate underscores are eliminated).
    """
    parts = re.split(r"([^_])", name, 1)
    rest = parts[2].split("_")
    rest[1:] = [v.capitalize() for v in rest[1:]]
    return "".join(parts[:2] + rest)


def camel_to_snake(name: str):
    return _interop.camel_to_snake(name)


def decode(obj):
    return _interop.decode(obj)


def get_methods(name: str):
    if name in _method_cache:
        logger.trace(f"Retreiving cached methods for {name!r}")
        return _method_cache[name]
    else:
        logger.trace(f"Finding methods for {name!r}")
        res = _interop.methods(name)
        _method_cache[name] = res
        logger.trace(f"Methods for {name!r} added to cache")
        return res


def to_jvm(value):
    if is_PyJObject(value):
        logger.trace(f"Converting value {value!r}, already PyJObject")
        return decode(value)
    elif isinstance(value, proxy.GenericScalaProxy):
        logger.trace(f"Converting value {value!r}, decoding proxy object")
        return decode(value._jvm_object)
    elif isinstance(value, Mapping):
        logger.trace(f"Converting value {value!r}, decoding as Mapping")
        return decode({to_jvm(k): to_jvm(v) for k, v in value.items()})
    elif isinstance(value, Iterable) and not isinstance(value, str):
        logger.trace(f"Converting value {value!r}, decoding as Iterable")
        return decode([to_jvm(v) for v in value])
    else:
        logger.trace(f"Converting value {value!r}, primitive value returned unchanged")
        return value


def to_python(obj):
    if is_PyJObject(obj):
        return proxy.GenericScalaProxy(obj)
    else:
        return obj


def assign_id(s: str):
    return _interop.assign_id(s)
