from __future__ import annotations

from keyword import iskeyword
from pyraphtory._codegen._docstring import convert_docstring
from collections import UserString, UserDict
from functools import cached_property


jpype_type_converters = {}
type_converters = {}


def jpype_type_converter(type_name: str):
    def converter(fun):
        jpype_type_converters[type_name] = fun.__name__
        return fun
    return converter


def type_converter(type_name: str):
    def converter(fun):
        type_converters[type_name] = fun.name
        return fun
    return converter


class LazyStr(UserString, str):
    def __new__(cls, *args, **kwargs):
        # we need to extend str so help works but don't actually want to create the immutable str here
        self = super().__new__(cls, "")
        return self

    def __init__(self, seq=None, initial=None):
        self._initial = initial
        if seq is not None:
            super().__init__(seq)

    @cached_property
    def data(self):
        value = self._initial()
        return value


class LazyAnnotations(UserDict, dict):
    # UserDict here is necessary such that dict(LazyAnnotations) actually calls the methods
    # instead of using fast dict copy
    def __init__(self, initials: dict):
        super().__init__(initials)
        self.lazy = set(initials.keys())

    def _unlazyfy(self):
        for key in self.lazy:
            self.__getitem__(key)

    def items(self):
        self._unlazyfy()
        return super().items()

    def values(self):
        self._unlazyfy()
        return super().values()

    def __getitem__(self, item):
        if item in self.lazy:
            self.lazy.remove(item)
            self[item] = clean_type(super().__getitem__(item))
        return super().__getitem__(item)

    def add_lazy_item(self, key, lazy_value):
        self[key] = lazy_value
        self.lazy.add(key)



def clean_identifier(name: str):
    if iskeyword(name):
        return str(name + "_")
    else:
        return str(name)


def clean_type(scala_type):
    from pyraphtory.interop._interop import get_type_repr
    type_name = str(get_type_repr(scala_type))
    return type_name


def get_jpype_type_converter(type):
    name = type.toString()
    return jpype_type_converters.get(name, type_converters.get(name, "to_jvm"))


def get_type_converter(type):
    name = type.toString()
    return type_converters.get(name, "to_jvm")


def build_method(name: str, method, jpype: bool, globals: dict, locals: dict):
    if jpype:
        type_converter = get_jpype_type_converter
    else:
        type_converter = get_type_converter

    params = [clean_identifier(name) for name in method.parameters()]
    types = list(method.types())
    name = clean_identifier(name)
    java_name = clean_identifier(method.name()) if jpype else method.name()
    implicits = [clean_identifier(name) for name in method.implicits()]
    implicit_types = types[len(params):]
    types = types[:len(params)]
    nargs = method.n()
    varargs = method.varargs()
    if varargs:
        varparam = params.pop()
        varparam_type = types.pop()
    defaults = method.defaults()
    required = min(defaults.keys(), default=nargs)

    args = ["self"]
    args.extend(f'{p} = DefaultValue("{defaults[i]}")' if i in defaults
                else (f"{p} = None" if i > required
                      else f"{p}")
                for i, p in enumerate(params))
    if varargs:
        args.append(f"*{varparam}")
    if implicits:
        args.append("_implicits=()")
    args = ", ".join(args)

    lines = [f"def {name}({args}):"]
    if implicits:
        lines.append(f"    if len(_implicits) < {len(implicits)}:")
        lines.append(f"        raise RuntimeError('missing implicit arguments')")
        lines.append(f"    if len(_implicits) > {len(implicits)}:")
        lines.append(f"        raise RuntimeError('too many implicit arguments')")
        for i, p in enumerate(implicits):
            lines.append(f"    {p} = {type_converter(implicit_types[i])}(_implicits[{i}])")
    lines.extend(f"    {p} = _check_default(self._jvm_object, {p}, {type_converter(types[i])})" if i in defaults else
                 f"    {p} = {type_converter(types[i])}({p})" for i, p in enumerate(params))
    if varargs:
        lines.append(f"    {varparam} = make_varargs(to_jvm(list({type_converter(varparam_type)}(v) for v in {varparam})))")
        params.append(varparam)
    lines.append(f"    return to_python(getattr(self._jvm_object, '{java_name}')({', '.join(p for p in params + implicits)}))")
    exec("\n".join(lines), globals, locals)
    py_method = locals[name]
    docstr = method.docs()
    py_method.__doc__ = LazyStr(initial=lambda: convert_docstring(docstr))
    py_method.__annotations__ = LazyAnnotations({n: t for n, t in zip(params, types)})
    if varargs:
        py_method.__annotations__.add_lazy_item(varparam, varparam_type)
    return py_method
