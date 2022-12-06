from keyword import iskeyword
from pyraphtory._docstring import convert_docstring


type_map = {"String": "str",
            "Double": "float",
            "Float": "float",
            "Short": "int",
            "Int": "int",
            "Long": "int",
            "Boolean": "bool",
            "Object": "Any",
            }


def clean_identifier(name: str):
    if iskeyword(name):
        return str(name + "_")
    else:
        return str(name)


def clean_type(scala_type):
    name = scala_type.toString()
    from pyraphtory.interop import get_type_repr
    type_name = get_type_repr(scala_type)
    return type_name


def build_method(name, method, jpype=False):
    params = [clean_identifier(name) for name in method.parameters()]
    types = [clean_type(name) for name in method.types()]
    name = clean_identifier(name)
    java_name = clean_identifier(method.name()) if jpype else method.name()
    implicits = [clean_identifier(name) for name in method.implicits()]
    nargs = method.n()
    varargs = method.varargs()
    if varargs:
        varparam = params.pop()
    defaults = method.defaults()
    required = max(defaults.keys(), default=nargs)

    args = ["self"]
    args.extend(f'{p}: {t} = DefaultValue("{defaults[i]}")' if i in defaults
                else (f"{p}: {t} = None" if i > required
                      else f"{p}: {t}")
                for i, (p, t) in enumerate(zip(params, types)))
    if varargs:
        args.append(f"*{varparam}: {types[-1]}")
    if implicits:
        args.append("_implicits=()")
    args = ", ".join(args)

    lines = [f"def {name}({args}):"]
    lines.append(f'    """{convert_docstring(method.docs())}"""')
    if implicits:
        lines.append(f"    if len(_implicits) < {len(implicits)}:")
        lines.append(f"        raise RuntimeError('missing implicit arguments')")
        lines.append(f"    if len(_implicits) > {len(implicits)}:")
        lines.append(f"        raise RuntimeError('too many implicit arguments')")
        for i, p in enumerate(implicits):
            lines.append(f"    {p} = to_jvm(_implicits[{i}])")
    lines.extend(f"    {p} = _check_default(self._jvm_object, {p})" if i in defaults else
                 f"    {p} = to_jvm({p})" for i, p in enumerate(params))
    if varargs:
        lines.append(f"    {varparam} = make_varargs(to_jvm(list({varparam})))")
        params.append(varparam)
    lines.append(f"    return to_python(getattr(self._jvm_object, '{java_name}')({', '.join(p for p in params + implicits)}))")
    return "\n".join(lines)
