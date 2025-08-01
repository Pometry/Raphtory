import ast
import builtins
import inspect
import logging
import textwrap
import types
from importlib.machinery import ExtensionFileLoader
from itertools import chain
from logging import ERROR
from pathlib import Path
from types import (
    BuiltinFunctionType,
    BuiltinMethodType,
    GetSetDescriptorType,
    MethodDescriptorType,
    ModuleType,
    ClassMethodDescriptorType,
)
from typing import *

from docstring_parser import parse, DocstringStyle, DocstringParam
import builtins


logger = logging.getLogger(__name__)
fn_logger = logging.getLogger(__name__)
cls_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TAB = " " * 4

MethodTypes = (
    BuiltinMethodType,
    MethodDescriptorType,
)


comment = """from __future__ import annotations

###############################################################################
#                                                                             #
#                      AUTOGENERATED TYPE STUB FILE                           #
#                                                                             #
#    This file was automatically generated. Do not modify it directly.        #
#    Any changes made here may be lost when the file is regenerated.          #
#                                                                             #
###############################################################################\n"""

imports = []

# imports for type checking
global_ns = {}


def is_builtin(name: str) -> bool:
    """check if name is actually builtin"""
    return hasattr(builtins, name)


def set_imports(preamble: list[str]):
    """
    Specify import statements that are added to each generated file

    Arguments:
        preamble: the list of import statements

    Note that this string will be evaluated to create the global namespace for checking type annotations
    """
    global imports
    global global_ns
    imports = preamble
    exec("\n".join(imports), global_ns)


def format_type(obj) -> str:
    if isinstance(obj, type):
        return obj.__qualname__
    if obj is ...:
        return "..."
    if isinstance(obj, types.FunctionType):
        return obj.__name__
    if isinstance(obj, tuple):
        # Special case for `repr` of types with `ParamSpec`:
        return "[" + ", ".join(format_type(t) for t in obj) + "]"
    return repr(obj)


class AnnotationError(Exception):
    pass


def _validate_ast(parsed):
    for node in ast.walk(parsed):
        if isinstance(node, ast.Name):
            if node.id not in global_ns and node.id not in builtins.__dict__:
                raise AnnotationError(f"Unknown type {node.id}")


def validate_annotation(annotation: str):
    parsed = ast.parse(f"_: {annotation}")
    _validate_ast(parsed.body[0].annotation)


def validate_default(default_value: str):
    try:
        parsed = ast.parse(default_value)
        _validate_ast(parsed)
    except Exception as e:
        print(e)
        raise e


def format_param(param: inspect.Parameter) -> str:
    if param.kind == param.VAR_KEYWORD:
        name = f"**{param.name}"
    elif param.kind == param.VAR_POSITIONAL:
        name = f"*{param.name}"
    else:
        name = param.name
    if param.annotation is not param.empty:
        annotation = param.annotation
        if not isinstance(annotation, str):
            annotation = format_type(annotation)
        if param.default is not param.empty:
            return f"{name}: {annotation} = {param.default}"
        else:
            return f"{name}: {annotation}"
    else:
        if param.default is param.empty:
            return name
        else:
            return f"{name}={param.default}"


def format_signature(sig: inspect.Signature) -> str:
    sig_str = (
        "(" + ", ".join(format_param(param) for param in sig.parameters.values()) + ")"
    )
    if sig.return_annotation is not sig.empty:
        sig_str += f" -> {sig.return_annotation}"
    return sig_str


def same_default(doc_default: Optional[str], param_default: Any) -> bool:
    if doc_default is None:
        return param_default is None
    else:
        doc_val = eval(doc_default)
        if doc_val is None:
            return param_default is None
        return doc_val == param_default


def clean_parameter(
    param: inspect.Parameter,
    type_annotations: dict[str, dict[str, Any]],
):
    annotations = {}
    if param.default is not inspect.Parameter.empty:
        annotations["default"] = format_type(param.default)

    doc_annotation = type_annotations.pop(param.name, None)
    if doc_annotation is not None:
        annotations["annotation"] = doc_annotation["annotation"]
        default_from_docs = doc_annotation.get("default", None)
        if default_from_docs is not None:
            if param.default is not param.empty:
                if param.default is not ...:
                    if not same_default(default_from_docs, param.default):
                        fn_logger.warning(
                            f"mismatched default value: docs={repr(default_from_docs)}, signature={param.default}"
                        )
                else:
                    annotations["default"] = default_from_docs
            else:
                fn_logger.error(
                    f"parameter {param.name} has default value {repr(default_from_docs)} in documentation but no default in signature."
                )
        else:
            if param.default is not param.empty and param.default is not None:
                fn_logger.warning(
                    f"default value for parameter {param.name} with value {repr(param.default)} in signature is not documented"
                )
    else:
        if param.name not in {"self", "cls"}:
            fn_logger.warning(f"missing parameter {param.name} in docs.")
    return param.replace(**annotations)


def clean_signature(
    sig: inspect.Signature,
    type_annotations: dict[str, dict[str, Any]],
    return_type: Optional[str] = None,
) -> str:

    new_params = [clean_parameter(p, type_annotations) for p in sig.parameters.values()]
    for param_name, annotations in type_annotations.items():
        fn_logger.error(
            f"parameter {param_name} appears in documentation but does not exist."
        )
    sig = sig.replace(parameters=new_params)
    if return_type is not None:
        sig = sig.replace(return_annotation=return_type)
    return format_signature(sig)


def insert_self(signature: inspect.Signature) -> inspect.Signature:
    self_param = inspect.Parameter("self", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
    return signature.replace(parameters=[self_param, *signature.parameters.values()])


def insert_cls(signature: inspect.Signature) -> inspect.Signature:
    cls_param = inspect.Parameter("cls", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)
    return signature.replace(parameters=[cls_param, *signature.parameters.values()])


def cls_signature(cls: type) -> Optional[inspect.Signature]:
    try:
        return inspect.signature(cls)
    except ValueError:
        pass


def from_module(obj, obj_name: str, name: str) -> bool:
    module = inspect.getmodule(obj)
    module_name = module.__name__ if module else None
    if module_name == "builtins" and not is_builtin(obj_name):
        logger.error(
            f"{obj_name} has default module 'builtins', should be {repr(name)}"
        )
        return True
    if module_name == name:
        return True
    logger.debug(f"ignoring {obj_name} with module name {module_name}")
    return False


def format_docstring(docstr: Optional[str], tab: str, ellipsis: bool) -> str:
    if docstr:
        if "\n" in docstr:
            return f'{tab}"""\n{textwrap.indent(docstr, tab)}\n{tab}"""\n'
        else:
            return f'{tab}"""{docstr}"""\n'
    else:
        return f"{tab}...\n" if ellipsis else ""


def extract_param_annotation(param: DocstringParam) -> dict:
    res = {}
    if param.type_name is None:
        res["annotation"] = "Any"
    else:
        type_val = param.type_name
        try:
            validate_annotation(type_val)
            if param.is_optional:
                type_val = f"Optional[{type_val}]"
            res["annotation"] = type_val
        except Exception as e:
            res["annotation"] = "Any"
            fn_logger.error(
                f"Invalid annotation {repr(type_val)} for parameter {param.arg_name}: {e}"
            )

    if param.default is not None or param.is_optional:
        if param.default is not None:
            try:
                validate_default(param.default)
                res["default"] = param.default
            except Exception as e:
                fn_logger.error(
                    f"Invalid default value {repr(param.default)} for parameter {param.arg_name}: {e}"
                )
    return res


def extract_types(
    obj, docs_overwrite: Optional[str] = None, needs_return=True
) -> (dict[str, dict], Optional[str]):
    """
    Extract types from documentation
    """
    try:
        docstr = docs_overwrite or obj.__doc__
        if docstr is not None:
            parse_result = parse(docstr, DocstringStyle.GOOGLE)
            type_annotations = {
                param.arg_name: extract_param_annotation(param)
                for param in parse_result.params
            }
            if (
                parse_result.returns is not None
                and parse_result.returns.type_name is not None
            ):
                return_type = parse_result.returns.type_name
                try:
                    validate_annotation(return_type)
                except Exception as e:
                    fn_logger.error(f"Invalid return type {repr(return_type)}: {e}")
                    return_type = None
            else:
                if needs_return:
                    fn_logger.warning(f"Missing return type annotation")
                return_type = None
            return type_annotations, return_type
        else:
            fn_logger.warning(f"Missing documentation")
            return dict(), None
    except Exception as e:
        fn_logger.error(f"failed to parse docstring: {e}")
        return dict(), None


def get_decorator(method):
    if (inspect.ismethod(method) and inspect.isclass(method.__self__)) or isinstance(
        method, ClassMethodDescriptorType
    ):
        return "@classmethod"

    if inspect.isgetsetdescriptor(method):
        return "@property"

    if inspect.isfunction(method) or isinstance(method, BuiltinFunctionType):
        return "@staticmethod"

    return None


def gen_fn(
    function: Union[BuiltinFunctionType, BuiltinMethodType, MethodDescriptorType],
    name: str,
    is_method: bool = False,
    signature_overwrite: Optional[inspect.Signature] = None,
    docs_overwrite: Optional[str] = None,
) -> str:
    init_tab = TAB if is_method else ""
    fn_tab = TAB * 2 if is_method else TAB
    signature = signature_overwrite or inspect.signature(function)
    type_annotations, return_type = extract_types(
        function, docs_overwrite, signature.return_annotation is signature.empty
    )
    docstr = format_docstring(function.__doc__, tab=fn_tab, ellipsis=True)
    signature = clean_signature(
        signature,  # type: ignore
        type_annotations,
        return_type,
    )
    decorator = None
    if is_method:
        decorator = get_decorator(function)

    if name == "__new__":
        # new is special and not a class method
        decorator = None

    fn_str = f"{init_tab}def {name}{signature}:\n{docstr}"

    return f"{init_tab}{decorator}\n{fn_str}" if decorator else fn_str


def gen_bases(cls: type) -> str:
    if cls.__bases__:
        bases = "(" + ", ".join(format_type(obj) for obj in cls.__bases__) + ")"
    else:
        bases = ""
    return bases


def gen_class(cls: type, name) -> str:
    contents = list(vars(cls).items())
    contents.sort(key=lambda x: x[0])
    entities: list[str] = []
    global cls_logger
    global fn_logger
    cls_logger = logger.getChild(name)
    for obj_name, entity in contents:
        fn_logger = cls_logger.getChild(obj_name)
        if obj_name.startswith("__") and not (
            obj_name == "__init__" or obj_name == "__new__"
        ):
            # Cannot get doc strings for __ methods (except for __new__ which we special-case by passing in the class docstring)
            fn_logger.setLevel(ERROR)
        entity = inspect.unwrap(entity)
        if obj_name == "__init__" or obj_name == "__new__":
            # Get __init__ signature from class info
            signature = cls_signature(cls)
            if signature is not None:
                if obj_name == "__new__":
                    signature = insert_cls(signature.replace(return_annotation=name))
                else:
                    signature = insert_self(signature)

                entities.append(
                    gen_fn(
                        entity,
                        obj_name,
                        is_method=True,
                        signature_overwrite=signature,
                        docs_overwrite=cls.__doc__,
                    )
                )
        else:
            if isinstance(entity, MethodTypes) or inspect.ismethoddescriptor(entity):
                entities.append(gen_fn(entity, obj_name, is_method=True))
            elif isinstance(entity, GetSetDescriptorType):
                entities.append(
                    gen_fn(
                        entity,
                        obj_name,
                        is_method=True,
                        signature_overwrite=insert_self(inspect.Signature()),
                    )
                )
            else:
                logger.debug(f"ignoring {repr(obj_name)}: {repr(entity)}")

    docstr = format_docstring(cls.__doc__, tab=TAB, ellipsis=not entities)
    str_entities = "\n".join(entities)
    bases = gen_bases(cls)
    return f"class {name}{bases}: \n{docstr}\n{str_entities}"


def not_this_module_import(line: str, full_name: str) -> bool:
    return (
        f"from {full_name} import" not in line
        and f"import {full_name}" != line
        and f"import {full_name} " not in line
    )


def gen_module(
    module: ModuleType, name: str, path: Path, log_path, full_name=None
) -> None:
    if full_name is None:
        full_name = name
    global logger
    global fn_logger
    objs = vars(module)
    # objs = list(vars(module).items())
    all_names = [n for n in module.__all__ if not n.startswith("_")]
    objs = [(name, objs[name]) for name in all_names]
    # objs.sort(key=lambda x: x[0])
    stubs: List[str] = []
    modules: List[(ModuleType, str)] = []
    path = path / name
    logger = logging.getLogger(log_path)
    if "." in name:
        logger.error("module name contains path separator '.'")
    for obj_name, obj in objs:
        if not obj_name.startswith("_"):
            if isinstance(obj, type):
                stubs.append(gen_class(obj, obj_name))
            elif isinstance(obj, BuiltinFunctionType):
                fn_logger = logger.getChild(obj_name)
                stubs.append(gen_fn(obj, obj_name))
            elif isinstance(obj, ModuleType):
                loader = getattr(obj, "__loader__", None)
                if loader is None or isinstance(loader, ExtensionFileLoader):
                    modules.append((obj, obj_name))
    valid_imports = (
        f"{line}" for line in imports if not_this_module_import(line, full_name)
    )
    doc_str = getattr(module, "__doc__", None)
    if doc_str:
        doc = ['"""']
        doc.extend(doc_str.splitlines())
        doc.append('"""')
    else:
        doc = []

    stub_file = "\n".join(
        chain(
            doc,
            [comment],
            # [ "from typing import TYPE_CHECKING", "if TYPE_CHECKING:"],
            valid_imports,
            ["", f"__all__ = {all_names}"],
            stubs,
        )
    )
    path.mkdir(parents=True, exist_ok=True)
    file = path / "__init__.pyi"
    file.write_text(stub_file)

    for module, name in modules:
        gen_module(module, name, path, f"{log_path}.{name}", f"{full_name}.{name}")

    return
