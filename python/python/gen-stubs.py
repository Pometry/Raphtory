import ast
import importlib
import inspect
from typing import List, Optional, TypedDict

import astunparse


class ParamsTypeDef(TypedDict):
    args: List[str]
    defaults: List[ast.expr]
    vararg: Optional[ast.arg]
    kwarg: Optional[ast.arg]


def is_builtin_function(obj) -> bool:
    return isinstance(obj, type(len))


def parse_params(sig: inspect.Signature) -> ParamsTypeDef:
    vararg = None
    kwarg = None

    args: List[str] = []
    defaults: List[ast.expr] = []

    for name, param in sig.parameters.items():
        if name == "self":
            continue

        if name == "args":
            vararg = ast.arg(arg="args", type_comment=None, annotation=None)
            continue
        if name == "kwargs":
            kwarg = ast.arg(arg="kwargs", type_comment=None, annotation=None)
            continue

        is_optional = param.default is not inspect.Parameter.empty
        default_value = param.default if is_optional else None

        args.append(name)
        if is_optional:
            if type(param.default).__name__ == "_Unset":  # TODO: Hack
                default_value = None
            defaults.append(ast.Constant(value=default_value, kind=None))

    return {"args": args, "defaults": defaults, "vararg": vararg, "kwarg": kwarg}


# TODO: Optional vs required params
def gen_stubs(module_name: str) -> None:
    rust_tree = importlib.import_module(module_name)
    stub_tree = ast.Module(body=[], type_ignores=[])

    for name, obj in inspect.getmembers(rust_tree):
        if inspect.isclass(obj):
            class_node = ast.ClassDef(
                name=name,
                bases=[],
                keywords=[],
                body=[],
                decorator_list=[],
            )

            for attr_name, attr in inspect.getmembers(obj):
                if attr_name.startswith("__"):
                    continue

                body = []
                docstring = inspect.getdoc(attr)

                if docstring:
                    body.append(
                        ast.Expr(value=ast.Constant(value=docstring, kind=None))
                    )

                body.append(ast.Expr(value=ast.Constant(value=...)))

                if isinstance(attr, property):
                    method_node = ast.FunctionDef(
                        name=attr_name,
                        args=ast.arguments(
                            posonlyargs=[],
                            args=[
                                ast.arg(arg="self", type_comment=None, annotation=None)
                            ],
                            vararg=None,
                            kwonlyargs=[],
                            kw_defaults=[],
                            kwarg=None,
                            defaults=[],
                        ),
                        body=body,
                        decorator_list=([ast.Name(id="property")]),
                    )

                elif callable(attr):
                    signature = inspect.signature(attr)
                    params = parse_params(signature)

                    if not params["args"] or params["args"][0] != "self":
                        params["args"] = ["self"] + params["args"]

                    method_node = ast.FunctionDef(
                        name=attr_name,
                        args=ast.arguments(
                            posonlyargs=[],
                            args=[  # TODO: type_comment, annotation
                                ast.arg(arg=arg, type_comment=None, annotation=None)
                                for arg in params["args"]
                            ],
                            vararg=params["vararg"],
                            kwonlyargs=[],
                            kw_defaults=[],
                            kwarg=params["kwarg"],
                            defaults=params["defaults"],
                        ),
                        body=body,
                        decorator_list=[],
                    )

                else:
                    method_node = ast.FunctionDef(
                        name=attr_name,
                        args=ast.arguments(
                            posonlyargs=[],
                            args=[
                                ast.arg(arg="self", type_comment=None, annotation=None)
                            ],
                            vararg=None,
                            kwonlyargs=[],
                            kw_defaults=[],
                            kwarg=None,
                            defaults=[],
                        ),
                        body=body,
                        decorator_list=([ast.Name(id="property")]),
                    )

                class_node.body.append(method_node)
            stub_tree.body.append(class_node)

        elif is_builtin_function(obj):
            docstring = inspect.getdoc(obj)
            body = []

            if docstring:
                body.append(ast.Expr(value=ast.Constant(value=docstring, kind=None)))

            body.append(ast.Expr(value=ast.Constant(value=...)))

            params = parse_params(inspect.signature(obj))

            fn_node = ast.FunctionDef(
                name=name,
                args=ast.arguments(
                    posonlyargs=[],
                    args=[  # TODO: type_comment, annotation
                        ast.arg(arg=arg, type_comment=None, annotation=None)
                        for arg in params["args"]
                    ],
                    vararg=params["vararg"],
                    kwonlyargs=[],
                    kw_defaults=[],
                    kwarg=params["kwarg"],
                    defaults=params["defaults"],
                ),
                body=body,
                decorator_list=[],
            )

            stub_tree.body.append(fn_node)

    stub_content = astunparse.unparse(stub_tree)

    comment = """###############################################################################
#                                                                             #
#                      AUTOGENERATED TYPE STUB FILE                           #
#                                                                             #
#    This file was automatically generated. Do not modify it directly.        #
#    Any changes made here may be lost when the file is regenerated.          #
#                                                                             #
###############################################################################"""

    full_content = f"{comment}{stub_content}"

    with open(f"./raphtory/{module_name.replace('raphtory.', '')}.pyi", "w") as file:
        file.write(full_content)


if __name__ == "__main__":
    modules = [
        "raphtory",
        "raphtory.algorithms",
        "raphtory.graph_gen",
        "raphtory.graph_loader",
        "raphtory.vectors",
    ]
    for module in modules:
        gen_stubs(module)
