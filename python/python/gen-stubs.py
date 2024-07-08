import ast
import importlib
import inspect
from typing import Dict, List, Optional, Tuple, TypedDict

import astunparse


class ParamsTypeDef(TypedDict):
    keywords: List[str]
    args: bool
    kwargs: bool
    docstring: Optional[str]


MethodTypeDef = Dict[str, ParamsTypeDef]
ClassTypeDef = Dict[str, MethodTypeDef]


def is_builtin_function(obj) -> bool:
    return isinstance(obj, type(len))


def parse_rust_module(module_name: str) -> Tuple[ClassTypeDef, MethodTypeDef]:
    tree = importlib.import_module(module_name)

    classes: ClassTypeDef = {}
    functions: MethodTypeDef = {}

    for name, obj in inspect.getmembers(tree):
        if inspect.isclass(obj):
            methods: MethodTypeDef = {}

            for attr_name, attr in inspect.getmembers(obj):
                docstring = inspect.getdoc(attr)

                if attr_name.startswith("__"):
                    continue

                elif callable(attr):
                    try:
                        signature = inspect.signature(attr)
                        args = list(signature.parameters.keys())

                        # Remove 'self' from the argument list if it's present
                        if args and args[0] == "self":
                            args = args[1:]

                        has_args = "args" in args
                        has_kwargs = "kwargs" in args

                        if "args" in args:
                            args.remove("args")

                        if "kwargs" in args:
                            args.remove("kwargs")

                        methods[attr_name] = {
                            "keywords": args,
                            "args": has_args,
                            "kwargs": has_kwargs,
                            "docstring": docstring,
                        }
                    except ValueError:
                        # If we can't get the signature, add the method with an empty arg list
                        methods[attr_name] = {
                            "keywords": [],
                            "args": False,
                            "kwargs": False,
                            "docstring": docstring,
                        }
                else:
                    methods[attr_name] = {
                        "keywords": [],
                        "args": False,
                        "kwargs": False,
                        "docstring": docstring,
                    }

            classes[name] = methods

        elif is_builtin_function(obj):
            params = list(inspect.signature(obj).parameters.keys())
            docstring = inspect.getdoc(obj)

            has_args = "args" in params
            has_kwargs = "kwargs" in params

            if "args" in params:
                params.remove("args")

            if "kwargs" in params:
                params.remove("kwargs")

            functions[name] = {
                "keywords": params,
                "args": has_args,
                "kwargs": has_kwargs,
                "docstring": docstring,
            }

    return classes, functions


def write_stubs(module_name: str) -> None:
    classes, functions = parse_rust_module(module_name)

    tree = ast.Module(body=[], type_ignores=[])

    # Write class stubs
    for class_name, methods in classes.items():
        class_node = ast.ClassDef(
            name=class_name,
            bases=[],
            keywords=[],
            body=[],
            decorator_list=[],
        )

        for method_name, method_params in methods.items():
            body = []

            if method_params["docstring"]:
                body.append(
                    ast.Expr(
                        value=ast.Constant(value=method_params["docstring"], kind=None)
                    )
                )

            body.append(ast.Expr(value=ast.Constant(value=...)))

            method_node = ast.FunctionDef(
                name=method_name,
                args=ast.arguments(
                    posonlyargs=[],
                    args=[  # TODO: type_comment, annotation
                        ast.arg(arg=arg, type_comment=None, annotation=None)
                        for arg in ["self", *method_params["keywords"]]
                    ],
                    vararg=(
                        ast.arg(arg="args", type_comment=None, annotation=None)
                        if method_params["args"]
                        else None
                    ),
                    kwonlyargs=[],
                    kw_defaults=[],
                    kwarg=(
                        ast.arg(arg="kwargs", type_comment=None, annotation=None)
                        if method_params["kwargs"]
                        else None
                    ),
                    defaults=[],
                ),
                body=body,
                decorator_list=[],
            )

            class_node.body.append(method_node)

        tree.body.append(class_node)

    # Write function stubs
    for fn_name, fn_params in functions.items():
        body = []

        if fn_params["docstring"]:
            body.append(
                ast.Expr(value=ast.Constant(value=fn_params["docstring"], kind=None))
            )

        body.append(ast.Expr(value=ast.Constant(value=...)))

        fn_node = ast.FunctionDef(
            name=fn_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[  # TODO: type_comment, annotation
                    ast.arg(arg=arg, type_comment=None, annotation=None)
                    for arg in fn_params["keywords"]
                ],
                vararg=(
                    ast.arg(arg="args", type_comment=None, annotation=None)
                    if fn_params["args"]
                    else None
                ),
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=(
                    ast.arg(arg="kwargs", type_comment=None, annotation=None)
                    if fn_params["kwargs"]
                    else None
                ),
                defaults=[],
            ),
            body=body,
            decorator_list=[],
        )

        tree.body.append(fn_node)

    stub_content = astunparse.unparse(tree)

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
        write_stubs(module)
