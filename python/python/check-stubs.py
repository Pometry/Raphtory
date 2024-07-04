import ast
import importlib
import inspect
from typing import Dict, List, Tuple

import pytest


def is_builtin_function(obj) -> bool:
    return isinstance(obj, type(len))


MethodTypeDef = Dict[str, List[str]]
ClassTypeDef = Dict[str, MethodTypeDef]


@pytest.fixture(scope="module")
def stub_entities() -> Tuple[ClassTypeDef, MethodTypeDef]:
    stub_filename = "./raphtory/raphtory.pyi"
    with open(stub_filename, "r") as file:
        tree = ast.parse(file.read())

    classes: ClassTypeDef = {}
    functions: MethodTypeDef = {}

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            methods: MethodTypeDef = {}
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    params = [arg.arg for arg in item.args.args if arg.arg != "self"]
                    methods[item.name] = params
            classes[node.name] = methods

        elif isinstance(node, ast.FunctionDef):
            params = [arg.arg for arg in node.args.args]
            functions[node.name] = params

    return classes, functions


@pytest.fixture(scope="module")
def actual_entities() -> Tuple[ClassTypeDef, MethodTypeDef]:
    tree = importlib.import_module("raphtory")

    classes: ClassTypeDef = {}
    functions: MethodTypeDef = {}

    for name, obj in inspect.getmembers(tree):
        if inspect.isclass(obj):
            methods: MethodTypeDef = {}
            attributes = dir(obj)

            for attr_name in attributes:
                if attr_name.startswith("__"):
                    continue

                attr = getattr(obj, attr_name)

                if callable(attr):
                    try:
                        signature = inspect.signature(attr)
                        args = list(signature.parameters.keys())
                        # Remove 'self' from the argument list if it's present
                        if args and args[0] == "self":
                            args = args[1:]
                        methods[attr_name] = args
                    except ValueError:
                        # If we can't get the signature, add the method with an empty arg list
                        methods[attr_name] = []

            classes[name] = methods

        elif is_builtin_function(obj):
            functions[name] = list(inspect.signature(obj).parameters.keys())

    return classes, functions


def test_stub_vs_actual(
    stub_entities: Tuple[ClassTypeDef, MethodTypeDef],
    actual_entities: Tuple[ClassTypeDef, MethodTypeDef],
):
    stub_classes, stub_functions = stub_entities
    actual_classes, actual_functions = actual_entities

    print(actual_classes.keys())

    # Check classes
    for name, stub_class in stub_classes.items():
        assert name in actual_classes, f"{name} is in stub but not in Rust module"
        actual_class = actual_classes[name]

        for method_name, stub_params in stub_class.items():
            print(name, actual_class)
            assert (
                method_name in actual_class
            ), f"Method {method_name} of {name} is in stub but not in Rust module"

            actual_params = actual_class[method_name]
            assert (
                stub_params == actual_params
            ), f"Parameters mismatch for method {method_name} of {name}"

    # Check functions
    for name, stub_function in stub_functions.items():
        assert (
            name in actual_functions
        ), f"{name} function is in stub but not in Rust module"
        assert (
            stub_function == actual_functions[name]
        ), f"Parameters mismatch for function {name}"

    for name in actual_classes:
        assert name in stub_classes, f"{name} class is in Rust module but not in stub"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
