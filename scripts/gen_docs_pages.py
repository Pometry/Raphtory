"""Generate the code reference pages."""

from pathlib import Path
from shutil import copy

import mkdocs_gen_files

root = Path(__file__).parent.parent
doc_root = Path("reference")
stubs_source = root / "python" / "python"
src = stubs_source

import griffe


def _public_items(items: dict) -> list:
    return [(name, item) for name, item in items.items() if item.is_public]


def _docstr_desc(item) -> str:
    if item.docstring:
        doc_str = item.docstring.parse("google")[0].value
    else:
        doc_str = ""
    return doc_str


def gen_class(name: str, cl) -> Path:
    doc_path = doc_root / cl.module.filepath.relative_to(src).with_stem(
        name
    ).with_suffix(".md")
    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# ::: {cl.path}", file=fd)
    return doc_path


def gen_module(name: str, module):
    doc_path = doc_root / module.filepath.relative_to(src).with_suffix(".md")
    if doc_path.stem == "__init__":
        doc_path = doc_path.with_stem("index")

    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# {name}", file=fd)

        public_modules = _public_items(module.modules)
        if public_modules:
            print("## Modules", file=fd)
            for member_name, sub_module in public_modules:
                sub_path = gen_module(member_name, sub_module)
                link_path = sub_path.relative_to(doc_path.parent)

                print(f"### [`{member_name}`]({link_path})", file=fd)
                print(f"{_docstr_desc(sub_module)}\n", file=fd)

        public_classes = _public_items(module.classes)
        if public_classes:
            print("## Classes", file=fd)
            for member_name, cl in public_classes:
                sub_path = gen_class(member_name, cl)
                link_path = sub_path.relative_to(doc_path.parent)
                print(f"### [`{member_name}`]({link_path})", file=fd)
                print(f"{_docstr_desc(cl)}\n", file=fd)

        public_attributes = _public_items(module.attributes)
        if public_attributes:
            print("## Attributes", file=fd)
            for member_name, attribute in public_attributes:
                print(f"### ::: {attribute.path}", file=fd)

        public_functions = _public_items(module.functions)
        if public_functions:
            print("## Functions", file=fd)
            for name, f in public_functions:
                print(f"### ::: {f.path}", file=fd)

    mkdocs_gen_files.set_edit_path(doc_path, module.filepath.relative_to(root))
    return doc_path


raphtory = griffe.load(
    "raphtory",
    search_paths=[src],
    allow_inspection=False,
    resolve_aliases=False,
)
gen_module("raphtory", raphtory)
