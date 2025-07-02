"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

root = Path(__file__).parent.parent.parent
doc_root = Path("reference")
stubs_source = root / "python" / "python"
src = stubs_source
nav = mkdocs_gen_files.Nav()

import griffe


def _public_items(items: dict) -> list:
    return [(name, item) for name, item in items.items() if item.is_public]


def _docstr_desc(item) -> str:
    if item.docstring:
        doc_str = item.docstring.parse("google")[0].value
    else:
        doc_str = ""
    return doc_str


def gen_class(name: str, cls: griffe.Class) -> Path:
    path = cls.module.filepath.relative_to(src).with_stem(name).with_suffix("")
    doc_path = doc_root / path.with_suffix(".md")
    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# ::: {cls.path}", file=fd)
    nav[tuple(path.parts)] = path.with_suffix(".md").as_posix()
    mkdocs_gen_files.set_edit_path(doc_path, cls.module.filepath.relative_to(root))
    return doc_path

# TODO
# - rename path as raphtory_path
# - create modules_path from raphtory_path by using the stem modules and suffix .md and insert in nav
# - make modules entry in index a link
def gen_module(name: str, module: griffe.Module) -> Path:
    path = module.filepath.relative_to(src).with_suffix("")
    parts = tuple(path.parts)
    modules_path = None
    if path.stem == "__init__":
        parts = parts[:-1]
        modules_path = path.with_stem("modules")
        path = path.with_stem("overview")
    nav[parts] = path.with_suffix(".md").as_posix()
    #if modules_path is not None:
    #    nav[parts] = modules_path.with_suffix(".md").as_posix()
    #    modules_path_full = doc_root / modules_path.with_suffix(".md")
    #
    #    with mkdocs_gen_files.open(modules_path_full, "w") as fd:
    #        print('# Dummy file test', file=fd)
            
    doc_path = doc_root / path.with_suffix(".md")

    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# ::: {module.path}", file=fd)
        print(f"    options:", file=fd)
        print(f"      members: false", file=fd)

        #print(f'docpath = {doc_path} \n', file=fd)
        #print(f'foo = {parts}', file=fd)

        public_modules = _public_items(module.modules)
        if public_modules:
            print("## Modules", file=fd)
            #print(f'docpath = {doc_path} \n', file=fd)
            #print(f'corepath = {module.filepath.relative_to(src).with_suffix("")}', file=fd)
            for member_name, sub_module in public_modules:
                sub_path = gen_module(member_name, sub_module)
                link_path = sub_path.relative_to(doc_path.parent)
                print(f"### [`{member_name}`]({link_path})", file=fd)
                print(f"{_docstr_desc(sub_module)}\n", file=fd)
                #print(f'sub_path = {sub_path}', file=fd)

        public_classes = _public_items(module.classes)
        if public_classes:
            print("## Classes", file=fd)
            for member_name, cls in public_classes:
                sub_path = gen_class(member_name, cls)
                link_path = sub_path.relative_to(doc_path.parent)
                print(f"### [`{member_name}`]({link_path})", file=fd)
                print(f"{_docstr_desc(cls)}\n", file=fd)

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

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
