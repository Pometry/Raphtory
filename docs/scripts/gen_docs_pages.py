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

def write_public_modules(public_modules, nav_loc_mod, doc_path, fd):
                for member_name, sub_module in public_modules:
                    sub_path = gen_module((*nav_loc_mod, member_name), sub_module)
                    link_path = sub_path.relative_to(doc_path.parent)
                    print(f"### [`{member_name}`]({link_path})", file=fd)
                    print(f"{_docstr_desc(sub_module)}\n", file=fd)

def write_public_classes(public_classes, nav_loc_cls, fd):
                for member_name, cls in public_classes:
                    sub_path = gen_class((*nav_loc_cls, member_name), cls)
                    link_path = sub_path
                    print(f"### [`{member_name}`]({link_path})", file=fd)
                    print(f"{_docstr_desc(cls)}\n", file=fd)

def gen_class(parts: tuple[str], cls: griffe.Class) -> Path:
    path = Path(*parts)
    doc_path = doc_root / path.with_suffix(".md")
    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# ::: {cls.path}", file=fd)
    nav[tuple(path.parts)] = path.with_suffix(".md").as_posix()
    mkdocs_gen_files.set_edit_path(doc_path, cls.module.filepath.relative_to(root))
    return doc_path

def gen_module(parts: tuple[str], module: griffe.Module) -> Path:
    path = module.filepath.relative_to(src).with_suffix("")
    modules_path = None
    if path.stem == "__init__":
        modules_path = path.with_stem("modules")
        path = path.with_stem("overview")
    nav[parts] = path.with_suffix(".md").as_posix()
            
    doc_path = doc_root / path.with_suffix(".md")

    with mkdocs_gen_files.open(doc_path, "w") as fd:
        print(f"# ::: {module.path}", file=fd)
        print(f"    options:", file=fd)
        print(f"      members: false", file=fd)

        public_modules = _public_items(module.modules)
        if public_modules:
            print("## Modules", file=fd)

            nav_loc_mod = (*parts, 'Modules')
            nav[nav_loc_mod] = Path(*nav_loc_mod).with_suffix(".md")
            modules_path_full = doc_root / Path(*nav_loc_mod).with_suffix(".md")

            with mkdocs_gen_files.open(modules_path_full, "w") as mod_fd:
                print('# Modules', file=mod_fd)
                write_public_modules(public_modules, nav_loc_mod, modules_path_full, mod_fd)

            write_public_modules(public_modules, nav_loc_mod, doc_path, fd)

        public_classes = _public_items(module.classes)
        if public_classes:
            print("## Classes", file=fd)
            nav_loc_cls = (*parts, 'Classes')
            nav[nav_loc_cls] = Path(*nav_loc_cls).with_suffix(".md")
            modules_path_full = doc_root / Path(*nav_loc_cls).with_suffix(".md")

            with mkdocs_gen_files.open(modules_path_full, "w") as cls_fd:
                print('# Classes', file=cls_fd)
                write_public_classes(public_classes, nav_loc_cls, cls_fd)

            write_public_classes(public_classes, nav_loc_cls, fd)

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

gen_module(("raphtory",), raphtory)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
