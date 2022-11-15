import sphinx.domains.std
from sphinx.application import Sphinx, Config
import sphinx.transforms.post_transforms
from sphinx.util import logging
from pathlib import Path
import shutil
from docutils import nodes
from docutils import utils
from docutils.utils.code_analyzer import Lexer, LexerError
from docutils.parsers.rst.roles import set_classes
import subprocess
import os

"""
Minimal Sphinx extension to extract Algorithm documentation from Raphtory.
"""

logger = logging.getLogger(__name__)

class SkipFile(Exception):
    pass


def setup(app: Sphinx):
    """Sphinx entrypoint"""

    app.add_role("s", scala_inline_code)
    app.add_role("scaladoc", scaladoc_link)
    app.add_config_value("raphtory_src_root", "", 'env', [str])
    app.add_config_value("autodoc_packages", [], 'env', [list[str]])
    app.add_config_value("build_scaladocs", True, 'env', [bool])
    app.add_config_value("build_algodocs", True, 'env', [bool])
    app.add_config_value("raphtory_root", "", 'env', [str])
    app.connect("config-inited", handle_config_init)

    return {
        "version": "0.1",
        "parallel_read_safe": False,
        "parallel_write_safe": False,
    }


def compile_move_scaladoc(config, source_dir):
    scalabuild_root = Path(config.raphtory_root) / "core" / "target"
    scaladoc_root = scalabuild_root / "scala-2.13" / "api"
    source_dir_scaladoc = source_dir / "_scaladoc"
    # clean up old files
    shutil.rmtree(scalabuild_root, ignore_errors=True)
    # Use SBT to build the scala docs, this is blocking
    process_call = subprocess.call(['sbt', 'core/doc'], cwd=config.raphtory_root)
    if process_call != 0:
        shutil.rmtree(scalabuild_root, ignore_errors=True)
        raise RuntimeError(f"sbt failed to build docs, error code: {process_call}")
    if process_call == 0:
        shutil.rmtree(source_dir_scaladoc, ignore_errors=True)
        # Move the scala docs to a custom folder
        shutil.move(scaladoc_root, source_dir_scaladoc)
    # clean up folder
    shutil.rmtree(scalabuild_root, ignore_errors=True)


def handle_config_init(app: Sphinx, config: Config):
    source_root = Path(app.srcdir)
    doc_root = source_root / "_autodoc"
    scala_src_root = config.raphtory_src_root

    if scala_src_root:
        if config.build_algodocs:
            scala_src_root = Path(scala_src_root)
            # clean up old files
            shutil.rmtree(doc_root, ignore_errors=True)
            for package in config.autodoc_packages:
                rel_path = Path(*package.split('.'))
                src_root = scala_src_root / rel_path

                write_index(doc_root / rel_path, package)
                discover_files(doc_root / rel_path, src_root, scala_src_root)
            # build scala doc
        if config.build_scaladocs:
            compile_move_scaladoc(config, source_root)


def discover_files(doc_root: Path, scala_root: Path, base_path: Path):
    """Recursively search for files to include and copy doc strings"""
    file_added = False
    for file_or_folder in sorted(scala_root.iterdir()):
        doc_file_or_folder = doc_root / file_or_folder.name
        rel_path = file_or_folder.relative_to(base_path)
        if file_or_folder.is_dir():
            write_index(doc_file_or_folder, ".".join(rel_path.parts))
            this_folder_not_empty = discover_files(doc_file_or_folder, file_or_folder, base_path)
            file_added = this_folder_not_empty or file_added
            if this_folder_not_empty:
                with open(doc_root / "index.rst", "a") as f:
                    f.write(f"   {file_or_folder.name}/index.rst\n")
        elif file_or_folder.suffix == ".scala":
            docstrs = extract_docs(base_path, rel_path)
            names_used = set()
            for docstr, name in docstrs:
                if name in names_used:
                    # continue
                    raise RuntimeError(f"Documentation for {'.'.join(rel_path.parts[:-1])}.{name} already exists")
                with open((doc_root / f"{name}.md"), 'w') as f:
                    f.write(docstr)
                with open(doc_root / "index.rst", "a") as f:
                    f.write(f"   {name}.md\n")
                names_used.add(name)
                file_added = True
    if not file_added:
        for f in doc_root.iterdir():
            f.unlink()
        doc_root.rmdir()
    return file_added


def extract_docs(base_path: Path, file: Path):
    """Extract docstring comment from scala file"""
    docstrs = []
    lines = []
    reading_docs = False
    any_docstr_found = False
    with open(base_path / file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith("/**"):
                line = line[2:]
                reading_docs = True
            if reading_docs:
                if line.endswith("*/"):
                    # docstring finished but last line could have content
                    line = line[:-2].rstrip()
                    reading_docs = False
                if line:
                    # line has content
                    if line == "*":
                        lines.append("")
                    else:
                        line = line.removeprefix("* ")
                        lines.append(line)
                if not reading_docs:
                    any_docstr_found = True
                    try:
                        docstrs.append(parse_docstr(lines, file))
                    except SkipFile:
                        pass
                    lines = []
    if not any_docstr_found:
        docstrs.append(parse_docstr([], file))
    return docstrs


def parse_docstr(docstr_lines: list[str], file: Path):
    """Custom parsing of docstr goes here"""
    header_line = 0
    # Package path
    path = '.'.join(file.parts[:-1])
    name = file.stem

    # Check if title line exists (first non-trivial line is heading1), else create it based on file name
    for header_line, line in enumerate(docstr_lines):
        if line:
            if line.startswith("@DoNotDocument"):
                raise SkipFile()
            elif not line.startswith("# "):
                header_line = 0
                docstr_lines.insert(0, f"# {name}")
            else:
                name = line[2:].strip()
            break
    else:  # all lines are blank (i.e., no docstring)
        header_line = 0
        docstr_lines = [
            f"# {file.stem}",
            "```{warning}",
            "   **Documentation Missing!**",
            "```",
        ]

    # create cross-reference link
    docstr_lines.insert(header_line, f"({path}.{name})=")

    # insert package path
    docstr_lines.insert(0, f'{{s}}`{path}.{name}`')

    return "\n".join(docstr_lines), name


def write_index(folder: Path, package, header=None):
    folder.mkdir(parents=True, exist_ok=True)
    if header is None:
        header = package.split(".")[-1]
    with open(folder / "index.rst", "w") as f:
        f.write(
            f""":s:`{package}`

.. _{package}:

{header}
{'=' * len(header)}

.. toctree::
   :glob:
   :maxdepth: 1

"""
        )


def scala_inline_code(role, rawtext, text, lineno, inliner, options={}, content=[]):
    set_classes(options)
    language = 'scala'
    classes = ['code', 'highlight']
    if 'classes' in options:
        classes.extend(options['classes'])
    if language and language not in classes:
        classes.append(language)
    try:
        tokens = Lexer(utils.unescape(text, True), language, 'short')
    except LexerError as error:
        msg = inliner.reporter.warning(error)
        prb = inliner.problematic(rawtext, rawtext, msg)
        return [prb], [msg]

    node = nodes.literal(rawtext, '', classes=classes)

    # analyse content and add nodes for every token
    for classes, value in tokens:
        if classes:
            node += nodes.inline(value, value, classes=classes)
        else:
            # insert as Text to decrease the verbosity of the output
            node += nodes.Text(value, value)

    return [node], []


def scaladoc_link(role, rawtext, text: str, lineno, inliner, options={}, content=[]):
    parts = text.split(".")
    if text.endswith(")"):
        # get class and method name for method link
        target = ".".join(parts[-2:])
        package = "/".join(parts[:-1])
    else:
        target = parts[-1]
        package = "/".join(parts)

    source_dir = Path(inliner.document.settings.env.srcdir)
    current_source = Path(inliner.document.current_source)

    rel_path = current_source.relative_to(source_dir)
    num_levels = len(rel_path.parents) - 1

    if (source_dir / "_scaladoc" / (package+".html")).is_file():
        link = "../"*num_levels + "_static/" + package + ".html"
    elif (source_dir / "_scaladoc" / (package+"$.html")).is_file():
        link = "../"*num_levels + "_static/" + package + "$.html"
    elif (source_dir / "_scaladoc" / package).is_dir():
        link = "../"*num_levels + "_static/" + package + "/index.html"
    else:
        link = None

    children, _ = scala_inline_code("s", target, target, lineno, inliner, options, content)

    if link is None:
        node = nodes.inline(rawtext)
        logger.warning(f"Cannot find docs for `{text}`", location=node, type='ref')
    else:
        node = nodes.reference(rawtext, refuri=link, target="_blank")
    node.line = lineno
    for child in children:
        node += child
    return [node], []

