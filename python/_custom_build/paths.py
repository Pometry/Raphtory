from pathlib import Path


build_folder = Path(__file__).resolve().parent
ivy_folder = build_folder / "ivy_data"
package_folder = build_folder.parent / "src" / "pyraphtory"
root_folder = build_folder.parent.parent
lib_folder = package_folder / "lib"
jre_folder = package_folder / "jre"
ivy_bin = package_folder / "ivy"