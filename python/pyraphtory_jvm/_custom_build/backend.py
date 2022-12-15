from setuptools import build_meta as _orig
import sys
from jre import package_folder, lib_folder
from packaging import tags
from pathlib import Path
from ivy import get_and_run_ivy
from java import check_system_dl_java

prepare_metadata_for_build_wheel = _orig.prepare_metadata_for_build_wheel
get_requires_for_build_wheel = _orig.get_requires_for_build_wheel
get_requires_for_build_sdist = _orig.get_requires_for_build_sdist
build_sdist = _orig.build_sdist

platform = next(tags.sys_tags()).platform
build_folder = Path(__file__).resolve().parent

def build_wheel(wheel_directory, config_settings: dict=None, metadata_directory=None):
    print(sys.path)
    java_bin = check_system_dl_java(package_folder)
    get_and_run_ivy(java_bin, build_folder / "ivy_data", lib_folder)

    # make wheel platform-specific as the jre downloaded will be different
    if config_settings is not None:
        config_settings.setdefault("--global-option", []).extend(["--plat-name", platform])
    else:
        config_settings = {"--global-option": ["--plat-name", platform]}

    name = _orig.build_wheel(wheel_directory, config_settings, metadata_directory)
    return name
