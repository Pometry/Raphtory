from __future__ import annotations


from setuptools import build_meta as _orig
import sys
from packaging import tags
from pathlib import Path
from ivy import get_and_run_ivy
from java import check_system_dl_java
from sbt_build import make_python_build
from check_platform import get_platform_tag

prepare_metadata_for_build_wheel = _orig.prepare_metadata_for_build_wheel
prepare_metadata_for_build_editable = _orig.prepare_metadata_for_build_editable
get_requires_for_build_wheel = _orig.get_requires_for_build_wheel
get_requires_for_build_sdist = _orig.get_requires_for_build_sdist
get_requires_for_build_editable = _orig.get_requires_for_build_editable


platform = get_platform_tag()


def build_sdist(sdist_directory, config_settings=None):
    make_python_build(rebuild=True)
    return _orig.build_sdist(sdist_directory, config_settings)


def setup_wheel_dependencies(rebuild=False):
    make_python_build(rebuild)
    java_bin = check_system_dl_java()
    get_and_run_ivy(java_bin)


def build_wheel(wheel_directory, config_settings: dict=None, metadata_directory=None):
    print(sys.path)
    setup_wheel_dependencies(rebuild=False)

    # make wheel platform-specific as the jre downloaded will be different
    if config_settings is not None:
        config_settings.setdefault("--global-option", []).extend(["--plat-name", platform])
    else:
        config_settings = {"--global-option": ["--plat-name", platform]}

    name = _orig.build_wheel(wheel_directory, config_settings, metadata_directory)
    return name


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    setup_wheel_dependencies(rebuild=True)
    return _orig.build_editable(wheel_directory, config_settings, metadata_directory)
