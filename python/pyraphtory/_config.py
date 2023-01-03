import os
from pathlib import Path
from importlib.resources import files, as_file
from typing import Iterable

import pyraphtory
import atexit
from itertools import chain


def get_local_jre_loc() -> Path:
    if os.environ.get("PYRAPHTORY_USE_SYSTEM_JAVA", ""):
        return Path(os.environ["JAVA_HOME"])
    else:
        manager = as_file(files(pyraphtory) / "jre")
        jre = manager.__enter__()

        def cleanup():
            manager.__exit__(None, None, None)

        atexit.register(cleanup)
        os.environ["JAVA_HOME"] = str(jre)
        return jre


def get_ivy_jars_from_local_lib():
    manager = as_file(files(pyraphtory) / "lib")
    # just in case these are not actually files for some reason
    lib = manager.__enter__()

    def cleanup():
        manager.__exit__(None, None, None)

    atexit.register(cleanup)
    jars = ":".join(str(f) for f in lib.rglob("*.jar"))
    # ivy_lib_dir = str(Path(ivy_folder).parent)+'/lib/compile'
    # jars_to_get = []
    # for file in Path(ivy_lib_dir).rglob("*.jar"):
    #     jars_to_get.append(str(file))
    # jars_to_get = ':'.join(set(jars_to_get))
    return jars


def join_jar_path(path: str, *new_paths: str) -> str:
    return ":".join((path, ":".join(new_paths)))


def setup_raphtory_jars():
    env_jar_location = os.environ.get("PYRAPTHORY_JAR_LOCATION", "")
    custom_jar_path = os.environ.get("PYRAPHTORYPATH", "")
    env_jar_glob_lookup = os.environ.get("PYRAPTHORY_JAR_GLOB_LOOKUP", '*.jar')
    java_args_env = os.environ.get("PYRAPTHORY_JVM_ARGS", "")
    path = get_ivy_jars_from_local_lib()
    if env_jar_location != "":
        path = join_jar_path(path, *(str(f) for f in Path(env_jar_location).rglob(env_jar_glob_lookup)))
    if custom_jar_path:
        path = join_jar_path(path, custom_jar_path)
    return path, java_args_env


jre = get_local_jre_loc()
jars, java_args = setup_raphtory_jars()
java = str(jre / "bin" / "java")
