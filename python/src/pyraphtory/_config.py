import os
from pathlib import Path
from importlib.resources import files
import pyraphtory
import logging
import shutil


def get_java_home() -> Path:
    logging.info("Getting JAVA_HOME")
    home = os.getenv('JAVA_HOME')
    if home is not None:
        return Path(home)
    elif shutil.which('java') is not None:
        logging.info(f'JAVA_HOME not found. But java found. Detecting home...')
        # resolve JAVA_HOME in case it is a symlink
        home = Path(shutil.which('java')).resolve().parents[1]
        os.environ["JAVA_HOME"] = str(home)
        return home
    else:
        raise FileNotFoundError("JAVA_HOME has not been set, java was also not found")


def get_local_jre_loc() -> Path:
    if os.environ.get("PYRAPHTORY_USE_SYSTEM_JAVA", ""):
        return get_java_home()
    else:
        jre = files(pyraphtory) / "jre"
        if not isinstance(jre, Path):
            raise RuntimeError("Pyraphtory is not installed correctly, are you trying to import from a compressed file?")

        os.environ["JAVA_HOME"] = str(jre)
        return jre


def get_ivy_jars_from_local_lib():
    lib = files(pyraphtory) / "lib"
    if not isinstance(lib, Path):
        raise RuntimeError("Pyraphtory is not installed correctly, are you trying to import from a compressed file?")

    jars = ":".join(str(f) for f in lib.rglob("*.jar"))
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
